// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package apis

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sync"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	rmserver "github.com/tikv/pd/pkg/mcs/resourcemanager/server"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/multiservicesapi"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/reflectutil"
)

// APIPathPrefix is the prefix of the API path.
const APIPathPrefix = "/resource-manager/api/v1/"

var (
	once            sync.Once
	apiServiceGroup = apiutil.APIServiceGroup{
		Name:       "resource-manager",
		Version:    "v1",
		IsCore:     false,
		PathPrefix: APIPathPrefix,
	}
)

func init() {
	rmserver.SetUpRestHandler = func(srv *rmserver.Service) (http.Handler, apiutil.APIServiceGroup) {
		s := NewService(srv)
		return s.handler(), apiServiceGroup
	}
}

// Service is the resource group service.
type Service struct {
	apiHandlerEngine *gin.Engine
	root             *gin.RouterGroup

	manager *rmserver.Manager
}

// NewService returns a new Service.
func NewService(srv *rmserver.Service) *Service {
	once.Do(func() {
		// These global modification will be effective only for the first invoke.
		_ = godotenv.Load()
		gin.SetMode(gin.ReleaseMode)
	})
	apiHandlerEngine := gin.New()
	apiHandlerEngine.Use(gin.Recovery())
	apiHandlerEngine.Use(cors.Default())
	apiHandlerEngine.Use(gzip.Gzip(gzip.DefaultCompression))
	manager := srv.GetManager()
	apiHandlerEngine.Use(func(c *gin.Context) {
		// manager implements the interface of basicserver.Service.
		c.Set(multiservicesapi.ServiceContextKey, manager.GetBasicServer())
		c.Next()
	})
	apiHandlerEngine.Use(multiservicesapi.ServiceRedirector())
	apiHandlerEngine.GET("metrics", utils.PromHandler())
	pprof.Register(apiHandlerEngine)
	endpoint := apiHandlerEngine.Group(APIPathPrefix)
	s := &Service{
		manager:          manager,
		apiHandlerEngine: apiHandlerEngine,
		root:             endpoint,
	}
	s.RegisterAdminRouter()
	s.RegisterRouter()
	return s
}

// RegisterAdminRouter registers the router of the TSO admin handler.
func (s *Service) RegisterAdminRouter() {
	router := s.root.Group("admin")
	router.PUT("/log", changeLogLevel)
}

// RegisterRouter registers the router of the service.
func (s *Service) RegisterRouter() {
	configEndpoint := s.root.Group("/config")
	configEndpoint.POST("/group", s.postResourceGroup)
	configEndpoint.PUT("/group", s.putResourceGroup)
	configEndpoint.GET("/group/:name", s.getResourceGroup)
	configEndpoint.GET("/groups", s.getResourceGroupList)
	configEndpoint.DELETE("/group/:name", s.deleteResourceGroup)
	configEndpoint.GET("/controller", s.getControllerConfig)
	configEndpoint.POST("/controller", s.setControllerConfig)
}

func (s *Service) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.apiHandlerEngine.ServeHTTP(w, r)
	})
}

func changeLogLevel(c *gin.Context) {
	svr := c.MustGet(multiservicesapi.ServiceContextKey).(*rmserver.Service)
	var level string
	if err := c.Bind(&level); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	if err := svr.SetLogLevel(level); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	log.SetLevel(logutil.StringToZapLogLevel(level))
	c.String(http.StatusOK, "The log level is updated.")
}

// postResourceGroup
//
//	@Tags		ResourceManager
//	@Summary	Add a resource group
//	@Param		groupInfo	body		object	true	"json params, rmpb.ResourceGroup"
//	@Success	200			{string}	string	"Success"
//	@Failure	400			{string}	error
//	@Failure	500			{string}	error
//	@Router		/config/group [POST]
func (s *Service) postResourceGroup(c *gin.Context) {
	var group rmpb.ResourceGroup
	if err := c.ShouldBindJSON(&group); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if err := s.manager.AddResourceGroup(&group); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "Success!")
}

// putResourceGroup
//
//	@Tags		ResourceManager
//	@Summary	updates an exists resource group
//	@Param		groupInfo	body	object	true	"json params, rmpb.ResourceGroup"
//	@Success	200			"Success"
//	@Failure	400			{string}	error
//	@Failure	500			{string}	error
//	@Router		/config/group [PUT]
func (s *Service) putResourceGroup(c *gin.Context) {
	var group rmpb.ResourceGroup
	if err := c.ShouldBindJSON(&group); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	if err := s.manager.ModifyResourceGroup(&group); err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "Success!")
}

// getResourceGroup
//
//	@Tags		ResourceManager
//	@Summary	Get resource group by name.
//	@Success	200		{string}	json	format	of	rmserver.ResourceGroup
//	@Failure	404		{string}	error
//	@Param		name	path		string	true	"groupName"
//	@Router		/config/group/{name} [GET]
func (s *Service) getResourceGroup(c *gin.Context) {
	group := s.manager.GetResourceGroup(c.Param("name"))
	if group == nil {
		c.String(http.StatusNotFound, errors.New("resource group not found").Error())
	}
	c.IndentedJSON(http.StatusOK, group)
}

// getResourceGroupList
//
//	@Tags		ResourceManager
//	@Summary	get all resource group with a list.
//	@Success	200	{string}	json	format	of	[]rmserver.ResourceGroup
//	@Failure	404	{string}	error
//	@Router		/config/groups [GET]
func (s *Service) getResourceGroupList(c *gin.Context) {
	groups := s.manager.GetResourceGroupList()
	c.IndentedJSON(http.StatusOK, groups)
}

// deleteResourceGroup
//
//	@Tags		ResourceManager
//	@Summary	delete resource group by name.
//	@Param		name	path		string	true	"Name of the resource group to be deleted"
//	@Success	200		{string}	string	"Success!"
//	@Failure	404		{string}	error
//	@Router		/config/group/{name} [DELETE]
func (s *Service) deleteResourceGroup(c *gin.Context) {
	if err := s.manager.DeleteResourceGroup(c.Param("name")); err != nil {
		c.String(http.StatusNotFound, err.Error())
	}
	c.String(http.StatusOK, "Success!")
}

// GetControllerConfig
//
//	@Tags		ResourceManager
//	@Summary	Get the resource controller config.
//	@Success	200		{string}	json	format	of	rmserver.ControllerConfig
//	@Failure	400 	{string}	error
//	@Router		/config/controller [GET]
func (s *Service) getControllerConfig(c *gin.Context) {
	config := s.manager.GetControllerConfig()
	c.IndentedJSON(http.StatusOK, config)
}

// SetControllerConfig
//
//	@Tags		ResourceManager
//	@Summary	Set the resource controller config.
//	@Param		config	body	object	true	"json params, rmserver.ControllerConfig"
//	@Success	200		{string}	string	"Success!"
//	@Failure	400 	{string}	error
//	@Router		/config/controller [POST]
func (s *Service) setControllerConfig(c *gin.Context) {
	conf := make(map[string]interface{})
	if err := c.ShouldBindJSON(&conf); err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	for k, v := range conf {
		key := reflectutil.FindJSONFullTagByChildTag(reflect.TypeOf(rmserver.ControllerConfig{}), k)
		if key == "" {
			c.String(http.StatusBadRequest, fmt.Sprintf("config item %s not found", k))
			return
		}
		if err := s.manager.UpdateControllerConfigItem(key, v); err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
	}
	c.String(http.StatusOK, "Success!")
}
