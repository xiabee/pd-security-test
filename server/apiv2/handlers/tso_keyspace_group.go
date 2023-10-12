// Copyright 2023 TiKV Project Authors.
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

package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/apiv2/middlewares"
)

// RegisterTSOKeyspaceGroup registers keyspace group handlers to the server.
func RegisterTSOKeyspaceGroup(r *gin.RouterGroup) {
	router := r.Group("tso/keyspace-groups")
	router.Use(middlewares.BootstrapChecker())
	router.POST("", CreateKeyspaceGroups)
	router.GET("", GetKeyspaceGroups)
	router.GET("/:id", GetKeyspaceGroupByID)
	router.DELETE("/:id", DeleteKeyspaceGroupByID)
	router.POST("/:id/alloc", AllocNodeForKeyspaceGroup)
	router.POST("/:id/split", SplitKeyspaceGroupByID)
	router.DELETE("/:id/split", FinishSplitKeyspaceByID)
}

// CreateKeyspaceGroupParams defines the params for creating keyspace groups.
type CreateKeyspaceGroupParams struct {
	KeyspaceGroups []*endpoint.KeyspaceGroup `json:"keyspace-groups"`
}

// CreateKeyspaceGroups creates keyspace groups.
func CreateKeyspaceGroups(c *gin.Context) {
	createParams := &CreateKeyspaceGroupParams{}
	err := c.BindJSON(createParams)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	for _, keyspaceGroup := range createParams.KeyspaceGroups {
		if !isValid(keyspaceGroup.ID) {
			c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
			return
		}
		if keyspaceGroup.UserKind == "" {
			keyspaceGroup.UserKind = endpoint.Basic.String()
		} else if !endpoint.IsUserKindValid(keyspaceGroup.UserKind) {
			c.AbortWithStatusJSON(http.StatusBadRequest, "invalid user kind")
			return
		}
	}

	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	err = manager.CreateKeyspaceGroups(createParams.KeyspaceGroups)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, nil)
}

// GetKeyspaceGroups gets keyspace groups from the start ID with limit.
// If limit is 0, it will load all keyspace groups from the start ID.
func GetKeyspaceGroups(c *gin.Context) {
	scanStart, scanLimit, err := parseLoadAllQuery(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, err.Error())
		return
	}

	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	keyspaceGroups, err := manager.GetKeyspaceGroups(scanStart, scanLimit)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	c.IndentedJSON(http.StatusOK, keyspaceGroups)
}

// GetKeyspaceGroupByID gets keyspace group by ID.
func GetKeyspaceGroupByID(c *gin.Context) {
	id, err := validateKeyspaceGroupID(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}

	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	kg, err := manager.GetKeyspaceGroupByID(id)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}

	c.IndentedJSON(http.StatusOK, kg)
}

// DeleteKeyspaceGroupByID deletes keyspace group by ID.
func DeleteKeyspaceGroupByID(c *gin.Context) {
	id, err := validateKeyspaceGroupID(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}

	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	kg, err := manager.DeleteKeyspaceGroupByID(id)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.IndentedJSON(http.StatusOK, kg)
}

// SplitKeyspaceGroupByIDParams defines the params for splitting a keyspace group.
type SplitKeyspaceGroupByIDParams struct {
	NewID     uint32   `json:"new-id"`
	Keyspaces []uint32 `json:"keyspaces"`
}

// SplitKeyspaceGroupByID splits keyspace group by ID into a new keyspace group with the given new ID.
// And the keyspaces in the old keyspace group will be moved to the new keyspace group.
func SplitKeyspaceGroupByID(c *gin.Context) {
	id, err := validateKeyspaceGroupID(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}
	splitParams := &SplitKeyspaceGroupByIDParams{}
	err = c.BindJSON(splitParams)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	if !isValid(splitParams.NewID) {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}
	if len(splitParams.Keyspaces) == 0 {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid empty keyspaces")
		return
	}

	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	err = manager.SplitKeyspaceGroupByID(id, splitParams.NewID, splitParams.Keyspaces)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, nil)
}

// FinishSplitKeyspaceByID finishes split keyspace group by ID.
func FinishSplitKeyspaceByID(c *gin.Context) {
	id, err := validateKeyspaceGroupID(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}

	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	err = manager.FinishSplitKeyspaceByID(id)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, nil)
}

// AllocNodeForKeyspaceGroupParams defines the params for allocating nodes for keyspace groups.
type AllocNodeForKeyspaceGroupParams struct {
	Replica int `json:"replica"`
}

// AllocNodeForKeyspaceGroup allocates nodes for keyspace group.
func AllocNodeForKeyspaceGroup(c *gin.Context) {
	id, err := validateKeyspaceGroupID(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid keyspace group id")
		return
	}
	svr := c.MustGet(middlewares.ServerContextKey).(*server.Server)
	manager := svr.GetKeyspaceGroupManager()
	allocParams := &AllocNodeForKeyspaceGroupParams{}
	err = c.BindJSON(allocParams)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, errs.ErrBindJSON.Wrap(err).GenWithStackByCause())
		return
	}
	if manager.GetNodesNum() < allocParams.Replica || allocParams.Replica < 1 {
		c.AbortWithStatusJSON(http.StatusBadRequest, "invalid replica, should be in [1, nodes_num]")
		return
	}
	keyspaceGroup, err := manager.GetKeyspaceGroupByID(id)
	if err != nil || keyspaceGroup == nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, "keyspace group does not exist")
		return
	}
	if len(keyspaceGroup.Members) >= allocParams.Replica {
		c.AbortWithStatusJSON(http.StatusBadRequest, "existed replica is larger than the new replica")
		return
	}
	// get the nodes
	nodes, err := manager.AllocNodesForKeyspaceGroup(id, allocParams.Replica)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, nodes)
}

func validateKeyspaceGroupID(c *gin.Context) (uint32, error) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		return 0, err
	}
	if !isValid(uint32(id)) {
		return 0, errors.Errorf("invalid keyspace group id: %d", id)
	}
	return uint32(id), nil
}

func isValid(id uint32) bool {
	return id >= utils.DefaultKeyspaceGroupID && id <= utils.MaxKeyspaceGroupCountInUse
}
