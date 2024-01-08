// Copyright 2016 TiKV Project Authors.
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

package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	scheapi "github.com/tikv/pd/pkg/mcs/scheduling/server/apis/v1"
	tsoapi "github.com/tikv/pd/pkg/mcs/tso/server/apis/v1"
	mcs "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/apiutil/serverapi"
	"github.com/tikv/pd/server"
	"github.com/urfave/negroni"
)

const apiPrefix = "/pd"

// NewHandler creates a HTTP handler for API.
func NewHandler(_ context.Context, svr *server.Server) (http.Handler, apiutil.APIServiceGroup, error) {
	group := apiutil.APIServiceGroup{
		Name:   "core",
		IsCore: true,
	}
	prefix := apiPrefix + "/api/v1"
	r := createRouter(apiPrefix, svr)
	router := mux.NewRouter()

	// Following requests are redirected:
	// 	"/admin/reset-ts", http.MethodPost
	//	"/operators", http.MethodGet
	//	"/operators", http.MethodPost
	//	"/operators/records",http.MethodGet
	//	"/operators/{region_id}", http.MethodGet
	//	"/operators/{region_id}", http.MethodDelete
	//	"/checker/{name}", http.MethodPost
	//	"/checker/{name}", http.MethodGet
	//	"/schedulers", http.MethodGet
	//	"/schedulers/{name}", http.MethodPost
	//	"/schedulers/diagnostic/{name}", http.MethodGet
	//	"/scheduler-config", http.MethodGet
	//	"/hotspot/regions/read", http.MethodGet
	//	"/hotspot/regions/write", http.MethodGet
	//	"/hotspot/regions/history", http.MethodGet
	//	"/hotspot/stores", http.MethodGet
	//	"/hotspot/buckets", http.MethodGet
	// Following requests are **not** redirected:
	//	"/schedulers", http.MethodPost
	//	"/schedulers/{name}", http.MethodDelete
	router.PathPrefix(apiPrefix).Handler(negroni.New(
		serverapi.NewRuntimeServiceValidator(svr, group),
		serverapi.NewRedirector(svr,
			serverapi.MicroserviceRedirectRule(
				prefix+"/admin/reset-ts",
				tsoapi.APIPathPrefix+"/admin/reset-ts",
				mcs.TSOServiceName,
				[]string{http.MethodPost}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/operators",
				scheapi.APIPathPrefix+"/operators",
				mcs.SchedulingServiceName,
				[]string{http.MethodPost, http.MethodGet, http.MethodDelete}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/checker", // Note: this is a typo in the original code
				scheapi.APIPathPrefix+"/checkers",
				mcs.SchedulingServiceName,
				[]string{http.MethodPost, http.MethodGet}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/region/id",
				scheapi.APIPathPrefix+"/config/regions",
				mcs.SchedulingServiceName,
				[]string{http.MethodGet},
				func(r *http.Request) bool {
					// The original code uses the path "/region/id" to get the region id.
					// However, the path "/region/id" is used to get the region by id, which is not what we want.
					return strings.Contains(r.URL.Path, "label")
				}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/regions/accelerate-schedule",
				scheapi.APIPathPrefix+"/regions/accelerate-schedule",
				mcs.SchedulingServiceName,
				[]string{http.MethodPost}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/regions/scatter",
				scheapi.APIPathPrefix+"/regions/scatter",
				mcs.SchedulingServiceName,
				[]string{http.MethodPost}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/regions/split",
				scheapi.APIPathPrefix+"/regions/split",
				mcs.SchedulingServiceName,
				[]string{http.MethodPost}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/regions/replicated",
				scheapi.APIPathPrefix+"/regions/replicated",
				mcs.SchedulingServiceName,
				[]string{http.MethodGet}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/config/region-label/rules",
				scheapi.APIPathPrefix+"/config/region-label/rules",
				mcs.SchedulingServiceName,
				[]string{http.MethodGet}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/config/region-label/rule/", // Note: this is a typo in the original code
				scheapi.APIPathPrefix+"/config/region-label/rules",
				mcs.SchedulingServiceName,
				[]string{http.MethodGet}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/hotspot",
				scheapi.APIPathPrefix+"/hotspot",
				mcs.SchedulingServiceName,
				[]string{http.MethodGet}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/config/rules",
				scheapi.APIPathPrefix+"/config/rules",
				mcs.SchedulingServiceName,
				[]string{http.MethodGet}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/config/rule/",
				scheapi.APIPathPrefix+"/config/rule",
				mcs.SchedulingServiceName,
				[]string{http.MethodGet}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/config/rule_group/",
				scheapi.APIPathPrefix+"/config/rule_groups", // Note: this is a typo in the original code
				mcs.SchedulingServiceName,
				[]string{http.MethodGet}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/config/rule_groups",
				scheapi.APIPathPrefix+"/config/rule_groups",
				mcs.SchedulingServiceName,
				[]string{http.MethodGet}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/config/placement-rule",
				scheapi.APIPathPrefix+"/config/placement-rule",
				mcs.SchedulingServiceName,
				[]string{http.MethodGet}),
			// because the writing of all the meta information of the scheduling service is in the API server,
			// we should not post and delete the scheduler directly in the scheduling service.
			serverapi.MicroserviceRedirectRule(
				prefix+"/schedulers",
				scheapi.APIPathPrefix+"/schedulers",
				mcs.SchedulingServiceName,
				[]string{http.MethodGet}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/scheduler-config",
				scheapi.APIPathPrefix+"/schedulers/config",
				mcs.SchedulingServiceName,
				[]string{http.MethodGet}),
			serverapi.MicroserviceRedirectRule(
				prefix+"/schedulers/", // Note: this means "/schedulers/{name}"
				scheapi.APIPathPrefix+"/schedulers",
				mcs.SchedulingServiceName,
				[]string{http.MethodPost}),
		),
		negroni.Wrap(r)),
	)

	return router, group, nil
}
