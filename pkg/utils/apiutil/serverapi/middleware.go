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

package serverapi

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
	"github.com/urfave/negroni"
	"go.uber.org/zap"
)

type runtimeServiceValidator struct {
	s     *server.Server
	group apiutil.APIServiceGroup
}

// NewRuntimeServiceValidator checks if the path is invalid.
func NewRuntimeServiceValidator(s *server.Server, group apiutil.APIServiceGroup) negroni.Handler {
	return &runtimeServiceValidator{s: s, group: group}
}

func (h *runtimeServiceValidator) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	if IsServiceAllowed(h.s, h.group) {
		next(w, r)
		return
	}

	http.Error(w, "no service", http.StatusServiceUnavailable)
}

// IsServiceAllowed checks the service through the path.
func IsServiceAllowed(s *server.Server, group apiutil.APIServiceGroup) bool {
	// for core path
	if group.IsCore {
		return true
	}

	opt := s.GetServerOption()
	cfg := opt.GetPDServerConfig()
	if cfg != nil {
		for _, allow := range cfg.RuntimeServices {
			if group.Name == allow {
				return true
			}
		}
	}

	return false
}

type redirector struct {
	s *server.Server

	microserviceRedirectRules []*microserviceRedirectRule
}

type microserviceRedirectRule struct {
	matchPath         string
	targetPath        string
	targetServiceName string
	matchMethods      []string
	filter            func(*http.Request) bool
}

// NewRedirector redirects request to the leader if needs to be handled in the leader.
func NewRedirector(s *server.Server, opts ...RedirectorOption) negroni.Handler {
	r := &redirector{s: s}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// RedirectorOption defines the option of redirector
type RedirectorOption func(*redirector)

// MicroserviceRedirectRule new a microservice redirect rule option
func MicroserviceRedirectRule(matchPath, targetPath, targetServiceName string,
	methods []string, filters ...func(*http.Request) bool) RedirectorOption {
	return func(s *redirector) {
		rule := &microserviceRedirectRule{
			matchPath:         matchPath,
			targetPath:        targetPath,
			targetServiceName: targetServiceName,
			matchMethods:      methods,
		}
		if len(filters) > 0 {
			rule.filter = filters[0]
		}
		s.microserviceRedirectRules = append(s.microserviceRedirectRules, rule)
	}
}

func (h *redirector) matchMicroServiceRedirectRules(r *http.Request) (bool, string) {
	if !h.s.IsAPIServiceMode() {
		return false, ""
	}
	if len(h.microserviceRedirectRules) == 0 {
		return false, ""
	}
	// Remove trailing '/' from the URL path
	// It will be helpful when matching the redirect rules "schedulers" or "schedulers/{name}"
	r.URL.Path = strings.TrimRight(r.URL.Path, "/")
	for _, rule := range h.microserviceRedirectRules {
		// Now we only support checking the scheduling service whether it is independent
		if rule.targetServiceName == mcsutils.SchedulingServiceName {
			if !h.s.IsServiceIndependent(mcsutils.SchedulingServiceName) {
				continue
			}
		}
		if strings.HasPrefix(r.URL.Path, rule.matchPath) &&
			slice.Contains(rule.matchMethods, r.Method) {
			if rule.filter != nil && !rule.filter(r) {
				continue
			}
			// we check the service primary addr here,
			// if the service is not available, we will return ErrRedirect by returning an empty addr.
			addr, ok := h.s.GetServicePrimaryAddr(r.Context(), rule.targetServiceName)
			if !ok || addr == "" {
				log.Warn("failed to get the service primary addr when trying to match redirect rules",
					zap.String("path", r.URL.Path))
				return true, ""
			}
			// If the URL contains escaped characters, use RawPath instead of Path
			origin := r.URL.Path
			path := r.URL.Path
			if r.URL.RawPath != "" {
				path = r.URL.RawPath
			}
			// Extract parameters from the URL path
			// e.g. r.URL.Path = /pd/api/v1/operators/1 (before redirect)
			//      matchPath  = /pd/api/v1/operators
			//      targetPath = /scheduling/api/v1/operators
			//      r.URL.Path = /scheduling/api/v1/operator/1 (after redirect)
			pathParams := strings.TrimPrefix(path, rule.matchPath)
			pathParams = strings.Trim(pathParams, "/") // Remove leading and trailing '/'
			if len(pathParams) > 0 {
				r.URL.Path = rule.targetPath + "/" + pathParams
			} else {
				r.URL.Path = rule.targetPath
			}
			log.Debug("redirect to micro service", zap.String("path", r.URL.Path), zap.String("origin-path", origin),
				zap.String("target", addr), zap.String("method", r.Method))
			return true, addr
		}
	}
	return false, ""
}

func (h *redirector) ServeHTTP(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	redirectToMicroService, targetAddr := h.matchMicroServiceRedirectRules(r)
	allowFollowerHandle := len(r.Header.Get(apiutil.PDAllowFollowerHandleHeader)) > 0

	if h.s.IsClosed() {
		http.Error(w, errs.ErrServerNotStarted.FastGenByArgs().Error(), http.StatusInternalServerError)
		return
	}

	if (allowFollowerHandle || h.s.GetMember().IsLeader()) && !redirectToMicroService {
		next(w, r)
		return
	}

	forwardedIP, forwardedPort := apiutil.GetIPPortFromHTTPRequest(r)
	if len(forwardedIP) > 0 {
		r.Header.Add(apiutil.XForwardedForHeader, forwardedIP)
	} else {
		// Fallback if GetIPPortFromHTTPRequest failed to get the IP.
		r.Header.Add(apiutil.XForwardedForHeader, r.RemoteAddr)
	}
	if len(forwardedPort) > 0 {
		r.Header.Add(apiutil.XForwardedPortHeader, forwardedPort)
	}

	var clientUrls []string
	if redirectToMicroService {
		if len(targetAddr) == 0 {
			http.Error(w, errs.ErrRedirect.FastGenByArgs().Error(), http.StatusInternalServerError)
			return
		}
		clientUrls = append(clientUrls, targetAddr)
		// Add a header to the response, this is not a failure injection
		// it is used for testing, to check whether the request is forwarded to the micro service
		failpoint.Inject("checkHeader", func() {
			w.Header().Set(apiutil.ForwardToMicroServiceHeader, "true")
		})
	} else {
		leader := h.s.GetMember().GetLeader()
		if leader == nil {
			http.Error(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		clientUrls = leader.GetClientUrls()
		// Prevent more than one redirection among PD/API servers.
		if name := r.Header.Get(apiutil.PDRedirectorHeader); len(name) != 0 {
			log.Error("redirect but server is not leader", zap.String("from", name), zap.String("server", h.s.Name()), errs.ZapError(errs.ErrRedirect))
			http.Error(w, errs.ErrRedirectToNotLeader.FastGenByArgs().Error(), http.StatusInternalServerError)
			return
		}
		r.Header.Set(apiutil.PDRedirectorHeader, h.s.Name())
	}

	urls := make([]url.URL, 0, len(clientUrls))
	for _, item := range clientUrls {
		u, err := url.Parse(item)
		if err != nil {
			http.Error(w, errs.ErrURLParse.Wrap(err).GenWithStackByCause().Error(), http.StatusInternalServerError)
			return
		}

		urls = append(urls, *u)
	}
	client := h.s.GetHTTPClient()
	apiutil.NewCustomReverseProxies(client, urls).ServeHTTP(w, r)
}
