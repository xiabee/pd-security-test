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

package server

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/mcs/registry"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gRPC errors
var (
	ErrNotStarted = status.Errorf(codes.Unavailable, "server not started")
)

var _ tsopb.TSOServer = (*Service)(nil)

// SetUpRestHandler is a hook to sets up the REST service.
var SetUpRestHandler = func(srv *Service) (http.Handler, apiutil.APIServiceGroup) {
	return dummyRestService{}, apiutil.APIServiceGroup{}
}

type dummyRestService struct{}

func (d dummyRestService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("not implemented"))
}

// Service is the TSO grpc service.
type Service struct {
	*Server
}

// NewService creates a new TSO service.
func NewService(svr bs.Server) registry.RegistrableService {
	server, ok := svr.(*Server)
	if !ok {
		log.Fatal("create tso server failed")
	}
	return &Service{
		Server: server,
	}
}

// RegisterGRPCService registers the service to gRPC server.
func (s *Service) RegisterGRPCService(g *grpc.Server) {
	tsopb.RegisterTSOServer(g, s)
}

// RegisterRESTHandler registers the service to REST server.
func (s *Service) RegisterRESTHandler(userDefineHandlers map[string]http.Handler) {
	handler, group := SetUpRestHandler(s)
	apiutil.RegisterUserDefinedHandlers(userDefineHandlers, &group, handler)
}

// Tso returns a stream of timestamps
func (s *Service) Tso(stream tsopb.TSO_TsoServer) error {
	var (
		doneCh chan struct{}
		errCh  chan error
	)
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	for {
		// Prevent unnecessary performance overhead of the channel.
		if errCh != nil {
			select {
			case err := <-errCh:
				return errors.WithStack(err)
			default:
			}
		}
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}

		streamCtx := stream.Context()
		forwardedHost := grpcutil.GetForwardedHost(streamCtx)
		if !s.IsLocalRequest(forwardedHost) {
			clientConn, err := s.GetDelegateClient(s.ctx, forwardedHost)
			if err != nil {
				return errors.WithStack(err)
			}

			if errCh == nil {
				doneCh = make(chan struct{})
				defer close(doneCh)
				errCh = make(chan error)
			}

			tsoProtoFactory := s.tsoProtoFactory
			tsoRequest := tsoutil.NewTSOProtoRequest(forwardedHost, clientConn, request, stream)
			s.tsoDispatcher.DispatchRequest(ctx, tsoRequest, tsoProtoFactory, doneCh, errCh)
			continue
		}

		start := time.Now()
		// TSO uses leader lease to determine validity. No need to check leader here.
		if s.IsClosed() {
			return status.Errorf(codes.Unknown, "server not started")
		}
		if request.GetHeader().GetClusterId() != s.clusterID {
			return status.Errorf(
				codes.FailedPrecondition, "mismatch cluster id, need %d but got %d",
				s.clusterID, request.GetHeader().GetClusterId())
		}
		count := request.GetCount()
		ts, keyspaceGroupBelongTo, err := s.keyspaceGroupManager.HandleTSORequest(
			request.Header.KeyspaceId, request.Header.KeyspaceGroupId, request.GetDcLocation(), count)
		if err != nil {
			return status.Errorf(codes.Unknown, err.Error())
		}
		tsoHandleDuration.Observe(time.Since(start).Seconds())
		response := &tsopb.TsoResponse{
			Header:    s.header(keyspaceGroupBelongTo),
			Timestamp: &ts,
			Count:     count,
		}
		if err := stream.Send(response); err != nil {
			return errors.WithStack(err)
		}
	}
}

func (s *Service) header(keyspaceGroupBelongTo uint32) *tsopb.ResponseHeader {
	if s.clusterID == 0 {
		return s.wrapErrorToHeader(
			tsopb.ErrorType_NOT_BOOTSTRAPPED, "cluster id is not ready", keyspaceGroupBelongTo)
	}
	return &tsopb.ResponseHeader{ClusterId: s.clusterID, KeyspaceGroupId: keyspaceGroupBelongTo}
}

func (s *Service) wrapErrorToHeader(
	errorType tsopb.ErrorType, message string, keyspaceGroupBelongTo uint32,
) *tsopb.ResponseHeader {
	return s.errorHeader(&tsopb.Error{Type: errorType, Message: message}, keyspaceGroupBelongTo)
}

func (s *Service) errorHeader(err *tsopb.Error, keyspaceGroupBelongTo uint32) *tsopb.ResponseHeader {
	return &tsopb.ResponseHeader{
		ClusterId:       s.clusterID,
		Error:           err,
		KeyspaceGroupId: keyspaceGroupBelongTo,
	}
}
