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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/schedulingpb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *GrpcServer) forwardTSORequest(
	ctx context.Context,
	request *pdpb.TsoRequest,
	forwardStream tsopb.TSO_TsoClient) (*tsopb.TsoResponse, error) {
	tsopbReq := &tsopb.TsoRequest{
		Header: &tsopb.RequestHeader{
			ClusterId:       request.GetHeader().GetClusterId(),
			SenderId:        request.GetHeader().GetSenderId(),
			KeyspaceId:      utils.DefaultKeyspaceID,
			KeyspaceGroupId: utils.DefaultKeyspaceGroupID,
		},
		Count:      request.GetCount(),
		DcLocation: request.GetDcLocation(),
	}

	failpoint.Inject("tsoProxySendToTSOTimeout", func() {
		// block until watchDeadline routine cancels the context.
		<-ctx.Done()
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if err := forwardStream.Send(tsopbReq); err != nil {
		return nil, err
	}

	failpoint.Inject("tsoProxyRecvFromTSOTimeout", func() {
		// block until watchDeadline routine cancels the context.
		<-ctx.Done()
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return forwardStream.Recv()
}

// forwardTSO forward the TSO requests to the TSO service.
func (s *GrpcServer) forwardTSO(stream pdpb.PD_TsoServer) error {
	var (
		server            = &tsoServer{stream: stream}
		forwardStream     tsopb.TSO_TsoClient
		forwardCtx        context.Context
		cancelForward     context.CancelFunc
		lastForwardedHost string
	)
	defer func() {
		s.concurrentTSOProxyStreamings.Add(-1)
		if cancelForward != nil {
			cancelForward()
		}
	}()

	maxConcurrentTSOProxyStreamings := int32(s.GetMaxConcurrentTSOProxyStreamings())
	if maxConcurrentTSOProxyStreamings >= 0 {
		if newCount := s.concurrentTSOProxyStreamings.Add(1); newCount > maxConcurrentTSOProxyStreamings {
			return errors.WithStack(ErrMaxCountTSOProxyRoutinesExceeded)
		}
	}

	tsDeadlineCh := make(chan *tsoutil.TSDeadline, 1)
	go tsoutil.WatchTSDeadline(stream.Context(), tsDeadlineCh)

	for {
		select {
		case <-s.ctx.Done():
			return errors.WithStack(s.ctx.Err())
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
		}

		request, err := server.Recv(s.GetTSOProxyRecvFromClientTimeout())
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		if request.GetCount() == 0 {
			err = errs.ErrGenerateTimestamp.FastGenByArgs("tso count should be positive")
			return status.Errorf(codes.Unknown, err.Error())
		}

		forwardedHost, ok := s.GetServicePrimaryAddr(stream.Context(), utils.TSOServiceName)
		if !ok || len(forwardedHost) == 0 {
			return errors.WithStack(ErrNotFoundTSOAddr)
		}
		if forwardStream == nil || lastForwardedHost != forwardedHost {
			if cancelForward != nil {
				cancelForward()
			}

			clientConn, err := s.getDelegateClient(s.ctx, forwardedHost)
			if err != nil {
				return errors.WithStack(err)
			}
			forwardStream, forwardCtx, cancelForward, err = s.createTSOForwardStream(stream.Context(), clientConn)
			if err != nil {
				return errors.WithStack(err)
			}
			lastForwardedHost = forwardedHost
		}

		tsopbResp, err := s.forwardTSORequestWithDeadLine(forwardCtx, cancelForward, forwardStream, request, tsDeadlineCh)
		if err != nil {
			return errors.WithStack(err)
		}

		// The error types defined for tsopb and pdpb are different, so we need to convert them.
		var pdpbErr *pdpb.Error
		tsopbErr := tsopbResp.GetHeader().GetError()
		if tsopbErr != nil {
			if tsopbErr.Type == tsopb.ErrorType_OK {
				pdpbErr = &pdpb.Error{
					Type:    pdpb.ErrorType_OK,
					Message: tsopbErr.GetMessage(),
				}
			} else {
				// TODO: specify FORWARD FAILURE error type instead of UNKNOWN.
				pdpbErr = &pdpb.Error{
					Type:    pdpb.ErrorType_UNKNOWN,
					Message: tsopbErr.GetMessage(),
				}
			}
		}

		response := &pdpb.TsoResponse{
			Header: &pdpb.ResponseHeader{
				ClusterId: tsopbResp.GetHeader().GetClusterId(),
				Error:     pdpbErr,
			},
			Count:     tsopbResp.GetCount(),
			Timestamp: tsopbResp.GetTimestamp(),
		}
		if err := server.Send(response); err != nil {
			return errors.WithStack(err)
		}
	}
}

func (s *GrpcServer) forwardTSORequestWithDeadLine(
	forwardCtx context.Context,
	cancelForward context.CancelFunc,
	forwardStream tsopb.TSO_TsoClient,
	request *pdpb.TsoRequest,
	tsDeadlineCh chan<- *tsoutil.TSDeadline) (*tsopb.TsoResponse, error) {
	done := make(chan struct{})
	dl := tsoutil.NewTSDeadline(tsoutil.DefaultTSOProxyTimeout, done, cancelForward)
	select {
	case tsDeadlineCh <- dl:
	case <-forwardCtx.Done():
		return nil, forwardCtx.Err()
	}

	start := time.Now()
	resp, err := s.forwardTSORequest(forwardCtx, request, forwardStream)
	close(done)
	if err != nil {
		if strings.Contains(err.Error(), errs.NotLeaderErr) {
			s.tsoPrimaryWatcher.ForceLoad()
		}
		return nil, err
	}
	tsoProxyBatchSize.Observe(float64(request.GetCount()))
	tsoProxyHandleDuration.Observe(time.Since(start).Seconds())
	return resp, nil
}

func (s *GrpcServer) createTSOForwardStream(ctx context.Context, client *grpc.ClientConn) (tsopb.TSO_TsoClient, context.Context, context.CancelFunc, error) {
	done := make(chan struct{})
	forwardCtx, cancelForward := context.WithCancel(ctx)
	go grpcutil.CheckStream(forwardCtx, cancelForward, done)
	forwardStream, err := tsopb.NewTSOClient(client).Tso(forwardCtx)
	done <- struct{}{}
	return forwardStream, forwardCtx, cancelForward, err
}

func (s *GrpcServer) createRegionHeartbeatForwardStream(client *grpc.ClientConn) (pdpb.PD_RegionHeartbeatClient, context.CancelFunc, error) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(s.ctx)
	go grpcutil.CheckStream(ctx, cancel, done)
	forwardStream, err := pdpb.NewPDClient(client).RegionHeartbeat(ctx)
	done <- struct{}{}
	return forwardStream, cancel, err
}

func (s *GrpcServer) createRegionHeartbeatSchedulingStream(ctx context.Context, client *grpc.ClientConn) (schedulingpb.Scheduling_RegionHeartbeatClient, context.Context, context.CancelFunc, error) {
	done := make(chan struct{})
	forwardCtx, cancelForward := context.WithCancel(ctx)
	go grpcutil.CheckStream(forwardCtx, cancelForward, done)
	forwardStream, err := schedulingpb.NewSchedulingClient(client).RegionHeartbeat(forwardCtx)
	done <- struct{}{}
	return forwardStream, forwardCtx, cancelForward, err
}

func forwardRegionHeartbeatToScheduling(forwardStream schedulingpb.Scheduling_RegionHeartbeatClient, server *heartbeatServer, errCh chan error) {
	defer logutil.LogPanic()
	defer close(errCh)
	for {
		resp, err := forwardStream.Recv()
		if err == io.EOF {
			errCh <- errors.WithStack(err)
			return
		}
		if err != nil {
			errCh <- errors.WithStack(err)
			return
		}
		// The error types defined for schedulingpb and pdpb are different, so we need to convert them.
		var pdpbErr *pdpb.Error
		schedulingpbErr := resp.GetHeader().GetError()
		if schedulingpbErr != nil {
			if schedulingpbErr.Type == schedulingpb.ErrorType_OK {
				pdpbErr = &pdpb.Error{
					Type:    pdpb.ErrorType_OK,
					Message: schedulingpbErr.GetMessage(),
				}
			} else {
				// TODO: specify FORWARD FAILURE error type instead of UNKNOWN.
				pdpbErr = &pdpb.Error{
					Type:    pdpb.ErrorType_UNKNOWN,
					Message: schedulingpbErr.GetMessage(),
				}
			}
		}
		response := &pdpb.RegionHeartbeatResponse{
			Header: &pdpb.ResponseHeader{
				ClusterId: resp.GetHeader().GetClusterId(),
				Error:     pdpbErr,
			},
			ChangePeer:      resp.GetChangePeer(),
			TransferLeader:  resp.GetTransferLeader(),
			RegionId:        resp.GetRegionId(),
			RegionEpoch:     resp.GetRegionEpoch(),
			TargetPeer:      resp.GetTargetPeer(),
			Merge:           resp.GetMerge(),
			SplitRegion:     resp.GetSplitRegion(),
			ChangePeerV2:    resp.GetChangePeerV2(),
			SwitchWitnesses: resp.GetSwitchWitnesses(),
		}

		if err := server.Send(response); err != nil {
			errCh <- errors.WithStack(err)
			return
		}
	}
}

func forwardRegionHeartbeatClientToServer(forwardStream pdpb.PD_RegionHeartbeatClient, server *heartbeatServer, errCh chan error) {
	defer logutil.LogPanic()
	defer close(errCh)
	for {
		resp, err := forwardStream.Recv()
		if err != nil {
			errCh <- errors.WithStack(err)
			return
		}
		if err := server.Send(resp); err != nil {
			errCh <- errors.WithStack(err)
			return
		}
	}
}

func forwardReportBucketClientToServer(forwardStream pdpb.PD_ReportBucketsClient, server *bucketHeartbeatServer, errCh chan error) {
	defer logutil.LogPanic()
	defer close(errCh)
	for {
		resp, err := forwardStream.CloseAndRecv()
		if err != nil {
			errCh <- errors.WithStack(err)
			return
		}
		if err := server.Send(resp); err != nil {
			errCh <- errors.WithStack(err)
			return
		}
	}
}

func (s *GrpcServer) createReportBucketsForwardStream(client *grpc.ClientConn) (pdpb.PD_ReportBucketsClient, context.CancelFunc, error) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(s.ctx)
	go grpcutil.CheckStream(ctx, cancel, done)
	forwardStream, err := pdpb.NewPDClient(client).ReportBuckets(ctx)
	done <- struct{}{}
	return forwardStream, cancel, err
}

func (s *GrpcServer) getDelegateClient(ctx context.Context, forwardedHost string) (*grpc.ClientConn, error) {
	client, ok := s.clientConns.Load(forwardedHost)
	if ok {
		// Mostly, the connection is already established, and return it directly.
		return client.(*grpc.ClientConn), nil
	}

	tlsConfig, err := s.GetTLSConfig().ToTLSConfig()
	if err != nil {
		return nil, err
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, defaultGRPCDialTimeout)
	defer cancel()
	newConn, err := grpcutil.GetClientConn(ctxTimeout, forwardedHost, tlsConfig)
	if err != nil {
		return nil, err
	}
	conn, loaded := s.clientConns.LoadOrStore(forwardedHost, newConn)
	if !loaded {
		// Successfully stored the connection we created.
		return newConn, nil
	}
	// Loaded a connection created/stored by another goroutine, so close the one we created
	// and return the one we loaded.
	newConn.Close()
	return conn.(*grpc.ClientConn), nil
}

func (s *GrpcServer) getForwardedHost(ctx, streamCtx context.Context, serviceName ...string) (forwardedHost string, err error) {
	if s.IsAPIServiceMode() {
		var ok bool
		if len(serviceName) == 0 {
			return "", ErrNotFoundService
		}
		forwardedHost, ok = s.GetServicePrimaryAddr(ctx, serviceName[0])
		if !ok || len(forwardedHost) == 0 {
			switch serviceName[0] {
			case utils.TSOServiceName:
				return "", ErrNotFoundTSOAddr
			case utils.SchedulingServiceName:
				return "", ErrNotFoundSchedulingAddr
			}
		}
	} else if fh := grpcutil.GetForwardedHost(streamCtx); !s.isLocalRequest(fh) {
		forwardedHost = fh
	}
	return forwardedHost, nil
}

func (s *GrpcServer) isLocalRequest(host string) bool {
	failpoint.Inject("useForwardRequest", func() {
		failpoint.Return(false)
	})
	if host == "" {
		return true
	}
	memberAddrs := s.GetMember().Member().GetClientUrls()
	for _, addr := range memberAddrs {
		if addr == host {
			return true
		}
	}
	return false
}

func (s *GrpcServer) getGlobalTSO(ctx context.Context) (pdpb.Timestamp, error) {
	if !s.IsAPIServiceMode() {
		return s.tsoAllocatorManager.HandleRequest(ctx, tso.GlobalDCLocation, 1)
	}
	request := &tsopb.TsoRequest{
		Header: &tsopb.RequestHeader{
			ClusterId:       s.clusterID,
			KeyspaceId:      utils.DefaultKeyspaceID,
			KeyspaceGroupId: utils.DefaultKeyspaceGroupID,
		},
		Count: 1,
	}
	var (
		forwardedHost string
		forwardStream tsopb.TSO_TsoClient
		ts            *tsopb.TsoResponse
		err           error
		ok            bool
	)
	handleStreamError := func(err error) (needRetry bool) {
		if strings.Contains(err.Error(), errs.NotLeaderErr) {
			s.tsoPrimaryWatcher.ForceLoad()
			log.Warn("force to load tso primary address due to error", zap.Error(err), zap.String("tso-addr", forwardedHost))
			return true
		}
		if grpcutil.NeedRebuildConnection(err) {
			s.tsoClientPool.Lock()
			delete(s.tsoClientPool.clients, forwardedHost)
			s.tsoClientPool.Unlock()
			log.Warn("client connection removed due to error", zap.Error(err), zap.String("tso-addr", forwardedHost))
			return true
		}
		return false
	}
	for i := 0; i < maxRetryTimesRequestTSOServer; i++ {
		if i > 0 {
			time.Sleep(retryIntervalRequestTSOServer)
		}
		forwardedHost, ok = s.GetServicePrimaryAddr(ctx, utils.TSOServiceName)
		if !ok || forwardedHost == "" {
			return pdpb.Timestamp{}, ErrNotFoundTSOAddr
		}
		forwardStream, err = s.getTSOForwardStream(forwardedHost)
		if err != nil {
			return pdpb.Timestamp{}, err
		}
		err = forwardStream.Send(request)
		if err != nil {
			if needRetry := handleStreamError(err); needRetry {
				continue
			}
			log.Error("send request to tso primary server failed", zap.Error(err), zap.String("tso-addr", forwardedHost))
			return pdpb.Timestamp{}, err
		}
		ts, err = forwardStream.Recv()
		if err != nil {
			if needRetry := handleStreamError(err); needRetry {
				continue
			}
			log.Error("receive response from tso primary server failed", zap.Error(err), zap.String("tso-addr", forwardedHost))
			return pdpb.Timestamp{}, err
		}
		return *ts.GetTimestamp(), nil
	}
	log.Error("get global tso from tso primary server failed after retry", zap.Error(err), zap.String("tso-addr", forwardedHost))
	return pdpb.Timestamp{}, err
}

func (s *GrpcServer) getTSOForwardStream(forwardedHost string) (tsopb.TSO_TsoClient, error) {
	s.tsoClientPool.RLock()
	forwardStream, ok := s.tsoClientPool.clients[forwardedHost]
	s.tsoClientPool.RUnlock()
	if ok {
		// This is the common case to return here
		return forwardStream, nil
	}

	s.tsoClientPool.Lock()
	defer s.tsoClientPool.Unlock()

	// Double check after entering the critical section
	forwardStream, ok = s.tsoClientPool.clients[forwardedHost]
	if ok {
		return forwardStream, nil
	}

	// Now let's create the client connection and the forward stream
	client, err := s.getDelegateClient(s.ctx, forwardedHost)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(s.ctx)
	go grpcutil.CheckStream(ctx, cancel, done)
	forwardStream, err = tsopb.NewTSOClient(client).Tso(ctx)
	done <- struct{}{}
	if err != nil {
		return nil, err
	}
	s.tsoClientPool.clients[forwardedHost] = forwardStream
	return forwardStream, nil
}
