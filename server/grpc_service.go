// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/grpcutil"
	"github.com/tikv/pd/pkg/logutil"
	"github.com/tikv/pd/pkg/tsoutil"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/tso"
	"github.com/tikv/pd/server/versioninfo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	heartbeatSendTimeout = 5 * time.Second
	slowThreshold        = 5 * time.Millisecond
)

// gRPC errors
var (
	// ErrNotLeader is returned when current server is not the leader and not possible to process request.
	// TODO: work as proxy.
	ErrNotLeader            = status.Errorf(codes.Unavailable, "not leader")
	ErrNotStarted           = status.Errorf(codes.Unavailable, "server not started")
	ErrSendHeartbeatTimeout = status.Errorf(codes.DeadlineExceeded, "send heartbeat timeout")
)

// GetMembers implements gRPC PDServer.
func (s *Server) GetMembers(context.Context, *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	if s.IsClosed() {
		return nil, status.Errorf(codes.Unknown, "server not started")
	}
	members, err := cluster.GetMembers(s.GetClient())
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	var etcdLeader, pdLeader *pdpb.Member
	leadID := s.member.GetEtcdLeader()
	for _, m := range members {
		if m.MemberId == leadID {
			etcdLeader = m
			break
		}
	}

	tsoAllocatorManager := s.GetTSOAllocatorManager()
	tsoAllocatorLeaders, err := tsoAllocatorManager.GetLocalAllocatorLeaders()
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	leader := s.member.GetLeader()
	for _, m := range members {
		if m.MemberId == leader.GetMemberId() {
			pdLeader = m
			break
		}
	}

	return &pdpb.GetMembersResponse{
		Header:              s.header(),
		Members:             members,
		Leader:              pdLeader,
		EtcdLeader:          etcdLeader,
		TsoAllocatorLeaders: tsoAllocatorLeaders,
	}, nil
}

// Tso implements gRPC PDServer.
func (s *Server) Tso(stream pdpb.PD_TsoServer) error {
	var (
		forwardStream     pdpb.PD_TsoClient
		cancel            context.CancelFunc
		lastForwardedHost string
	)
	defer func() {
		// cancel the forward stream
		if cancel != nil {
			cancel()
		}
	}()
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}

		forwardedHost := getForwardedHost(stream.Context())
		if !s.isLocalRequest(forwardedHost) {
			if forwardStream == nil || lastForwardedHost != forwardedHost {
				if cancel != nil {
					cancel()
				}
				client, err := s.getDelegateClient(s.ctx, forwardedHost)
				if err != nil {
					return err
				}
				// TODO: change it to the info level once the TiKV doesn't use it in a unary way.
				log.Debug("create TSO forward stream", zap.String("forwarded-host", forwardedHost))
				forwardStream, cancel, err = s.createTsoForwardStream(client)
				if err != nil {
					return err
				}
				lastForwardedHost = forwardedHost
			}

			// In order to avoid the forwarding stream being canceled, we are not going to handle the EOF before the
			// forwarding stream responds.
			// TODO: we should change this part of logic once the TiKV uses this RPC call in the streaming way.
			if err := forwardStream.Send(request); err != nil {
				return errors.WithStack(err)
			}
			resp, err := forwardStream.Recv()
			if err != nil {
				return errors.WithStack(err)
			}
			if err := stream.Send(resp); err != nil {
				return errors.WithStack(err)
			}
			continue
		}

		start := time.Now()
		// TSO uses leader lease to determine validity. No need to check leader here.
		if s.IsClosed() {
			return status.Errorf(codes.Unknown, "server not started")
		}
		if request.GetHeader().GetClusterId() != s.clusterID {
			return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.clusterID, request.GetHeader().GetClusterId())
		}
		count := request.GetCount()
		ts, err := s.tsoAllocatorManager.HandleTSORequest(request.GetDcLocation(), count)
		if err != nil {
			return status.Errorf(codes.Unknown, err.Error())
		}

		elapsed := time.Since(start)
		if elapsed > slowThreshold {
			log.Warn("get timestamp too slow", zap.Duration("cost", elapsed))
		}
		tsoHandleDuration.Observe(time.Since(start).Seconds())
		response := &pdpb.TsoResponse{
			Header:    s.header(),
			Timestamp: &ts,
			Count:     count,
		}
		if err := stream.Send(response); err != nil {
			return errors.WithStack(err)
		}
	}
}

// Bootstrap implements gRPC PDServer.
func (s *Server) Bootstrap(ctx context.Context, request *pdpb.BootstrapRequest) (*pdpb.BootstrapResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).Bootstrap(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc != nil {
		err := &pdpb.Error{
			Type:    pdpb.ErrorType_ALREADY_BOOTSTRAPPED,
			Message: "cluster is already bootstrapped",
		}
		return &pdpb.BootstrapResponse{
			Header: s.errorHeader(err),
		}, nil
	}

	res, err := s.bootstrapCluster(request)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	res.Header = s.header()
	return res, nil
}

// IsBootstrapped implements gRPC PDServer.
func (s *Server) IsBootstrapped(ctx context.Context, request *pdpb.IsBootstrappedRequest) (*pdpb.IsBootstrappedResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).IsBootstrapped(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	return &pdpb.IsBootstrappedResponse{
		Header:       s.header(),
		Bootstrapped: rc != nil,
	}, nil
}

// AllocID implements gRPC PDServer.
func (s *Server) AllocID(ctx context.Context, request *pdpb.AllocIDRequest) (*pdpb.AllocIDResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).AllocID(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	// We can use an allocator for all types ID allocation.
	id, err := s.idAllocator.Alloc()
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb.AllocIDResponse{
		Header: s.header(),
		Id:     id,
	}, nil
}

// GetStore implements gRPC PDServer.
func (s *Server) GetStore(ctx context.Context, request *pdpb.GetStoreRequest) (*pdpb.GetStoreResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).GetStore(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetStoreResponse{Header: s.notBootstrappedHeader()}, nil
	}

	storeID := request.GetStoreId()
	store := rc.GetStore(storeID)
	if store == nil {
		return nil, status.Errorf(codes.Unknown, "invalid store ID %d, not found", storeID)
	}
	return &pdpb.GetStoreResponse{
		Header: s.header(),
		Store:  store.GetMeta(),
		Stats:  store.GetStoreStats(),
	}, nil
}

// checkStore returns an error response if the store exists and is in tombstone state.
// It returns nil if it can't get the store.
func checkStore(rc *cluster.RaftCluster, storeID uint64) *pdpb.Error {
	store := rc.GetStore(storeID)
	if store != nil {
		if store.GetState() == metapb.StoreState_Tombstone {
			return &pdpb.Error{
				Type:    pdpb.ErrorType_STORE_TOMBSTONE,
				Message: "store is tombstone",
			}
		}
	}
	return nil
}

// PutStore implements gRPC PDServer.
func (s *Server) PutStore(ctx context.Context, request *pdpb.PutStoreRequest) (*pdpb.PutStoreResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).PutStore(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.PutStoreResponse{Header: s.notBootstrappedHeader()}, nil
	}

	store := request.GetStore()
	if pberr := checkStore(rc, store.GetId()); pberr != nil {
		return &pdpb.PutStoreResponse{
			Header: s.errorHeader(pberr),
		}, nil
	}

	// NOTE: can be removed when placement rules feature is enabled by default.
	if !s.GetConfig().Replication.EnablePlacementRules && core.IsTiFlashStore(store) {
		return nil, status.Errorf(codes.FailedPrecondition, "placement rules is disabled")
	}

	if err := rc.PutStore(store); err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	log.Info("put store ok", zap.Stringer("store", store))
	CheckPDVersion(s.persistOptions)

	return &pdpb.PutStoreResponse{
		Header:            s.header(),
		ReplicationStatus: rc.GetReplicationMode().GetReplicationStatus(),
	}, nil
}

// GetAllStores implements gRPC PDServer.
func (s *Server) GetAllStores(ctx context.Context, request *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).GetAllStores(ctx, request)
	}

	failpoint.Inject("customTimeout", func() {
		time.Sleep(5 * time.Second)
	})
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetAllStoresResponse{Header: s.notBootstrappedHeader()}, nil
	}

	// Don't return tombstone stores.
	var stores []*metapb.Store
	if request.GetExcludeTombstoneStores() {
		for _, store := range rc.GetMetaStores() {
			if store.GetState() != metapb.StoreState_Tombstone {
				stores = append(stores, store)
			}
		}
	} else {
		stores = rc.GetMetaStores()
	}

	return &pdpb.GetAllStoresResponse{
		Header: s.header(),
		Stores: stores,
	}, nil
}

// StoreHeartbeat implements gRPC PDServer.
func (s *Server) StoreHeartbeat(ctx context.Context, request *pdpb.StoreHeartbeatRequest) (*pdpb.StoreHeartbeatResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).StoreHeartbeat(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	if request.GetStats() == nil {
		return nil, errors.Errorf("invalid store heartbeat command, but %v", request)
	}
	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.StoreHeartbeatResponse{Header: s.notBootstrappedHeader()}, nil
	}

	if pberr := checkStore(rc, request.GetStats().GetStoreId()); pberr != nil {
		return &pdpb.StoreHeartbeatResponse{
			Header: s.errorHeader(pberr),
		}, nil
	}

	storeID := request.Stats.GetStoreId()
	store := rc.GetStore(storeID)
	if store == nil {
		return nil, errors.Errorf("store %v not found", storeID)
	}

	storeAddress := store.GetAddress()
	storeLabel := strconv.FormatUint(storeID, 10)
	start := time.Now()

	err := rc.HandleStoreHeartbeat(request.Stats)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	storeHeartbeatHandleDuration.WithLabelValues(storeAddress, storeLabel).Observe(time.Since(start).Seconds())

	return &pdpb.StoreHeartbeatResponse{
		Header:            s.header(),
		ReplicationStatus: rc.GetReplicationMode().GetReplicationStatus(),
		ClusterVersion:    rc.GetClusterVersion(),
	}, nil
}

// heartbeatServer wraps PD_RegionHeartbeatServer to ensure when any error
// occurs on Send() or Recv(), both endpoints will be closed.
type heartbeatServer struct {
	stream pdpb.PD_RegionHeartbeatServer
	closed int32
}

func (s *heartbeatServer) Send(m *pdpb.RegionHeartbeatResponse) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return io.EOF
	}
	done := make(chan error, 1)
	go func() { done <- s.stream.Send(m) }()
	select {
	case err := <-done:
		if err != nil {
			atomic.StoreInt32(&s.closed, 1)
		}
		return errors.WithStack(err)
	case <-time.After(heartbeatSendTimeout):
		atomic.StoreInt32(&s.closed, 1)
		return ErrSendHeartbeatTimeout
	}
}

func (s *heartbeatServer) Recv() (*pdpb.RegionHeartbeatRequest, error) {
	if atomic.LoadInt32(&s.closed) == 1 {
		return nil, io.EOF
	}
	req, err := s.stream.Recv()
	if err != nil {
		atomic.StoreInt32(&s.closed, 1)
		return nil, errors.WithStack(err)
	}
	return req, nil
}

// RegionHeartbeat implements gRPC PDServer.
func (s *Server) RegionHeartbeat(stream pdpb.PD_RegionHeartbeatServer) error {
	var (
		server            = &heartbeatServer{stream: stream}
		flowRoundOption   = core.WithFlowRoundByDigit(s.persistOptions.GetPDServerConfig().FlowRoundByDigit)
		forwardStream     pdpb.PD_RegionHeartbeatClient
		cancel            context.CancelFunc
		lastForwardedHost string
		lastBind          time.Time
		errCh             chan error
	)
	defer func() {
		// cancel the forward stream
		if cancel != nil {
			cancel()
		}
	}()

	for {
		request, err := server.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}

		forwardedHost := getForwardedHost(stream.Context())
		if !s.isLocalRequest(forwardedHost) {
			if forwardStream == nil || lastForwardedHost != forwardedHost {
				if cancel != nil {
					cancel()
				}
				client, err := s.getDelegateClient(s.ctx, forwardedHost)
				if err != nil {
					return err
				}
				log.Info("create region heartbeat forward stream", zap.String("forwarded-host", forwardedHost))
				forwardStream, cancel, err = s.createHeartbeatForwardStream(client)
				if err != nil {
					return err
				}
				lastForwardedHost = forwardedHost
				errCh = make(chan error, 1)
				go forwardRegionHeartbeatClientToServer(forwardStream, server, errCh)
			}
			if err := forwardStream.Send(request); err != nil {
				return errors.WithStack(err)
			}

			select {
			case err := <-errCh:
				return err
			default:
			}
			continue
		}

		rc := s.GetRaftCluster()
		if rc == nil {
			resp := &pdpb.RegionHeartbeatResponse{
				Header: s.notBootstrappedHeader(),
			}
			err := server.Send(resp)
			return errors.WithStack(err)
		}

		if err = s.validateRequest(request.GetHeader()); err != nil {
			return err
		}

		storeID := request.GetLeader().GetStoreId()
		storeLabel := strconv.FormatUint(storeID, 10)
		store := rc.GetStore(storeID)
		if store == nil {
			return errors.Errorf("invalid store ID %d, not found", storeID)
		}
		storeAddress := store.GetAddress()

		regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "recv").Inc()
		regionHeartbeatLatency.WithLabelValues(storeAddress, storeLabel).Observe(float64(time.Now().Unix()) - float64(request.GetInterval().GetEndTimestamp()))

		if time.Since(lastBind) > s.cfg.HeartbeatStreamBindInterval.Duration {
			regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "bind").Inc()
			s.hbStreams.BindStream(storeID, server)
			// refresh FlowRoundByDigit
			flowRoundOption = core.WithFlowRoundByDigit(s.persistOptions.GetPDServerConfig().FlowRoundByDigit)
			lastBind = time.Now()
		}

		region := core.RegionFromHeartbeat(request, flowRoundOption, core.SetFromHeartbeat(true))
		if region.GetLeader() == nil {
			log.Error("invalid request, the leader is nil", zap.Reflect("request", request), errs.ZapError(errs.ErrLeaderNil))
			regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "invalid-leader").Inc()
			msg := fmt.Sprintf("invalid request leader, %v", request)
			s.hbStreams.SendErr(pdpb.ErrorType_UNKNOWN, msg, request.GetLeader())
			continue
		}
		if region.GetID() == 0 {
			regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "invalid-region").Inc()
			msg := fmt.Sprintf("invalid request region, %v", request)
			s.hbStreams.SendErr(pdpb.ErrorType_UNKNOWN, msg, request.GetLeader())
			continue
		}

		// If the region peer count is 0, then we should not handle this.
		if len(region.GetPeers()) == 0 {
			log.Warn("invalid region, zero region peer count",
				logutil.ZapRedactStringer("region-meta", core.RegionToHexMeta(region.GetMeta())))
			regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "no-peer").Inc()
			msg := fmt.Sprintf("invalid region, zero region peer count: %v", logutil.RedactStringer(core.RegionToHexMeta(region.GetMeta())))
			s.hbStreams.SendErr(pdpb.ErrorType_UNKNOWN, msg, request.GetLeader())
			continue
		}
		start := time.Now()

		err = rc.HandleRegionHeartbeat(region)
		if err != nil {
			regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "err").Inc()
			msg := err.Error()
			s.hbStreams.SendErr(pdpb.ErrorType_UNKNOWN, msg, request.GetLeader())
			continue
		}
		regionHeartbeatHandleDuration.WithLabelValues(storeAddress, storeLabel).Observe(time.Since(start).Seconds())
		regionHeartbeatCounter.WithLabelValues(storeAddress, storeLabel, "report", "ok").Inc()
	}
}

// GetRegion implements gRPC PDServer.
func (s *Server) GetRegion(ctx context.Context, request *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).GetRegion(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}
	region := rc.GetRegionByKey(request.GetRegionKey())
	if region == nil {
		return &pdpb.GetRegionResponse{Header: s.header()}, nil
	}
	return &pdpb.GetRegionResponse{
		Header:       s.header(),
		Region:       region.GetMeta(),
		Leader:       region.GetLeader(),
		DownPeers:    region.GetDownPeers(),
		PendingPeers: region.GetPendingPeers(),
	}, nil
}

// GetPrevRegion implements gRPC PDServer
func (s *Server) GetPrevRegion(ctx context.Context, request *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).GetPrevRegion(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}

	region := rc.GetPrevRegionByKey(request.GetRegionKey())
	if region == nil {
		return &pdpb.GetRegionResponse{Header: s.header()}, nil
	}
	return &pdpb.GetRegionResponse{
		Header:       s.header(),
		Region:       region.GetMeta(),
		Leader:       region.GetLeader(),
		DownPeers:    region.GetDownPeers(),
		PendingPeers: region.GetPendingPeers(),
	}, nil
}

// GetRegionByID implements gRPC PDServer.
func (s *Server) GetRegionByID(ctx context.Context, request *pdpb.GetRegionByIDRequest) (*pdpb.GetRegionResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).GetRegionByID(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}
	region := rc.GetRegion(request.GetRegionId())
	if region == nil {
		return &pdpb.GetRegionResponse{Header: s.header()}, nil
	}
	return &pdpb.GetRegionResponse{
		Header:       s.header(),
		Region:       region.GetMeta(),
		Leader:       region.GetLeader(),
		DownPeers:    region.GetDownPeers(),
		PendingPeers: region.GetPendingPeers(),
	}, nil
}

// ScanRegions implements gRPC PDServer.
func (s *Server) ScanRegions(ctx context.Context, request *pdpb.ScanRegionsRequest) (*pdpb.ScanRegionsResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).ScanRegions(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.ScanRegionsResponse{Header: s.notBootstrappedHeader()}, nil
	}
	regions := rc.ScanRegions(request.GetStartKey(), request.GetEndKey(), int(request.GetLimit()))
	resp := &pdpb.ScanRegionsResponse{Header: s.header()}
	for _, r := range regions {
		leader := r.GetLeader()
		if leader == nil {
			leader = &metapb.Peer{}
		}
		// Set RegionMetas and Leaders to make it compatible with old client.
		resp.RegionMetas = append(resp.RegionMetas, r.GetMeta())
		resp.Leaders = append(resp.Leaders, leader)
		resp.Regions = append(resp.Regions, &pdpb.Region{
			Region:       r.GetMeta(),
			Leader:       leader,
			DownPeers:    r.GetDownPeers(),
			PendingPeers: r.GetPendingPeers(),
		})
	}
	return resp, nil
}

// AskSplit implements gRPC PDServer.
func (s *Server) AskSplit(ctx context.Context, request *pdpb.AskSplitRequest) (*pdpb.AskSplitResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).AskSplit(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.AskSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}
	if request.GetRegion() == nil {
		return nil, errors.New("missing region for split")
	}
	req := &pdpb.AskSplitRequest{
		Region: request.Region,
	}
	split, err := rc.HandleAskSplit(req)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb.AskSplitResponse{
		Header:      s.header(),
		NewRegionId: split.NewRegionId,
		NewPeerIds:  split.NewPeerIds,
	}, nil
}

// AskBatchSplit implements gRPC PDServer.
func (s *Server) AskBatchSplit(ctx context.Context, request *pdpb.AskBatchSplitRequest) (*pdpb.AskBatchSplitResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).AskBatchSplit(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.AskBatchSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}

	if !rc.IsFeatureSupported(versioninfo.BatchSplit) {
		return &pdpb.AskBatchSplitResponse{Header: s.incompatibleVersion("batch_split")}, nil
	}
	if request.GetRegion() == nil {
		return nil, errors.New("missing region for split")
	}
	req := &pdpb.AskBatchSplitRequest{
		Region:     request.Region,
		SplitCount: request.SplitCount,
	}
	split, err := rc.HandleAskBatchSplit(req)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb.AskBatchSplitResponse{
		Header: s.header(),
		Ids:    split.Ids,
	}, nil
}

// ReportSplit implements gRPC PDServer.
func (s *Server) ReportSplit(ctx context.Context, request *pdpb.ReportSplitRequest) (*pdpb.ReportSplitResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).ReportSplit(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.ReportSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}
	_, err := rc.HandleReportSplit(request)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb.ReportSplitResponse{
		Header: s.header(),
	}, nil
}

// ReportBatchSplit implements gRPC PDServer.
func (s *Server) ReportBatchSplit(ctx context.Context, request *pdpb.ReportBatchSplitRequest) (*pdpb.ReportBatchSplitResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).ReportBatchSplit(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.ReportBatchSplitResponse{Header: s.notBootstrappedHeader()}, nil
	}

	_, err := rc.HandleBatchReportSplit(request)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb.ReportBatchSplitResponse{
		Header: s.header(),
	}, nil
}

// GetClusterConfig implements gRPC PDServer.
func (s *Server) GetClusterConfig(ctx context.Context, request *pdpb.GetClusterConfigRequest) (*pdpb.GetClusterConfigResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).GetClusterConfig(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetClusterConfigResponse{Header: s.notBootstrappedHeader()}, nil
	}
	return &pdpb.GetClusterConfigResponse{
		Header:  s.header(),
		Cluster: rc.GetConfig(),
	}, nil
}

// PutClusterConfig implements gRPC PDServer.
func (s *Server) PutClusterConfig(ctx context.Context, request *pdpb.PutClusterConfigRequest) (*pdpb.PutClusterConfigResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).PutClusterConfig(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.PutClusterConfigResponse{Header: s.notBootstrappedHeader()}, nil
	}
	conf := request.GetCluster()
	if err := rc.PutConfig(conf); err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	log.Info("put cluster config ok", zap.Reflect("config", conf))

	return &pdpb.PutClusterConfigResponse{
		Header: s.header(),
	}, nil
}

// ScatterRegion implements gRPC PDServer.
func (s *Server) ScatterRegion(ctx context.Context, request *pdpb.ScatterRegionRequest) (*pdpb.ScatterRegionResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).ScatterRegion(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.ScatterRegionResponse{Header: s.notBootstrappedHeader()}, nil
	}

	if len(request.GetRegionsId()) > 0 {
		ops, failures, err := rc.GetRegionScatter().ScatterRegionsByID(request.GetRegionsId(), request.GetGroup(), int(request.GetRetryLimit()))
		if err != nil {
			return nil, err
		}
		for _, op := range ops {
			if ok := rc.GetOperatorController().AddOperator(op); !ok {
				failures[op.RegionID()] = fmt.Errorf("region %v failed to add operator", op.RegionID())
			}
		}
		percentage := 100
		if len(failures) > 0 {
			percentage = 100 - 100*len(failures)/(len(ops)+len(failures))
			log.Debug("scatter regions", zap.Errors("failures", func() []error {
				r := make([]error, 0, len(failures))
				for _, err := range failures {
					r = append(r, err)
				}
				return r
			}()))
		}
		return &pdpb.ScatterRegionResponse{
			Header:             s.header(),
			FinishedPercentage: uint64(percentage),
		}, nil
	}

	//nolint
	region := rc.GetRegion(request.GetRegionId())
	if region == nil {
		if request.GetRegion() == nil {
			//nolint
			return nil, errors.Errorf("region %d not found", request.GetRegionId())
		}
		region = core.NewRegionInfo(request.GetRegion(), request.GetLeader())
	}

	op, err := rc.GetRegionScatter().Scatter(region, request.GetGroup())
	if err != nil {
		return nil, err
	}
	if op != nil {
		rc.GetOperatorController().AddOperator(op)
	}

	return &pdpb.ScatterRegionResponse{
		Header:             s.header(),
		FinishedPercentage: 100,
	}, nil
}

// GetGCSafePoint implements gRPC PDServer.
func (s *Server) GetGCSafePoint(ctx context.Context, request *pdpb.GetGCSafePointRequest) (*pdpb.GetGCSafePointResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).GetGCSafePoint(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetGCSafePointResponse{Header: s.notBootstrappedHeader()}, nil
	}

	safePoint, err := s.storage.LoadGCSafePoint()
	if err != nil {
		return nil, err
	}

	return &pdpb.GetGCSafePointResponse{
		Header:    s.header(),
		SafePoint: safePoint,
	}, nil
}

// SyncRegions syncs the regions.
func (s *Server) SyncRegions(stream pdpb.PD_SyncRegionsServer) error {
	if s.cluster == nil {
		return ErrNotStarted
	}
	return s.cluster.GetRegionSyncer().Sync(stream)
}

// UpdateGCSafePoint implements gRPC PDServer.
func (s *Server) UpdateGCSafePoint(ctx context.Context, request *pdpb.UpdateGCSafePointRequest) (*pdpb.UpdateGCSafePointResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).UpdateGCSafePoint(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.UpdateGCSafePointResponse{Header: s.notBootstrappedHeader()}, nil
	}

	oldSafePoint, err := s.storage.LoadGCSafePoint()
	if err != nil {
		return nil, err
	}

	newSafePoint := request.SafePoint

	// Only save the safe point if it's greater than the previous one
	if newSafePoint > oldSafePoint {
		if err := s.storage.SaveGCSafePoint(newSafePoint); err != nil {
			return nil, err
		}
		log.Info("updated gc safe point",
			zap.Uint64("safe-point", newSafePoint))
	} else if newSafePoint < oldSafePoint {
		log.Warn("trying to update gc safe point",
			zap.Uint64("old-safe-point", oldSafePoint),
			zap.Uint64("new-safe-point", newSafePoint))
		newSafePoint = oldSafePoint
	}

	return &pdpb.UpdateGCSafePointResponse{
		Header:       s.header(),
		NewSafePoint: newSafePoint,
	}, nil
}

// UpdateServiceGCSafePoint update the safepoint for specific service
func (s *Server) UpdateServiceGCSafePoint(ctx context.Context, request *pdpb.UpdateServiceGCSafePointRequest) (*pdpb.UpdateServiceGCSafePointResponse, error) {
	s.serviceSafePointLock.Lock()
	defer s.serviceSafePointLock.Unlock()

	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).UpdateServiceGCSafePoint(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.UpdateServiceGCSafePointResponse{Header: s.notBootstrappedHeader()}, nil
	}
	if request.TTL <= 0 {
		if err := s.storage.RemoveServiceGCSafePoint(string(request.ServiceId)); err != nil {
			return nil, err
		}
	}

	nowTSO, err := s.tsoAllocatorManager.HandleTSORequest(tso.GlobalDCLocation, 1)
	if err != nil {
		return nil, err
	}
	now, _ := tsoutil.ParseTimestamp(nowTSO)
	min, err := s.storage.LoadMinServiceGCSafePoint(now)
	if err != nil {
		return nil, err
	}

	if request.TTL > 0 && request.SafePoint >= min.SafePoint {
		ssp := &core.ServiceSafePoint{
			ServiceID: string(request.ServiceId),
			ExpiredAt: now.Unix() + request.TTL,
			SafePoint: request.SafePoint,
		}
		if math.MaxInt64-now.Unix() <= request.TTL {
			ssp.ExpiredAt = math.MaxInt64
		}
		if err := s.storage.SaveServiceGCSafePoint(ssp); err != nil {
			return nil, err
		}
		log.Info("update service GC safe point",
			zap.String("service-id", ssp.ServiceID),
			zap.Int64("expire-at", ssp.ExpiredAt),
			zap.Uint64("safepoint", ssp.SafePoint))
		// If the min safepoint is updated, load the next one
		if string(request.ServiceId) == min.ServiceID {
			min, err = s.storage.LoadMinServiceGCSafePoint(now)
			if err != nil {
				return nil, err
			}
		}
	}

	return &pdpb.UpdateServiceGCSafePointResponse{
		Header:       s.header(),
		ServiceId:    []byte(min.ServiceID),
		TTL:          min.ExpiredAt - now.Unix(),
		MinSafePoint: min.SafePoint,
	}, nil
}

// GetOperator gets information about the operator belonging to the specify region.
func (s *Server) GetOperator(ctx context.Context, request *pdpb.GetOperatorRequest) (*pdpb.GetOperatorResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).GetOperator(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	rc := s.GetRaftCluster()
	if rc == nil {
		return &pdpb.GetOperatorResponse{Header: s.notBootstrappedHeader()}, nil
	}

	opController := rc.GetOperatorController()
	requestID := request.GetRegionId()
	r := opController.GetOperatorStatus(requestID)
	if r == nil {
		header := s.errorHeader(&pdpb.Error{
			Type:    pdpb.ErrorType_REGION_NOT_FOUND,
			Message: "Not Found",
		})
		return &pdpb.GetOperatorResponse{Header: header}, nil
	}

	return &pdpb.GetOperatorResponse{
		Header:   s.header(),
		RegionId: requestID,
		Desc:     []byte(r.Op.Desc()),
		Kind:     []byte(r.Op.Kind().String()),
		Status:   r.Status,
	}, nil
}

// validateRequest checks if Server is leader and clusterID is matched.
// TODO: Call it in gRPC interceptor.
func (s *Server) validateRequest(header *pdpb.RequestHeader) error {
	if s.IsClosed() || !s.member.IsLeader() {
		return ErrNotLeader
	}
	if header.GetClusterId() != s.clusterID {
		return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.clusterID, header.GetClusterId())
	}
	return nil
}

func (s *Server) header() *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{ClusterId: s.clusterID}
}

func (s *Server) errorHeader(err *pdpb.Error) *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{
		ClusterId: s.clusterID,
		Error:     err,
	}
}

func (s *Server) notBootstrappedHeader() *pdpb.ResponseHeader {
	return s.errorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_NOT_BOOTSTRAPPED,
		Message: "cluster is not bootstrapped",
	})
}

func (s *Server) incompatibleVersion(tag string) *pdpb.ResponseHeader {
	msg := fmt.Sprintf("%s incompatible with current cluster version %s", tag, s.persistOptions.GetClusterVersion())
	return s.errorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_INCOMPATIBLE_VERSION,
		Message: msg,
	})
}

// SyncMaxTS will check whether MaxTS is the biggest one among all Local TSOs this PD is holding when skipCheck is set,
// and write it into all Local TSO Allocators then if it's indeed the biggest one.
func (s *Server) SyncMaxTS(ctx context.Context, request *pdpb.SyncMaxTSRequest) (*pdpb.SyncMaxTSResponse, error) {
	if err := s.validateInternalRequest(request.GetHeader(), true); err != nil {
		return nil, err
	}
	tsoAllocatorManager := s.GetTSOAllocatorManager()
	// There is no dc-location found in this server, return err.
	if tsoAllocatorManager.GetClusterDCLocationsNumber() == 0 {
		return nil, status.Errorf(codes.Unknown, "empty cluster dc-location found, checker may not work properly")
	}
	// Get all Local TSO Allocator leaders
	allocatorLeaders, err := tsoAllocatorManager.GetHoldingLocalAllocatorLeaders()
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}
	if !request.GetSkipCheck() {
		var maxLocalTS *pdpb.Timestamp
		syncedDCs := make([]string, 0, len(allocatorLeaders))
		for _, allocator := range allocatorLeaders {
			// No longer leader, just skip here because
			// the global allocator will check if all DCs are handled.
			if !allocator.IsAllocatorLeader() {
				continue
			}
			currentLocalTSO, err := allocator.GetCurrentTSO()
			if err != nil {
				return nil, status.Errorf(codes.Unknown, err.Error())
			}
			if tsoutil.CompareTimestamp(currentLocalTSO, maxLocalTS) > 0 {
				maxLocalTS = currentLocalTSO
			}
			syncedDCs = append(syncedDCs, allocator.GetDCLocation())
		}
		// Found a bigger or equal maxLocalTS, return it directly.
		cmpResult := tsoutil.CompareTimestamp(maxLocalTS, request.GetMaxTs())
		if cmpResult >= 0 {
			// Found an equal maxLocalTS, plus 1 to logical part before returning it.
			// For example, we have a Global TSO t1 and a Local TSO t2, they have the
			// same physical and logical parts. After being differentiating with suffix,
			// there will be (t1.logical << suffixNum + 0) < (t2.logical << suffixNum + N),
			// where N is bigger than 0, which will cause a Global TSO fallback than the previous Local TSO.
			if cmpResult == 0 {
				maxLocalTS.Logical += 1
			}
			return &pdpb.SyncMaxTSResponse{
				Header:     s.header(),
				MaxLocalTs: maxLocalTS,
				SyncedDcs:  syncedDCs,
			}, nil
		}
	}
	syncedDCs := make([]string, 0, len(allocatorLeaders))
	for _, allocator := range allocatorLeaders {
		if !allocator.IsAllocatorLeader() {
			continue
		}
		if err := allocator.WriteTSO(request.GetMaxTs()); err != nil {
			return nil, status.Errorf(codes.Unknown, err.Error())
		}
		syncedDCs = append(syncedDCs, allocator.GetDCLocation())
	}
	return &pdpb.SyncMaxTSResponse{
		Header:    s.header(),
		SyncedDcs: syncedDCs,
	}, nil
}

// SplitRegions split regions by the given split keys
func (s *Server) SplitRegions(ctx context.Context, request *pdpb.SplitRegionsRequest) (*pdpb.SplitRegionsResponse, error) {
	forwardedHost := getForwardedHost(ctx)
	if !s.isLocalRequest(forwardedHost) {
		client, err := s.getDelegateClient(ctx, forwardedHost)
		if err != nil {
			return nil, err
		}
		ctx = grpcutil.ResetForwardContext(ctx)
		return pdpb.NewPDClient(client).SplitRegions(ctx, request)
	}

	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}
	finishedPercentage, newRegionIDs := s.cluster.GetRegionSplitter().SplitRegions(ctx, request.GetSplitKeys(), int(request.GetRetryLimit()))
	return &pdpb.SplitRegionsResponse{
		Header:             s.header(),
		RegionsId:          newRegionIDs,
		FinishedPercentage: uint64(finishedPercentage),
	}, nil
}

// GetDCLocationInfo gets the dc-location info of the given dc-location from PD leader's TSO allocator manager.
func (s *Server) GetDCLocationInfo(ctx context.Context, request *pdpb.GetDCLocationInfoRequest) (*pdpb.GetDCLocationInfoResponse, error) {
	var err error
	if err = s.validateInternalRequest(request.GetHeader(), false); err != nil {
		return nil, err
	}
	if !s.member.IsLeader() {
		return nil, ErrNotLeader
	}
	am := s.tsoAllocatorManager
	info, ok := am.GetDCLocationInfo(request.GetDcLocation())
	if !ok {
		am.ClusterDCLocationChecker()
		return nil, status.Errorf(codes.Unknown, "dc-location %s is not found", request.GetDcLocation())
	}
	resp := &pdpb.GetDCLocationInfoResponse{
		Header: s.header(),
		Suffix: info.Suffix,
	}
	// Because the number of suffix bits is changing dynamically according to the dc-location number,
	// there is a corner case may cause the Local TSO is not unique while member changing.
	// Example:
	//     t1: xxxxxxxxxxxxxxx1 | 11
	//     t2: xxxxxxxxxxxxxxx | 111
	// So we will force the newly added Local TSO Allocator to have a Global TSO synchronization
	// when it becomes the Local TSO Allocator leader.
	// Please take a look at https://github.com/tikv/pd/issues/3260 for more details.
	if resp.MaxTs, err = am.GetMaxLocalTSO(ctx); err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}
	return resp, nil
}

// validateInternalRequest checks if server is closed, which is used to validate
// the gRPC communication between PD servers internally.
func (s *Server) validateInternalRequest(header *pdpb.RequestHeader, onlyAllowLeader bool) error {
	if s.IsClosed() {
		return ErrNotStarted
	}
	// If onlyAllowLeader is true, check whether the sender is PD leader.
	if onlyAllowLeader {
		leaderID := s.GetLeader().GetMemberId()
		if leaderID != header.GetSenderId() {
			return status.Errorf(codes.FailedPrecondition, "%s, need %d but got %d", errs.MismatchLeaderErr, leaderID, header.GetSenderId())
		}
	}
	return nil
}

func (s *Server) getDelegateClient(ctx context.Context, forwardedHost string) (*grpc.ClientConn, error) {
	client, ok := s.clientConns.Load(forwardedHost)
	if !ok {
		tlsConfig, err := s.GetTLSConfig().ToTLSConfig()
		if err != nil {
			return nil, err
		}
		cc, err := grpcutil.GetClientConn(ctx, forwardedHost, tlsConfig)
		if err != nil {
			return nil, err
		}
		client = cc
		s.clientConns.Store(forwardedHost, cc)
	}
	return client.(*grpc.ClientConn), nil
}

func getForwardedHost(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		log.Error("failed to get forwarding metadata")
	}
	if t, ok := md[grpcutil.ForwardMetadataKey]; ok {
		return t[0]
	}
	return ""
}

func (s *Server) isLocalRequest(forwardedHost string) bool {
	if forwardedHost == "" {
		return true
	}
	memberAddrs := s.GetMember().Member().GetClientUrls()
	for _, addr := range memberAddrs {
		if addr == forwardedHost {
			return true
		}
	}
	return false
}

func (s *Server) createTsoForwardStream(client *grpc.ClientConn) (pdpb.PD_TsoClient, context.CancelFunc, error) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(s.ctx)
	go checkStream(ctx, cancel, done)
	forwardStream, err := pdpb.NewPDClient(client).Tso(ctx)
	done <- struct{}{}
	return forwardStream, cancel, err
}

func (s *Server) createHeartbeatForwardStream(client *grpc.ClientConn) (pdpb.PD_RegionHeartbeatClient, context.CancelFunc, error) {
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(s.ctx)
	go checkStream(ctx, cancel, done)
	forwardStream, err := pdpb.NewPDClient(client).RegionHeartbeat(ctx)
	done <- struct{}{}
	return forwardStream, cancel, err
}

func forwardRegionHeartbeatClientToServer(forwardStream pdpb.PD_RegionHeartbeatClient, server *heartbeatServer, errCh chan error) {
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

// TODO: If goroutine here timeout when tso stream created successfully, we need to handle it correctly.
func checkStream(streamCtx context.Context, cancel context.CancelFunc, done chan struct{}) {
	select {
	case <-done:
		return
	case <-time.After(3 * time.Second):
		cancel()
	case <-streamCtx.Done():
	}
	<-done
}
