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

package simulator

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tools/pd-simulator/simulator/simutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Client is a PD (Placement Driver) client.
// It should not be used after calling Close().
type Client interface {
	GetClusterID(ctx context.Context) uint64
	AllocID(ctx context.Context) (uint64, error)
	Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) error
	PutStore(ctx context.Context, store *metapb.Store) error
	StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error
	RegionHeartbeat(ctx context.Context, region *core.RegionInfo) error
	Close()
}

const (
	pdTimeout             = time.Second
	maxInitClusterRetries = 100
)

var (
	// errFailInitClusterID is returned when failed to load clusterID from all supplied PD addresses.
	errFailInitClusterID = errors.New("[pd] failed to get cluster id")
)

type client struct {
	url        string
	tag        string
	clusterID  uint64
	clientConn *grpc.ClientConn

	reportRegionHeartbeatCh  chan *core.RegionInfo
	receiveRegionHeartbeatCh chan *pdpb.RegionHeartbeatResponse

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewClient creates a PD client.
func NewClient(pdAddr string, tag string) (Client, <-chan *pdpb.RegionHeartbeatResponse, error) {
	simutil.Logger.Info("create pd client with endpoints", zap.String("tag", tag), zap.String("pd-address", pdAddr))
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		url:                      pdAddr,
		reportRegionHeartbeatCh:  make(chan *core.RegionInfo, 1),
		receiveRegionHeartbeatCh: make(chan *pdpb.RegionHeartbeatResponse, 1),
		ctx:                      ctx,
		cancel:                   cancel,
		tag:                      tag,
	}
	cc, err := c.createConn()
	if err != nil {
		return nil, nil, err
	}
	c.clientConn = cc
	if err := c.initClusterID(); err != nil {
		return nil, nil, err
	}
	simutil.Logger.Info("init cluster id", zap.String("tag", c.tag), zap.Uint64("cluster-id", c.clusterID))
	c.wg.Add(1)
	go c.heartbeatStreamLoop()

	return c, c.receiveRegionHeartbeatCh, nil
}

func (c *client) pdClient() pdpb.PDClient {
	return pdpb.NewPDClient(c.clientConn)
}

func (c *client) initClusterID() error {
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	for i := 0; i < maxInitClusterRetries; i++ {
		members, err := c.getMembers(ctx)
		if err != nil || members.GetHeader() == nil {
			simutil.Logger.Error("failed to get cluster id", zap.String("tag", c.tag), zap.Error(err))
			continue
		}
		c.clusterID = members.GetHeader().GetClusterId()
		return nil
	}

	return errors.WithStack(errFailInitClusterID)
}

func (c *client) getMembers(ctx context.Context) (*pdpb.GetMembersResponse, error) {
	members, err := c.pdClient().GetMembers(ctx, &pdpb.GetMembersRequest{})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return members, nil
}

func (c *client) createConn() (*grpc.ClientConn, error) {
	cc, err := grpc.Dial(strings.TrimPrefix(c.url, "http://"), grpc.WithInsecure())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return cc, nil
}

func (c *client) createHeartbeatStream() (pdpb.PD_RegionHeartbeatClient, context.Context, context.CancelFunc) {
	var (
		stream pdpb.PD_RegionHeartbeatClient
		err    error
		cancel context.CancelFunc
		ctx    context.Context
	)
	for {
		ctx, cancel = context.WithCancel(c.ctx)
		stream, err = c.pdClient().RegionHeartbeat(ctx)
		if err != nil {
			simutil.Logger.Error("create region heartbeat stream error", zap.String("tag", c.tag), zap.Error(err))
			cancel()
			select {
			case <-time.After(time.Second):
				continue
			case <-c.ctx.Done():
				simutil.Logger.Info("cancel create stream loop")
				return nil, ctx, cancel
			}
		}
		break
	}
	return stream, ctx, cancel
}

func (c *client) heartbeatStreamLoop() {
	defer c.wg.Done()
	for {
		stream, ctx, cancel := c.createHeartbeatStream()
		if stream == nil {
			return
		}
		errCh := make(chan error, 1)
		wg := &sync.WaitGroup{}
		wg.Add(2)
		go c.reportRegionHeartbeat(ctx, stream, errCh, wg)
		go c.receiveRegionHeartbeat(ctx, stream, errCh, wg)
		select {
		case err := <-errCh:
			simutil.Logger.Error("heartbeat stream get error", zap.String("tag", c.tag), zap.Error(err))
			cancel()
		case <-c.ctx.Done():
			simutil.Logger.Info("cancel heartbeat stream loop")
			return
		}
		wg.Wait()
	}
}

func (c *client) receiveRegionHeartbeat(ctx context.Context, stream pdpb.PD_RegionHeartbeatClient, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		resp, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		select {
		case c.receiveRegionHeartbeatCh <- resp:
		case <-ctx.Done():
			return
		}
	}
}

func (c *client) reportRegionHeartbeat(ctx context.Context, stream pdpb.PD_RegionHeartbeatClient, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case region := <-c.reportRegionHeartbeatCh:
			request := &pdpb.RegionHeartbeatRequest{
				Header:          c.requestHeader(),
				Region:          region.GetMeta(),
				Leader:          region.GetLeader(),
				DownPeers:       region.GetDownPeers(),
				PendingPeers:    region.GetPendingPeers(),
				BytesWritten:    region.GetBytesWritten(),
				BytesRead:       region.GetBytesRead(),
				ApproximateSize: uint64(region.GetApproximateSize()),
				ApproximateKeys: uint64(region.GetApproximateKeys()),
			}
			err := stream.Send(request)
			if err != nil {
				errCh <- err
				simutil.Logger.Error("report regionHeartbeat error", zap.String("tag", c.tag), zap.Error(err))
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *client) Close() {
	c.cancel()
	c.wg.Wait()

	if err := c.clientConn.Close(); err != nil {
		simutil.Logger.Error("failed to close grpc client connection", zap.String("tag", c.tag), zap.Error(err))
	}
}

func (c *client) GetClusterID(context.Context) uint64 {
	return c.clusterID
}

func (c *client) AllocID(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().AllocID(ctx, &pdpb.AllocIDRequest{
		Header: c.requestHeader(),
	})
	cancel()
	if err != nil {
		return 0, err
	}
	return resp.GetId(), nil
}

func (c *client) Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) error {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	defer cancel()
	req := &pdpb.IsBootstrappedRequest{
		Header: &pdpb.RequestHeader{
			ClusterId: c.clusterID,
		},
	}
	resp, err := c.pdClient().IsBootstrapped(ctx, req)
	if resp.GetBootstrapped() {
		simutil.Logger.Fatal("failed to bootstrap, server is not clean")
	}
	if err != nil {
		return err
	}
	_, err = c.pdClient().Bootstrap(ctx, &pdpb.BootstrapRequest{
		Header: c.requestHeader(),
		Store:  store,
		Region: region,
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *client) PutStore(ctx context.Context, store *metapb.Store) error {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().PutStore(ctx, &pdpb.PutStoreRequest{
		Header: c.requestHeader(),
		Store:  store,
	})
	cancel()
	if err != nil {
		return err
	}
	if resp.Header.GetError() != nil {
		simutil.Logger.Error("put store error", zap.Reflect("error", resp.Header.GetError()))
		return nil
	}
	return nil
}

func (c *client) StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().StoreHeartbeat(ctx, &pdpb.StoreHeartbeatRequest{
		Header: c.requestHeader(),
		Stats:  stats,
	})
	cancel()
	if err != nil {
		return err
	}
	if resp.Header.GetError() != nil {
		simutil.Logger.Error("store heartbeat error", zap.Reflect("error", resp.Header.GetError()))
		return nil
	}
	return nil
}

func (c *client) RegionHeartbeat(ctx context.Context, region *core.RegionInfo) error {
	c.reportRegionHeartbeatCh <- region
	return nil
}

func (c *client) requestHeader() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: c.clusterID,
	}
}
