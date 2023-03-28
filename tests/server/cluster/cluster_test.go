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

package cluster_test

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/replication_modepb"
	"github.com/tikv/pd/pkg/dashboard"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/core/storelimit"
	"github.com/tikv/pd/server/id"
	syncer "github.com/tikv/pd/server/region_syncer"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/storage"
	"github.com/tikv/pd/tests"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func Test(t *testing.T) {
	TestingT(t)
}

const (
	initEpochVersion uint64 = 1
	initEpochConfVer uint64 = 1

	testMetaStoreAddr = "127.0.0.1:12345"
	testStoreAddr     = "127.0.0.1:0"
)

var _ = Suite(&clusterTestSuite{})

type clusterTestSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *clusterTestSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
	// to prevent GetStorage
	dashboard.SetCheckInterval(30 * time.Minute)
}

func (s *clusterTestSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *clusterTestSuite) TestBootstrap(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()

	// IsBootstrapped returns false.
	req := newIsBootstrapRequest(clusterID)
	resp, err := grpcPDClient.IsBootstrapped(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	c.Assert(resp.GetBootstrapped(), IsFalse)

	// Bootstrap the cluster.
	bootstrapCluster(c, clusterID, grpcPDClient)

	// IsBootstrapped returns true.
	req = newIsBootstrapRequest(clusterID)
	resp, err = grpcPDClient.IsBootstrapped(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetBootstrapped(), IsTrue)

	// check bootstrapped error.
	reqBoot := newBootstrapRequest(clusterID)
	respBoot, err := grpcPDClient.Bootstrap(context.Background(), reqBoot)
	c.Assert(err, IsNil)
	c.Assert(respBoot.GetHeader().GetError(), NotNil)
	c.Assert(respBoot.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_ALREADY_BOOTSTRAPPED)
}

func (s *clusterTestSuite) TestDamagedRegion(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient)
	rc := leaderServer.GetRaftCluster()

	region := &metapb.Region{
		Id:       10,
		StartKey: []byte("abc"),
		EndKey:   []byte("xyz"),
		Peers: []*metapb.Peer{
			{Id: 101, StoreId: 1},
			{Id: 102, StoreId: 2},
			{Id: 103, StoreId: 3},
		},
	}

	// To put region.
	regionInfo := core.NewRegionInfo(region, region.Peers[0], core.SetApproximateSize(30))
	err = tc.HandleRegionHeartbeat(regionInfo)
	c.Assert(err, IsNil)

	stores := []*pdpb.PutStoreRequest{
		{
			Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			Store: &metapb.Store{
				Id:      1,
				Address: "mock-1",
				Version: "2.0.1",
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			Store: &metapb.Store{
				Id:      2,
				Address: "mock-4",
				Version: "2.0.1",
			},
		},
		{
			Header: &pdpb.RequestHeader{ClusterId: leaderServer.GetClusterID()},
			Store: &metapb.Store{
				Id:      3,
				Address: "mock-6",
				Version: "2.0.1",
			},
		},
	}

	// To put stores.
	svr := &server.GrpcServer{Server: leaderServer.GetServer()}
	for _, store := range stores {
		_, err = svr.PutStore(context.Background(), store)
		c.Assert(err, IsNil)
	}

	// To validate remove peer op be added.
	req1 := &pdpb.StoreHeartbeatRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Stats:  &pdpb.StoreStats{StoreId: 2, DamagedRegionsId: []uint64{10}},
	}
	c.Assert(rc.GetOperatorController().OperatorCount(operator.OpAdmin), Equals, uint64(0))
	_, err1 := grpcPDClient.StoreHeartbeat(context.Background(), req1)
	c.Assert(err1, IsNil)
	c.Assert(rc.GetOperatorController().OperatorCount(operator.OpAdmin), Equals, uint64(1))
}

func (s *clusterTestSuite) TestGetPutConfig(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient)
	rc := leaderServer.GetRaftCluster()
	c.Assert(rc, NotNil)
	// Get region.
	region := getRegion(c, clusterID, grpcPDClient, []byte("abc"))
	c.Assert(region.GetPeers(), HasLen, 1)
	peer := region.GetPeers()[0]

	// Get region by id.
	regionByID := getRegionByID(c, clusterID, grpcPDClient, region.GetId())
	c.Assert(region, DeepEquals, regionByID)

	r := core.NewRegionInfo(region, region.Peers[0], core.SetApproximateSize(30))
	err = tc.HandleRegionHeartbeat(r)
	c.Assert(err, IsNil)

	// Get store.
	storeID := peer.GetStoreId()
	store := getStore(c, clusterID, grpcPDClient, storeID)

	// Update store.
	store.Address = "127.0.0.1:1"
	testPutStore(c, clusterID, rc, grpcPDClient, store)

	// Remove store.
	testRemoveStore(c, clusterID, rc, grpcPDClient, store)

	// Update cluster config.
	req := &pdpb.PutClusterConfigRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Cluster: &metapb.Cluster{
			Id:           clusterID,
			MaxPeerCount: 5,
		},
	}
	resp, err := grpcPDClient.PutClusterConfig(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	meta := getClusterConfig(c, clusterID, grpcPDClient)
	c.Assert(meta.GetMaxPeerCount(), Equals, uint32(5))
}

func testPutStore(c *C, clusterID uint64, rc *cluster.RaftCluster, grpcPDClient pdpb.PDClient, store *metapb.Store) {
	// Update store.
	_, err := putStore(grpcPDClient, clusterID, store)
	c.Assert(err, IsNil)
	updatedStore := getStore(c, clusterID, grpcPDClient, store.GetId())
	c.Assert(updatedStore, DeepEquals, store)

	// Update store again.
	_, err = putStore(grpcPDClient, clusterID, store)
	c.Assert(err, IsNil)

	rc.GetAllocator().Alloc()
	id, err := rc.GetAllocator().Alloc()
	c.Assert(err, IsNil)
	// Put new store with a duplicated address when old store is up will fail.
	_, err = putStore(grpcPDClient, clusterID, newMetaStore(id, store.GetAddress(), "2.1.0", metapb.StoreState_Up, getTestDeployPath(id)))
	c.Assert(err, NotNil)

	id, err = rc.GetAllocator().Alloc()
	c.Assert(err, IsNil)
	// Put new store with a duplicated address when old store is offline will fail.
	resetStoreState(c, rc, store.GetId(), metapb.StoreState_Offline)
	_, err = putStore(grpcPDClient, clusterID, newMetaStore(id, store.GetAddress(), "2.1.0", metapb.StoreState_Up, getTestDeployPath(id)))
	c.Assert(err, NotNil)

	id, err = rc.GetAllocator().Alloc()
	c.Assert(err, IsNil)
	// Put new store with a duplicated address when old store is tombstone is OK.
	resetStoreState(c, rc, store.GetId(), metapb.StoreState_Tombstone)
	rc.GetStore(store.GetId())
	_, err = putStore(grpcPDClient, clusterID, newMetaStore(id, store.GetAddress(), "2.1.0", metapb.StoreState_Up, getTestDeployPath(id)))
	c.Assert(err, IsNil)

	id, err = rc.GetAllocator().Alloc()
	c.Assert(err, IsNil)
	deployPath := getTestDeployPath(id)
	// Put a new store.
	_, err = putStore(grpcPDClient, clusterID, newMetaStore(id, testMetaStoreAddr, "2.1.0", metapb.StoreState_Up, deployPath))
	c.Assert(err, IsNil)
	s := rc.GetStore(id).GetMeta()
	c.Assert(s.DeployPath, Equals, deployPath)

	deployPath = fmt.Sprintf("move/test/store%d", id)
	_, err = putStore(grpcPDClient, clusterID, newMetaStore(id, testMetaStoreAddr, "2.1.0", metapb.StoreState_Up, deployPath))
	c.Assert(err, IsNil)
	s = rc.GetStore(id).GetMeta()
	c.Assert(s.DeployPath, Equals, deployPath)

	// Put an existed store with duplicated address with other old stores.
	resetStoreState(c, rc, store.GetId(), metapb.StoreState_Up)
	_, err = putStore(grpcPDClient, clusterID, newMetaStore(store.GetId(), testMetaStoreAddr, "2.1.0", metapb.StoreState_Up, getTestDeployPath(store.GetId())))
	c.Assert(err, NotNil)
}

func getTestDeployPath(storeID uint64) string {
	return fmt.Sprintf("test/store%d", storeID)
}

func resetStoreState(c *C, rc *cluster.RaftCluster, storeID uint64, state metapb.StoreState) {
	store := rc.GetStore(storeID)
	c.Assert(store, NotNil)
	newStore := store.Clone(core.OfflineStore(false))
	if state == metapb.StoreState_Up {
		newStore = newStore.Clone(core.UpStore())
	} else if state == metapb.StoreState_Tombstone {
		newStore = newStore.Clone(core.TombstoneStore())
	}

	rc.GetBasicCluster().PutStore(newStore)
	if state == metapb.StoreState_Offline {
		rc.SetStoreLimit(storeID, storelimit.RemovePeer, storelimit.Unlimited)
	} else if state == metapb.StoreState_Tombstone {
		rc.RemoveStoreLimit(storeID)
	}
}

func testStateAndLimit(c *C, clusterID uint64, rc *cluster.RaftCluster, grpcPDClient pdpb.PDClient, store *metapb.Store, beforeState metapb.StoreState, run func(*cluster.RaftCluster) error, expectStates ...metapb.StoreState) {
	// prepare
	storeID := store.GetId()
	oc := rc.GetOperatorController()
	rc.SetStoreLimit(storeID, storelimit.AddPeer, 60)
	rc.SetStoreLimit(storeID, storelimit.RemovePeer, 60)
	op := operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.AddPeer{ToStore: storeID, PeerID: 3})
	oc.AddOperator(op)
	op = operator.NewTestOperator(2, &metapb.RegionEpoch{}, operator.OpRegion, operator.RemovePeer{FromStore: storeID})
	oc.AddOperator(op)

	resetStoreState(c, rc, store.GetId(), beforeState)
	_, isOKBefore := rc.GetAllStoresLimit()[storeID]
	// run
	err := run(rc)
	// judge
	_, isOKAfter := rc.GetAllStoresLimit()[storeID]
	if len(expectStates) != 0 {
		c.Assert(err, IsNil)
		expectState := expectStates[0]
		c.Assert(getStore(c, clusterID, grpcPDClient, storeID).GetState(), Equals, expectState)
		if expectState == metapb.StoreState_Offline {
			c.Assert(isOKAfter, IsTrue)
		} else if expectState == metapb.StoreState_Tombstone {
			c.Assert(isOKAfter, IsFalse)
		}
	} else {
		c.Assert(err, NotNil)
		c.Assert(isOKBefore, Equals, isOKAfter)
	}
}

func testRemoveStore(c *C, clusterID uint64, rc *cluster.RaftCluster, grpcPDClient pdpb.PDClient, store *metapb.Store) {
	rc.GetOpts().SetMaxReplicas(2)
	defer rc.GetOpts().SetMaxReplicas(3)
	{
		beforeState := metapb.StoreState_Up // When store is up
		// Case 1: RemoveStore should be OK;
		testStateAndLimit(c, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.RemoveStore(store.GetId(), false)
		}, metapb.StoreState_Offline)
		// Case 2: RemoveStore with physically destroyed should be OK;
		testStateAndLimit(c, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.RemoveStore(store.GetId(), true)
		}, metapb.StoreState_Offline)
	}
	{
		beforeState := metapb.StoreState_Offline // When store is offline
		// Case 1: RemoveStore should be OK;
		testStateAndLimit(c, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.RemoveStore(store.GetId(), false)
		}, metapb.StoreState_Offline)
		// Case 2: remove store with physically destroyed should be success
		testStateAndLimit(c, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.RemoveStore(store.GetId(), true)
		}, metapb.StoreState_Offline)
	}
	{
		beforeState := metapb.StoreState_Tombstone // When store is tombstone
		// Case 1: RemoveStore should should fail;
		testStateAndLimit(c, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.RemoveStore(store.GetId(), false)
		})
		// Case 2: RemoveStore with physically destroyed should fail;
		testStateAndLimit(c, clusterID, rc, grpcPDClient, store, beforeState, func(cluster *cluster.RaftCluster) error {
			return cluster.RemoveStore(store.GetId(), true)
		})
	}
	{
		// Put after removed should return tombstone error.
		resp, err := putStore(grpcPDClient, clusterID, store)
		c.Assert(err, IsNil)
		c.Assert(resp.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_STORE_TOMBSTONE)
	}
	{
		// Update after removed should return tombstone error.
		req := &pdpb.StoreHeartbeatRequest{
			Header: testutil.NewRequestHeader(clusterID),
			Stats:  &pdpb.StoreStats{StoreId: store.GetId()},
		}
		resp, err := grpcPDClient.StoreHeartbeat(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(resp.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_STORE_TOMBSTONE)
	}
}

// Make sure PD will not panic if it start and stop again and again.
func (s *clusterTestSuite) TestRaftClusterRestart(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient)

	rc := leaderServer.GetRaftCluster()
	c.Assert(rc, NotNil)
	rc.Stop()

	err = rc.Start(leaderServer.GetServer())
	c.Assert(err, IsNil)

	rc = leaderServer.GetRaftCluster()
	c.Assert(rc, NotNil)
	rc.Stop()
}

// Make sure PD will not deadlock if it start and stop again and again.
func (s *clusterTestSuite) TestRaftClusterMultipleRestart(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient)
	// add an offline store
	storeID, err := leaderServer.GetAllocator().Alloc()
	c.Assert(err, IsNil)
	store := newMetaStore(storeID, "127.0.0.1:4", "2.1.0", metapb.StoreState_Offline, getTestDeployPath(storeID))
	rc := leaderServer.GetRaftCluster()
	c.Assert(rc, NotNil)
	err = rc.PutStore(store)
	c.Assert(err, IsNil)
	c.Assert(tc, NotNil)

	// let the job run at small interval
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs", `return(true)`), IsNil)
	for i := 0; i < 100; i++ {
		err = rc.Start(leaderServer.GetServer())
		c.Assert(err, IsNil)
		time.Sleep(time.Millisecond)
		rc = leaderServer.GetRaftCluster()
		c.Assert(rc, NotNil)
		rc.Stop()
	}
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/cluster/highFrequencyClusterJobs"), IsNil)
}

func newMetaStore(storeID uint64, addr, version string, state metapb.StoreState, deployPath string) *metapb.Store {
	return &metapb.Store{Id: storeID, Address: addr, Version: version, State: state, DeployPath: deployPath}
}

func (s *clusterTestSuite) TestGetPDMembers(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	req := &pdpb.GetMembersRequest{Header: testutil.NewRequestHeader(clusterID)}
	resp, err := grpcPDClient.GetMembers(context.Background(), req)
	c.Assert(err, IsNil)
	// A more strict test can be found at api/member_test.go
	c.Assert(resp.GetMembers(), Not(HasLen), 0)
}

func (s *clusterTestSuite) TestNotLeader(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 2)
	defer tc.Destroy()
	c.Assert(err, IsNil)
	c.Assert(tc.RunInitialServers(), IsNil)

	tc.WaitLeader()
	followerServer := tc.GetServer(tc.GetFollower())
	grpcPDClient := testutil.MustNewGrpcClient(c, followerServer.GetAddr())
	clusterID := followerServer.GetClusterID()
	req := &pdpb.AllocIDRequest{Header: testutil.NewRequestHeader(clusterID)}
	resp, err := grpcPDClient.AllocID(context.Background(), req)
	c.Assert(resp, IsNil)
	grpcStatus, ok := status.FromError(err)
	c.Assert(ok, IsTrue)
	c.Assert(grpcStatus.Code(), Equals, codes.Unavailable)
	c.Assert(grpcStatus.Message(), Equals, "not leader")
}

func (s *clusterTestSuite) TestStoreVersionChange(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient)
	svr := leaderServer.GetServer()
	svr.SetClusterVersion("2.0.0")
	storeID, err := leaderServer.GetAllocator().Alloc()
	c.Assert(err, IsNil)
	store := newMetaStore(storeID, "127.0.0.1:4", "2.1.0", metapb.StoreState_Up, getTestDeployPath(storeID))
	var wg sync.WaitGroup
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/versionChangeConcurrency", `return(true)`), IsNil)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err = putStore(grpcPDClient, clusterID, store)
		c.Assert(err, IsNil)
	}()
	time.Sleep(100 * time.Millisecond)
	svr.SetClusterVersion("1.0.0")
	wg.Wait()
	v, err := semver.NewVersion("1.0.0")
	c.Assert(err, IsNil)
	c.Assert(svr.GetClusterVersion(), Equals, *v)
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/versionChangeConcurrency"), IsNil)
}

func (s *clusterTestSuite) TestConcurrentHandleRegion(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient)
	storeAddrs := []string{"127.0.1.1:0", "127.0.1.1:1", "127.0.1.1:2"}
	rc := leaderServer.GetRaftCluster()
	c.Assert(rc, NotNil)
	rc.SetStorage(storage.NewStorageWithMemoryBackend())
	stores := make([]*metapb.Store, 0, len(storeAddrs))
	id := leaderServer.GetAllocator()
	for _, addr := range storeAddrs {
		storeID, err := id.Alloc()
		c.Assert(err, IsNil)
		store := newMetaStore(storeID, addr, "2.1.0", metapb.StoreState_Up, getTestDeployPath(storeID))
		stores = append(stores, store)
		_, err = putStore(grpcPDClient, clusterID, store)
		c.Assert(err, IsNil)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	// register store and bind stream
	for i, store := range stores {
		req := &pdpb.StoreHeartbeatRequest{
			Header: testutil.NewRequestHeader(clusterID),
			Stats: &pdpb.StoreStats{
				StoreId:   store.GetId(),
				Capacity:  1000 * (1 << 20),
				Available: 1000 * (1 << 20),
			},
		}
		grpcServer := &server.GrpcServer{Server: leaderServer.GetServer()}
		_, err := grpcServer.StoreHeartbeat(context.TODO(), req)
		c.Assert(err, IsNil)
		stream, err := grpcPDClient.RegionHeartbeat(ctx)
		c.Assert(err, IsNil)
		peerID, err := id.Alloc()
		c.Assert(err, IsNil)
		regionID, err := id.Alloc()
		c.Assert(err, IsNil)
		peer := &metapb.Peer{Id: peerID, StoreId: store.GetId()}
		regionReq := &pdpb.RegionHeartbeatRequest{
			Header: testutil.NewRequestHeader(clusterID),
			Region: &metapb.Region{
				Id:    regionID,
				Peers: []*metapb.Peer{peer},
			},
			Leader: peer,
		}
		err = stream.Send(regionReq)
		c.Assert(err, IsNil)
		// make sure the first store can receive one response
		if i == 0 {
			wg.Add(1)
		}
		go func(isReceiver bool) {
			if isReceiver {
				_, err := stream.Recv()
				c.Assert(err, IsNil)
				wg.Done()
			}
			for {
				select {
				case <-ctx.Done():
					return
				default:
					stream.Recv()
				}
			}
		}(i == 0)
	}

	concurrent := 1000
	for i := 0; i < concurrent; i++ {
		peerID, err := id.Alloc()
		c.Assert(err, IsNil)
		regionID, err := id.Alloc()
		c.Assert(err, IsNil)
		region := &metapb.Region{
			Id:       regionID,
			StartKey: []byte(fmt.Sprintf("%5d", i)),
			EndKey:   []byte(fmt.Sprintf("%5d", i+1)),
			Peers:    []*metapb.Peer{{Id: peerID, StoreId: stores[0].GetId()}},
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: initEpochConfVer,
				Version: initEpochVersion,
			},
		}
		if i == 0 {
			region.StartKey = []byte("")
		} else if i == concurrent-1 {
			region.EndKey = []byte("")
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := rc.HandleRegionHeartbeat(core.NewRegionInfo(region, region.Peers[0]))
			c.Assert(err, IsNil)
		}()
	}
	wg.Wait()
}

func (s *clusterTestSuite) TestSetScheduleOpt(c *C) {
	// TODO: enable placementrules
	tc, err := tests.NewTestCluster(s.ctx, 1, func(cfg *config.Config, svr string) { cfg.Replication.EnablePlacementRules = false })
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient)

	cfg := config.NewConfig()
	cfg.Schedule.TolerantSizeRatio = 5
	err = cfg.Adjust(nil, false)
	c.Assert(err, IsNil)
	opt := config.NewPersistOptions(cfg)
	c.Assert(err, IsNil)

	svr := leaderServer.GetServer()
	scheduleCfg := opt.GetScheduleConfig()
	replicationCfg := svr.GetReplicationConfig()
	persistOptions := svr.GetPersistOptions()
	pdServerCfg := persistOptions.GetPDServerConfig()

	// PUT GET DELETE succeed
	replicationCfg.MaxReplicas = 5
	scheduleCfg.MaxSnapshotCount = 10
	pdServerCfg.UseRegionStorage = true
	typ, labelKey, labelValue := "testTyp", "testKey", "testValue"

	c.Assert(svr.SetScheduleConfig(*scheduleCfg), IsNil)
	c.Assert(svr.SetPDServerConfig(*pdServerCfg), IsNil)
	c.Assert(svr.SetLabelProperty(typ, labelKey, labelValue), IsNil)
	c.Assert(svr.SetReplicationConfig(*replicationCfg), IsNil)

	c.Assert(persistOptions.GetMaxReplicas(), Equals, 5)
	c.Assert(persistOptions.GetMaxSnapshotCount(), Equals, uint64(10))
	c.Assert(persistOptions.IsUseRegionStorage(), IsTrue)
	c.Assert(persistOptions.GetLabelPropertyConfig()[typ][0].Key, Equals, "testKey")
	c.Assert(persistOptions.GetLabelPropertyConfig()[typ][0].Value, Equals, "testValue")

	c.Assert(svr.DeleteLabelProperty(typ, labelKey, labelValue), IsNil)

	c.Assert(persistOptions.GetLabelPropertyConfig()[typ], HasLen, 0)

	// PUT GET failed
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/storage/kv/etcdSaveFailed", `return(true)`), IsNil)
	replicationCfg.MaxReplicas = 7
	scheduleCfg.MaxSnapshotCount = 20
	pdServerCfg.UseRegionStorage = false

	c.Assert(svr.SetScheduleConfig(*scheduleCfg), NotNil)
	c.Assert(svr.SetReplicationConfig(*replicationCfg), NotNil)
	c.Assert(svr.SetPDServerConfig(*pdServerCfg), NotNil)
	c.Assert(svr.SetLabelProperty(typ, labelKey, labelValue), NotNil)

	c.Assert(persistOptions.GetMaxReplicas(), Equals, 5)
	c.Assert(persistOptions.GetMaxSnapshotCount(), Equals, uint64(10))
	c.Assert(persistOptions.GetPDServerConfig().UseRegionStorage, IsTrue)
	c.Assert(persistOptions.GetLabelPropertyConfig()[typ], HasLen, 0)

	// DELETE failed
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/storage/kv/etcdSaveFailed"), IsNil)
	c.Assert(svr.SetReplicationConfig(*replicationCfg), IsNil)

	c.Assert(failpoint.Enable("github.com/tikv/pd/server/storage/kv/etcdSaveFailed", `return(true)`), IsNil)
	c.Assert(svr.DeleteLabelProperty(typ, labelKey, labelValue), NotNil)

	c.Assert(persistOptions.GetLabelPropertyConfig()[typ][0].Key, Equals, "testKey")
	c.Assert(persistOptions.GetLabelPropertyConfig()[typ][0].Value, Equals, "testValue")
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/storage/kv/etcdSaveFailed"), IsNil)
}

func (s *clusterTestSuite) TestLoadClusterInfo(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	svr := leaderServer.GetServer()
	rc := cluster.NewRaftCluster(s.ctx, svr.ClusterID(), syncer.NewRegionSyncer(svr), svr.GetClient(), svr.GetHTTPClient())

	// Cluster is not bootstrapped.
	rc.InitCluster(svr.GetAllocator(), svr.GetPersistOptions(), svr.GetStorage(), svr.GetBasicCluster())
	raftCluster, err := rc.LoadClusterInfo()
	c.Assert(err, IsNil)
	c.Assert(raftCluster, IsNil)

	storage := rc.GetStorage()
	basicCluster := rc.GetBasicCluster()
	opt := rc.GetOpts()
	// Save meta, stores and regions.
	n := 10
	meta := &metapb.Cluster{Id: 123}
	c.Assert(storage.SaveMeta(meta), IsNil)
	stores := make([]*metapb.Store, 0, n)
	for i := 0; i < n; i++ {
		store := &metapb.Store{Id: uint64(i)}
		stores = append(stores, store)
	}

	for _, store := range stores {
		c.Assert(storage.SaveStore(store), IsNil)
	}

	regions := make([]*metapb.Region, 0, n)
	for i := uint64(0); i < uint64(n); i++ {
		region := &metapb.Region{
			Id:          i,
			StartKey:    []byte(fmt.Sprintf("%20d", i)),
			EndKey:      []byte(fmt.Sprintf("%20d", i+1)),
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
		}
		regions = append(regions, region)
	}

	for _, region := range regions {
		c.Assert(storage.SaveRegion(region), IsNil)
	}
	c.Assert(storage.Flush(), IsNil)

	raftCluster = cluster.NewRaftCluster(s.ctx, svr.ClusterID(), syncer.NewRegionSyncer(svr), svr.GetClient(), svr.GetHTTPClient())
	raftCluster.InitCluster(mockid.NewIDAllocator(), opt, storage, basicCluster)
	raftCluster, err = raftCluster.LoadClusterInfo()
	c.Assert(err, IsNil)
	c.Assert(raftCluster, NotNil)

	// Check meta, stores, and regions.
	c.Assert(raftCluster.GetMetaCluster(), DeepEquals, meta)
	c.Assert(raftCluster.GetStoreCount(), Equals, n)
	for _, store := range raftCluster.GetMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()])
	}
	c.Assert(raftCluster.GetRegionCount(), Equals, n)
	for _, region := range raftCluster.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
	}

	m := 20
	regions = make([]*metapb.Region, 0, n)
	for i := uint64(0); i < uint64(m); i++ {
		region := &metapb.Region{
			Id:          i,
			StartKey:    []byte(fmt.Sprintf("%20d", i)),
			EndKey:      []byte(fmt.Sprintf("%20d", i+1)),
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
		}
		regions = append(regions, region)
	}

	for _, region := range regions {
		c.Assert(storage.SaveRegion(region), IsNil)
	}
	raftCluster.GetStorage().LoadRegionsOnce(s.ctx, raftCluster.GetBasicCluster().PutRegion)
	c.Assert(raftCluster.GetRegionCount(), Equals, n)
}

func (s *clusterTestSuite) TestTiFlashWithPlacementRules(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1, func(cfg *config.Config, name string) { cfg.Replication.EnablePlacementRules = false })
	defer tc.Destroy()
	c.Assert(err, IsNil)
	err = tc.RunInitialServers()
	c.Assert(err, IsNil)
	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient)

	tiflashStore := &metapb.Store{
		Id:      11,
		Address: "127.0.0.1:1",
		Labels:  []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}},
		Version: "v4.1.0",
	}

	// cannot put TiFlash node without placement rules
	_, err = putStore(grpcPDClient, clusterID, tiflashStore)
	c.Assert(err, NotNil)
	rep := leaderServer.GetConfig().Replication
	rep.EnablePlacementRules = true
	svr := leaderServer.GetServer()
	err = svr.SetReplicationConfig(rep)
	c.Assert(err, IsNil)
	_, err = putStore(grpcPDClient, clusterID, tiflashStore)
	c.Assert(err, IsNil)
	// test TiFlash store limit
	expect := map[uint64]config.StoreLimitConfig{11: {AddPeer: 30, RemovePeer: 30}}
	c.Assert(svr.GetScheduleConfig().StoreLimit, DeepEquals, expect)

	// cannot disable placement rules with TiFlash nodes
	rep.EnablePlacementRules = false
	err = svr.SetReplicationConfig(rep)
	c.Assert(err, NotNil)
	err = svr.GetRaftCluster().BuryStore(11, true)
	c.Assert(err, IsNil)
	err = svr.SetReplicationConfig(rep)
	c.Assert(err, IsNil)
	c.Assert(len(svr.GetScheduleConfig().StoreLimit), Equals, 0)
}

func (s *clusterTestSuite) TestReplicationModeStatus(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1, func(conf *config.Config, serverName string) {
		conf.ReplicationMode.ReplicationMode = "dr-auto-sync"
	})

	defer tc.Destroy()
	c.Assert(err, IsNil)
	err = tc.RunInitialServers()
	c.Assert(err, IsNil)
	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	req := newBootstrapRequest(clusterID)
	res, err := grpcPDClient.Bootstrap(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(res.GetReplicationStatus().GetMode(), Equals, replication_modepb.ReplicationMode_DR_AUTO_SYNC) // check status in bootstrap response
	store := &metapb.Store{Id: 11, Address: "127.0.0.1:1", Version: "v4.1.0"}
	putRes, err := putStore(grpcPDClient, clusterID, store)
	c.Assert(err, IsNil)
	c.Assert(putRes.GetReplicationStatus().GetMode(), Equals, replication_modepb.ReplicationMode_DR_AUTO_SYNC) // check status in putStore response
	hbReq := &pdpb.StoreHeartbeatRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Stats:  &pdpb.StoreStats{StoreId: store.GetId()},
	}
	hbRes, err := grpcPDClient.StoreHeartbeat(context.Background(), hbReq)
	c.Assert(err, IsNil)
	c.Assert(hbRes.GetReplicationStatus().GetMode(), Equals, replication_modepb.ReplicationMode_DR_AUTO_SYNC) // check status in store heartbeat response
}

func newIsBootstrapRequest(clusterID uint64) *pdpb.IsBootstrappedRequest {
	req := &pdpb.IsBootstrappedRequest{
		Header: testutil.NewRequestHeader(clusterID),
	}

	return req
}

func newBootstrapRequest(clusterID uint64) *pdpb.BootstrapRequest {
	req := &pdpb.BootstrapRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  &metapb.Store{Id: 1, Address: testStoreAddr},
		Region: &metapb.Region{Id: 2, Peers: []*metapb.Peer{{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter}}},
	}

	return req
}

// helper function to check and bootstrap.
func bootstrapCluster(c *C, clusterID uint64, grpcPDClient pdpb.PDClient) {
	req := newBootstrapRequest(clusterID)
	_, err := grpcPDClient.Bootstrap(context.Background(), req)
	c.Assert(err, IsNil)
}

func putStore(grpcPDClient pdpb.PDClient, clusterID uint64, store *metapb.Store) (*pdpb.PutStoreResponse, error) {
	req := &pdpb.PutStoreRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Store:  store,
	}
	resp, err := grpcPDClient.PutStore(context.Background(), req)
	return resp, err
}

func getStore(c *C, clusterID uint64, grpcPDClient pdpb.PDClient, storeID uint64) *metapb.Store {
	req := &pdpb.GetStoreRequest{
		Header:  testutil.NewRequestHeader(clusterID),
		StoreId: storeID,
	}
	resp, err := grpcPDClient.GetStore(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetStore().GetId(), Equals, storeID)

	return resp.GetStore()
}

func getRegion(c *C, clusterID uint64, grpcPDClient pdpb.PDClient, regionKey []byte) *metapb.Region {
	req := &pdpb.GetRegionRequest{
		Header:    testutil.NewRequestHeader(clusterID),
		RegionKey: regionKey,
	}

	resp, err := grpcPDClient.GetRegion(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetRegion(), NotNil)

	return resp.GetRegion()
}

func getRegionByID(c *C, clusterID uint64, grpcPDClient pdpb.PDClient, regionID uint64) *metapb.Region {
	req := &pdpb.GetRegionByIDRequest{
		Header:   testutil.NewRequestHeader(clusterID),
		RegionId: regionID,
	}

	resp, err := grpcPDClient.GetRegionByID(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetRegion(), NotNil)

	return resp.GetRegion()
}

func getClusterConfig(c *C, clusterID uint64, grpcPDClient pdpb.PDClient) *metapb.Cluster {
	req := &pdpb.GetClusterConfigRequest{Header: testutil.NewRequestHeader(clusterID)}

	resp, err := grpcPDClient.GetClusterConfig(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetCluster(), NotNil)

	return resp.GetCluster()
}

func (s *clusterTestSuite) TestOfflineStoreLimit(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)
	err = tc.RunInitialServers()
	c.Assert(err, IsNil)
	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient)
	storeAddrs := []string{"127.0.1.1:0", "127.0.1.1:1"}
	rc := leaderServer.GetRaftCluster()
	c.Assert(rc, NotNil)
	rc.SetStorage(storage.NewStorageWithMemoryBackend())
	id := leaderServer.GetAllocator()
	for _, addr := range storeAddrs {
		storeID, err := id.Alloc()
		c.Assert(err, IsNil)
		store := newMetaStore(storeID, addr, "4.0.0", metapb.StoreState_Up, getTestDeployPath(storeID))
		_, err = putStore(grpcPDClient, clusterID, store)
		c.Assert(err, IsNil)
	}
	for i := uint64(1); i <= 2; i++ {
		r := &metapb.Region{
			Id: i,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i + 1)},
			EndKey:   []byte{byte(i + 2)},
			Peers:    []*metapb.Peer{{Id: i + 10, StoreId: i}},
		}
		region := core.NewRegionInfo(r, r.Peers[0], core.SetApproximateSize(10))

		err = rc.HandleRegionHeartbeat(region)
		c.Assert(err, IsNil)
	}

	oc := rc.GetOperatorController()
	opt := rc.GetOpts()
	opt.SetAllStoresLimit(storelimit.RemovePeer, 1)
	// only can add 5 remove peer operators on store 1
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewTestOperator(1, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 1})
		c.Assert(oc.AddOperator(op), IsTrue)
		c.Assert(oc.RemoveOperator(op), IsTrue)
	}
	op := operator.NewTestOperator(1, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 1})
	c.Assert(oc.AddOperator(op), IsFalse)
	c.Assert(oc.RemoveOperator(op), IsFalse)

	// only can add 5 remove peer operators on store 2
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewTestOperator(2, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
		c.Assert(oc.AddOperator(op), IsTrue)
		c.Assert(oc.RemoveOperator(op), IsTrue)
	}
	op = operator.NewTestOperator(2, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
	c.Assert(oc.AddOperator(op), IsFalse)
	c.Assert(oc.RemoveOperator(op), IsFalse)

	// reset all store limit
	opt.SetAllStoresLimit(storelimit.RemovePeer, 2)

	// only can add 5 remove peer operators on store 2
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewTestOperator(2, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
		c.Assert(oc.AddOperator(op), IsTrue)
		c.Assert(oc.RemoveOperator(op), IsTrue)
	}
	op = operator.NewTestOperator(2, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 2})
	c.Assert(oc.AddOperator(op), IsFalse)
	c.Assert(oc.RemoveOperator(op), IsFalse)

	// offline store 1
	rc.SetStoreLimit(1, storelimit.RemovePeer, storelimit.Unlimited)
	rc.RemoveStore(1, false)

	// can add unlimited remove peer operators on store 1
	for i := uint64(1); i <= 30; i++ {
		op := operator.NewTestOperator(1, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 1})
		c.Assert(oc.AddOperator(op), IsTrue)
		c.Assert(oc.RemoveOperator(op), IsTrue)
	}
}

func (s *clusterTestSuite) TestUpgradeStoreLimit(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)
	err = tc.RunInitialServers()
	c.Assert(err, IsNil)
	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient)
	rc := leaderServer.GetRaftCluster()
	c.Assert(rc, NotNil)
	rc.SetStorage(storage.NewStorageWithMemoryBackend())
	store := newMetaStore(1, "127.0.1.1:0", "4.0.0", metapb.StoreState_Up, "test/store1")
	_, err = putStore(grpcPDClient, clusterID, store)
	c.Assert(err, IsNil)
	r := &metapb.Region{
		Id: 1,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		StartKey: []byte{byte(2)},
		EndKey:   []byte{byte(3)},
		Peers:    []*metapb.Peer{{Id: 11, StoreId: uint64(1)}},
	}
	region := core.NewRegionInfo(r, r.Peers[0], core.SetApproximateSize(10))

	err = rc.HandleRegionHeartbeat(region)
	c.Assert(err, IsNil)

	// restart PD
	// Here we use an empty storelimit to simulate the upgrade progress.
	opt := rc.GetOpts()
	scheduleCfg := opt.GetScheduleConfig().Clone()
	scheduleCfg.StoreLimit = map[uint64]config.StoreLimitConfig{}
	c.Assert(leaderServer.GetServer().SetScheduleConfig(*scheduleCfg), IsNil)
	err = leaderServer.Stop()
	c.Assert(err, IsNil)
	err = leaderServer.Run()
	c.Assert(err, IsNil)

	oc := rc.GetOperatorController()
	// only can add 5 remove peer operators on store 1
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewTestOperator(1, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 1})
		c.Assert(oc.AddOperator(op), IsTrue)
		c.Assert(oc.RemoveOperator(op), IsTrue)
	}
	op := operator.NewTestOperator(1, &metapb.RegionEpoch{ConfVer: 1, Version: 1}, operator.OpRegion, operator.RemovePeer{FromStore: 1})
	c.Assert(oc.AddOperator(op), IsFalse)
	c.Assert(oc.RemoveOperator(op), IsFalse)
}

func (s *clusterTestSuite) TestStaleTermHeartbeat(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 1)
	c.Assert(err, IsNil)
	defer tc.Destroy()

	err = tc.RunInitialServers()
	c.Assert(err, IsNil)

	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient)
	storeAddrs := []string{"127.0.1.1:0", "127.0.1.1:1", "127.0.1.1:2"}
	rc := leaderServer.GetRaftCluster()
	c.Assert(rc, NotNil)
	rc.SetStorage(storage.NewStorageWithMemoryBackend())
	peers := make([]*metapb.Peer, 0, len(storeAddrs))
	id := leaderServer.GetAllocator()
	for _, addr := range storeAddrs {
		storeID, err := id.Alloc()
		c.Assert(err, IsNil)
		peerID, err := id.Alloc()
		c.Assert(err, IsNil)
		store := newMetaStore(storeID, addr, "3.0.0", metapb.StoreState_Up, getTestDeployPath(storeID))
		_, err = putStore(grpcPDClient, clusterID, store)
		c.Assert(err, IsNil)
		peers = append(peers, &metapb.Peer{
			Id:      peerID,
			StoreId: storeID,
		})
	}

	regionReq := &pdpb.RegionHeartbeatRequest{
		Header: testutil.NewRequestHeader(clusterID),
		Region: &metapb.Region{
			Id:       1,
			Peers:    peers,
			StartKey: []byte{byte(2)},
			EndKey:   []byte{byte(3)},
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 2,
				Version: 1,
			},
		},
		Leader:          peers[0],
		Term:            5,
		ApproximateSize: 10,
	}

	region := core.RegionFromHeartbeat(regionReq)
	err = rc.HandleRegionHeartbeat(region)
	c.Assert(err, IsNil)

	// Transfer leader
	regionReq.Term = 6
	regionReq.Leader = peers[1]
	region = core.RegionFromHeartbeat(regionReq)
	err = rc.HandleRegionHeartbeat(region)
	c.Assert(err, IsNil)

	// issue #3379
	regionReq.KeysWritten = uint64(18446744073709551615)  // -1
	regionReq.BytesWritten = uint64(18446744073709550602) // -1024
	region = core.RegionFromHeartbeat(regionReq)
	c.Assert(region.GetKeysWritten(), Equals, uint64(0))
	c.Assert(region.GetBytesWritten(), Equals, uint64(0))
	err = rc.HandleRegionHeartbeat(region)
	c.Assert(err, IsNil)

	// Stale heartbeat, update check should fail
	regionReq.Term = 5
	regionReq.Leader = peers[0]
	region = core.RegionFromHeartbeat(regionReq)
	err = rc.HandleRegionHeartbeat(region)
	c.Assert(err, NotNil)

	// Allow regions that are created by unsafe recover to send a heartbeat, even though they
	// are considered "stale" because their conf ver and version are both equal to 1.
	regionReq.Region.RegionEpoch.ConfVer = 1
	region = core.RegionFromHeartbeat(regionReq)
	err = rc.HandleRegionHeartbeat(region)
	c.Assert(err, IsNil)
}

func (s *clusterTestSuite) putRegionWithLeader(c *C, rc *cluster.RaftCluster, id id.Allocator, storeID uint64) {
	for i := 0; i < 3; i++ {
		regionID, err := id.Alloc()
		c.Assert(err, IsNil)
		peerID, err := id.Alloc()
		c.Assert(err, IsNil)
		region := &metapb.Region{
			Id:       regionID,
			Peers:    []*metapb.Peer{{Id: peerID, StoreId: storeID}},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
		}
		rc.HandleRegionHeartbeat(core.NewRegionInfo(region, region.Peers[0]))
	}
	c.Assert(rc.GetStore(storeID).GetLeaderCount(), Equals, 3)
}

func (s *clusterTestSuite) checkMinResolvedTSFromStorage(c *C, rc *cluster.RaftCluster, expect uint64) {
	time.Sleep(time.Millisecond * 10)
	ts2, err := rc.GetStorage().LoadMinResolvedTS()
	c.Assert(err, IsNil)
	c.Assert(ts2, Equals, expect)
}

func (s *clusterTestSuite) setMinResolvedTSPersistenceInterval(c *C, rc *cluster.RaftCluster, svr *server.Server, interval time.Duration) {
	cfg := rc.GetOpts().GetPDServerConfig().Clone()
	cfg.MinResolvedTSPersistenceInterval = typeutil.NewDuration(interval)
	err := svr.SetPDServerConfig(*cfg)
	c.Assert(err, IsNil)
	time.Sleep(time.Millisecond + interval)
}

func (s *clusterTestSuite) TestMinResolvedTS(c *C) {
	cluster.DefaultMinResolvedTSPersistenceInterval = time.Millisecond
	tc, err := tests.NewTestCluster(s.ctx, 1)
	defer tc.Destroy()
	c.Assert(err, IsNil)
	err = tc.RunInitialServers()
	c.Assert(err, IsNil)
	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	id := leaderServer.GetAllocator()
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	bootstrapCluster(c, clusterID, grpcPDClient)
	rc := leaderServer.GetRaftCluster()
	c.Assert(rc, NotNil)
	svr := leaderServer.GetServer()
	addStoreAndCheckMinResolvedTS := func(c *C, isTiflash bool, minResolvedTS, expect uint64) uint64 {
		storeID, err := id.Alloc()
		c.Assert(err, IsNil)
		store := &metapb.Store{
			Id:      storeID,
			Version: "v6.0.0",
			Address: "127.0.0.1:" + strconv.Itoa(int(storeID)),
		}
		if isTiflash {
			store.Labels = []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}
		}
		_, err = putStore(grpcPDClient, clusterID, store)
		c.Assert(err, IsNil)
		req := &pdpb.ReportMinResolvedTsRequest{
			Header:        testutil.NewRequestHeader(clusterID),
			StoreId:       storeID,
			MinResolvedTs: minResolvedTS,
		}
		_, err = grpcPDClient.ReportMinResolvedTS(context.Background(), req)
		c.Assert(err, IsNil)
		ts := rc.GetMinResolvedTS()
		c.Assert(ts, Equals, expect)
		return storeID
	}

	// case1: cluster is no initialized
	// min resolved ts should be not available
	status, err := rc.LoadClusterStatus()
	c.Assert(err, IsNil)
	c.Assert(status.IsInitialized, IsFalse)
	store1TS := uint64(233)
	store1 := addStoreAndCheckMinResolvedTS(c, false /* not tiflash */, store1TS, math.MaxUint64)

	// case2: add leader peer to store1 but no run job
	// min resolved ts should be zero
	s.putRegionWithLeader(c, rc, id, store1)
	ts := rc.GetMinResolvedTS()
	c.Assert(ts, Equals, uint64(0))

	// case3: add leader peer to store1 and run job
	// min resolved ts should be store1TS
	s.setMinResolvedTSPersistenceInterval(c, rc, svr, time.Millisecond)
	ts = rc.GetMinResolvedTS()
	c.Assert(ts, Equals, store1TS)
	s.checkMinResolvedTSFromStorage(c, rc, ts)

	// case4: add tiflash store
	// min resolved ts should no change
	addStoreAndCheckMinResolvedTS(c, true /* is tiflash */, 0, store1TS)

	// case5: add new store with lager min resolved ts
	// min resolved ts should no change
	store3TS := store1TS + 10
	store3 := addStoreAndCheckMinResolvedTS(c, false /* not tiflash */, store3TS, store1TS)
	s.putRegionWithLeader(c, rc, id, store3)

	// case6: set store1 to tombstone
	// min resolved ts should change to store 3
	resetStoreState(c, rc, store1, metapb.StoreState_Tombstone)
	ts = rc.GetMinResolvedTS()
	c.Assert(ts, Equals, store3TS)

	// case7: add a store with leader peer but no report min resolved ts
	// min resolved ts should be no change
	s.checkMinResolvedTSFromStorage(c, rc, store3TS)
	store4 := addStoreAndCheckMinResolvedTS(c, false /* not tiflash */, 0, store3TS)
	s.putRegionWithLeader(c, rc, id, store4)
	ts = rc.GetMinResolvedTS()
	c.Assert(ts, Equals, store3TS)
	s.checkMinResolvedTSFromStorage(c, rc, store3TS)
	resetStoreState(c, rc, store4, metapb.StoreState_Tombstone)

	// case8: set min resolved ts persist interval to zero
	// although min resolved ts increase, it should be not persisted until job running.
	store5TS := store3TS + 10
	s.setMinResolvedTSPersistenceInterval(c, rc, svr, 0)
	store5 := addStoreAndCheckMinResolvedTS(c, false /* not tiflash */, store5TS, store3TS)
	resetStoreState(c, rc, store3, metapb.StoreState_Tombstone)
	s.putRegionWithLeader(c, rc, id, store5)
	ts = rc.GetMinResolvedTS()
	c.Assert(ts, Equals, store3TS)
	s.setMinResolvedTSPersistenceInterval(c, rc, svr, time.Millisecond)
	ts = rc.GetMinResolvedTS()
	c.Assert(ts, Equals, store5TS)
}

// See https://github.com/tikv/pd/issues/4941
func (s *clusterTestSuite) TestTransferLeaderBack(c *C) {
	tc, err := tests.NewTestCluster(s.ctx, 2)
	defer tc.Destroy()
	c.Assert(err, IsNil)
	err = tc.RunInitialServers()
	c.Assert(err, IsNil)
	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	svr := leaderServer.GetServer()
	rc := cluster.NewRaftCluster(s.ctx, svr.ClusterID(), syncer.NewRegionSyncer(svr), svr.GetClient(), svr.GetHTTPClient())
	rc.InitCluster(svr.GetAllocator(), svr.GetPersistOptions(), svr.GetStorage(), svr.GetBasicCluster())
	storage := rc.GetStorage()
	meta := &metapb.Cluster{Id: 123}
	c.Assert(storage.SaveMeta(meta), IsNil)
	n := 4
	stores := make([]*metapb.Store, 0, n)
	for i := 1; i <= n; i++ {
		store := &metapb.Store{Id: uint64(i), State: metapb.StoreState_Up}
		stores = append(stores, store)
	}

	for _, store := range stores {
		c.Assert(storage.SaveStore(store), IsNil)
	}
	rc, err = rc.LoadClusterInfo()
	c.Assert(err, IsNil)
	c.Assert(rc, NotNil)
	// offline a store
	c.Assert(rc.RemoveStore(1, false), IsNil)
	c.Assert(rc.GetStore(1).GetState(), Equals, metapb.StoreState_Offline)

	// transfer PD leader to another PD
	tc.ResignLeader()
	tc.WaitLeader()
	leaderServer = tc.GetServer(tc.GetLeader())
	svr1 := leaderServer.GetServer()
	rc1 := svr1.GetRaftCluster()
	c.Assert(err, IsNil)
	c.Assert(rc1, NotNil)
	// tombstone a store, and remove its record
	c.Assert(rc1.BuryStore(1, false), IsNil)
	c.Assert(rc1.RemoveTombStoneRecords(), IsNil)

	// transfer PD leader back to the previous PD
	tc.ResignLeader()
	tc.WaitLeader()
	leaderServer = tc.GetServer(tc.GetLeader())
	svr = leaderServer.GetServer()
	rc = svr.GetRaftCluster()
	c.Assert(rc, NotNil)

	// check store count
	c.Assert(rc.GetMetaCluster(), DeepEquals, meta)
	c.Assert(rc.GetStoreCount(), Equals, 3)
}
