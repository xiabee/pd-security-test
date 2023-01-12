// Copyright 2018 TiKV Project Authors.
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

package syncer_test

import (
	"context"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
)

func Test(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

var _ = SerialSuites(&regionSyncerTestSuite{})

type regionSyncerTestSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *regionSyncerTestSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *regionSyncerTestSuite) TearDownSuite(c *C) {
	s.cancel()
}

type idAllocator struct {
	allocator *mockid.IDAllocator
}

func (i *idAllocator) alloc() uint64 {
	v, _ := i.allocator.Alloc()
	return v
}

func (s *regionSyncerTestSuite) TestRegionSyncer(c *C) {
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/core/regionStorageFastFlush", `return(true)`), IsNil)
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/core/noFastExitSync", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/tikv/pd/server/core/regionStorageFastFlush")
	defer failpoint.Disable("github.com/tikv/pd/server/core/noFastExitSync")

	cluster, err := tests.NewTestCluster(s.ctx, 3, func(conf *config.Config, serverName string) { conf.PDServerCfg.UseRegionStorage = true })
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	rc := leaderServer.GetServer().GetRaftCluster()
	c.Assert(rc, NotNil)
	c.Assert(cluster.WaitRegionSyncerClientsReady(2), IsTrue)

	regionLen := 110
	regions := initRegions(regionLen)
	for _, region := range regions {
		err = rc.HandleRegionHeartbeat(region)
		c.Assert(err, IsNil)
	}
	// merge case
	// region2 -> region1 -> region0
	// merge A to B will increases version to max(versionA, versionB)+1, but does not increase conver
	regions[0] = regions[0].Clone(core.WithEndKey(regions[2].GetEndKey()), core.WithIncVersion(), core.WithIncVersion())
	err = rc.HandleRegionHeartbeat(regions[2])
	c.Assert(err, IsNil)

	// merge case
	// region3 -> region4
	// merge A to B will increases version to max(versionA, versionB)+1, but does not increase conver
	regions[4] = regions[3].Clone(core.WithEndKey(regions[4].GetEndKey()), core.WithIncVersion())
	err = rc.HandleRegionHeartbeat(regions[4])
	c.Assert(err, IsNil)

	// merge case
	// region0 -> region4
	// merge A to B will increases version to max(versionA, versionB)+1, but does not increase conver
	regions[4] = regions[0].Clone(core.WithEndKey(regions[4].GetEndKey()), core.WithIncVersion(), core.WithIncVersion())
	err = rc.HandleRegionHeartbeat(regions[4])
	c.Assert(err, IsNil)
	regions = regions[4:]
	regionLen = len(regions)

	// change the statistics of regions
	for i := 0; i < len(regions); i++ {
		idx := uint64(i)
		regions[i] = regions[i].Clone(
			core.SetWrittenBytes(idx+10),
			core.SetWrittenKeys(idx+20),
			core.SetReadBytes(idx+30),
			core.SetReadKeys(idx+40))
		err = rc.HandleRegionHeartbeat(regions[i])
		c.Assert(err, IsNil)
	}

	// change the leader of region
	for i := 0; i < len(regions); i++ {
		regions[i] = regions[i].Clone(core.WithLeader(regions[i].GetPeers()[1]))
		err = rc.HandleRegionHeartbeat(regions[i])
		c.Assert(err, IsNil)
	}

	// ensure flush to region storage, we use a duration larger than the
	// region storage flush rate limit (3s).
	time.Sleep(4 * time.Second)

	// test All regions have been synchronized to the cache of followerServer
	followerServer := cluster.GetServer(cluster.GetFollower())
	c.Assert(followerServer, NotNil)
	cacheRegions := leaderServer.GetServer().GetBasicCluster().GetRegions()
	c.Assert(cacheRegions, HasLen, regionLen)
	testutil.WaitUntil(c, func(c *C) bool {
		for _, region := range cacheRegions {
			r := followerServer.GetServer().GetBasicCluster().GetRegion(region.GetID())
			if !(c.Check(r.GetMeta(), DeepEquals, region.GetMeta()) &&
				c.Check(r.GetStat(), DeepEquals, region.GetStat()) &&
				c.Check(r.GetLeader(), DeepEquals, region.GetLeader())) {
				return false
			}
		}
		return true
	})

	err = leaderServer.Stop()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer = cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer, NotNil)
	loadRegions := leaderServer.GetServer().GetRaftCluster().GetRegions()
	c.Assert(loadRegions, HasLen, regionLen)
	for _, region := range regions {
		r := leaderServer.GetRegionInfoByID(region.GetID())
		c.Assert(r.GetMeta(), DeepEquals, region.GetMeta())
		c.Assert(r.GetStat(), DeepEquals, region.GetStat())
		c.Assert(r.GetLeader(), DeepEquals, region.GetLeader())
	}
}

func (s *regionSyncerTestSuite) TestFullSyncWithAddMember(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1, func(conf *config.Config, serverName string) { conf.PDServerCfg.UseRegionStorage = true })
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	rc := leaderServer.GetServer().GetRaftCluster()
	c.Assert(rc, NotNil)
	regionLen := 110
	regions := initRegions(regionLen)
	for _, region := range regions {
		err = rc.HandleRegionHeartbeat(region)
		c.Assert(err, IsNil)
	}
	// ensure flush to region storage
	time.Sleep(3 * time.Second)
	// restart pd1
	err = leaderServer.Stop()
	c.Assert(err, IsNil)
	err = leaderServer.Run()
	c.Assert(err, IsNil)
	c.Assert(cluster.WaitLeader(), Equals, "pd1")

	// join new PD
	pd2, err := cluster.Join(s.ctx)
	c.Assert(err, IsNil)
	err = pd2.Run()
	c.Assert(err, IsNil)
	c.Assert(cluster.WaitLeader(), Equals, "pd1")
	// waiting for synchronization to complete
	time.Sleep(3 * time.Second)
	err = cluster.ResignLeader()
	c.Assert(err, IsNil)
	c.Assert(cluster.WaitLeader(), Equals, "pd2")
	loadRegions := pd2.GetServer().GetRaftCluster().GetRegions()
	c.Assert(loadRegions, HasLen, regionLen)
}

func (s *regionSyncerTestSuite) TestPrepareChecker(c *C) {
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/cluster/changeCoordinatorTicker", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/tikv/pd/server/cluster/changeCoordinatorTicker")
	cluster, err := tests.NewTestCluster(s.ctx, 1, func(conf *config.Config, serverName string) { conf.PDServerCfg.UseRegionStorage = true })
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	rc := leaderServer.GetServer().GetRaftCluster()
	c.Assert(rc, NotNil)
	regionLen := 110
	regions := initRegions(regionLen)
	for _, region := range regions {
		err = rc.HandleRegionHeartbeat(region)
		c.Assert(err, IsNil)
	}

	// ensure flush to region storage
	time.Sleep(3 * time.Second)
	c.Assert(leaderServer.GetRaftCluster().IsPrepared(), IsTrue)

	// join new PD
	pd2, err := cluster.Join(s.ctx)
	c.Assert(err, IsNil)
	err = pd2.Run()
	c.Assert(err, IsNil)
	// waiting for synchronization to complete
	time.Sleep(3 * time.Second)
	err = cluster.ResignLeader()
	c.Assert(err, IsNil)
	c.Assert(cluster.WaitLeader(), Equals, "pd2")
	leaderServer = cluster.GetServer(cluster.GetLeader())
	rc = leaderServer.GetServer().GetRaftCluster()
	for _, region := range regions {
		err = rc.HandleRegionHeartbeat(region)
		c.Assert(err, IsNil)
	}
	time.Sleep(time.Second)
	c.Assert(rc.IsPrepared(), IsTrue)
}

func initRegions(regionLen int) []*core.RegionInfo {
	allocator := &idAllocator{allocator: mockid.NewIDAllocator()}
	regions := make([]*core.RegionInfo, 0, regionLen)
	for i := 0; i < regionLen; i++ {
		r := &metapb.Region{
			Id: allocator.alloc(),
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
			Peers: []*metapb.Peer{
				{Id: allocator.alloc(), StoreId: uint64(0)},
				{Id: allocator.alloc(), StoreId: uint64(0)},
			},
		}
		regions = append(regions, core.NewRegionInfo(r, r.Peers[0]))
	}
	return regions
}
