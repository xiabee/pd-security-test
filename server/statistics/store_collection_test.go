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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
)

var _ = Suite(&testStoreStatisticsSuite{})

type testStoreStatisticsSuite struct{}

func (t *testStoreStatisticsSuite) TestStoreStatistics(c *C) {
	opt := config.NewTestOptions()
	rep := opt.GetReplicationConfig().Clone()
	rep.LocationLabels = []string{"zone", "host"}
	opt.SetReplicationConfig(rep)

	metaStores := []*metapb.Store{
		{Id: 1, Address: "mock://tikv-1", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h1"}}},
		{Id: 2, Address: "mock://tikv-2", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z1"}, {Key: "host", Value: "h2"}}},
		{Id: 3, Address: "mock://tikv-3", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z2"}, {Key: "host", Value: "h1"}}},
		{Id: 4, Address: "mock://tikv-4", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z2"}, {Key: "host", Value: "h2"}}},
		{Id: 5, Address: "mock://tikv-5", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z3"}, {Key: "host", Value: "h1"}}},
		{Id: 6, Address: "mock://tikv-6", Labels: []*metapb.StoreLabel{{Key: "zone", Value: "z3"}, {Key: "host", Value: "h2"}}},
		{Id: 7, Address: "mock://tikv-7", Labels: []*metapb.StoreLabel{{Key: "host", Value: "h1"}}},
		{Id: 8, Address: "mock://tikv-8", Labels: []*metapb.StoreLabel{{Key: "host", Value: "h2"}}},
		{Id: 8, Address: "mock://tikv-9", Labels: []*metapb.StoreLabel{{Key: "host", Value: "h3"}}, State: metapb.StoreState_Tombstone},
	}
	storesStats := NewStoresStats()
	stores := make([]*core.StoreInfo, 0, len(metaStores))
	for _, m := range metaStores {
		s := core.NewStoreInfo(m, core.SetLastHeartbeatTS(time.Now()))
		storesStats.GetOrCreateRollingStoreStats(m.GetId())
		stores = append(stores, s)
	}

	store3 := stores[3].Clone(core.OfflineStore(false))
	stores[3] = store3
	store4 := stores[4].Clone(core.SetLastHeartbeatTS(stores[4].GetLastHeartbeatTS().Add(-time.Hour)))
	stores[4] = store4
	storeStats := NewStoreStatisticsMap(opt)
	for _, store := range stores {
		storeStats.Observe(store, storesStats)
	}
	stats := storeStats.stats

	c.Assert(stats.Up, Equals, 6)
	c.Assert(stats.Down, Equals, 1)
	c.Assert(stats.Offline, Equals, 1)
	c.Assert(stats.RegionCount, Equals, 0)
	c.Assert(stats.Unhealthy, Equals, 0)
	c.Assert(stats.Disconnect, Equals, 0)
	c.Assert(stats.Tombstone, Equals, 1)
	c.Assert(stats.LowSpace, Equals, 8)
	c.Assert(stats.LabelCounter["zone:z1"], Equals, 2)
	c.Assert(stats.LabelCounter["zone:z2"], Equals, 2)
	c.Assert(stats.LabelCounter["zone:z3"], Equals, 2)
	c.Assert(stats.LabelCounter["host:h1"], Equals, 4)
	c.Assert(stats.LabelCounter["host:h2"], Equals, 4)
	c.Assert(stats.LabelCounter["zone:unknown"], Equals, 2)
}
