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

package cases

import (
	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/core"
	sc "github.com/tikv/pd/tools/pd-simulator/simulator/config"
	"github.com/tikv/pd/tools/pd-simulator/simulator/info"
)

func newRegionSplit(config *sc.SimConfig) *Case {
	var simCase Case
	totalStore := config.TotalStore

	for i := 0; i < totalStore; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:     uint64(i),
			Status: metapb.StoreState_Up,
		})
	}
	peers := []*metapb.Peer{
		{Id: 4, StoreId: 1},
	}
	simCase.Regions = append(simCase.Regions, Region{
		ID:     5,
		Peers:  peers,
		Leader: peers[0],
		Size:   1 * units.MiB,
		Keys:   10000,
	})

	simCase.RegionSplitSize = 128 * units.MiB
	simCase.RegionSplitKeys = 10000
	// Events description
	e := &WriteFlowOnSpotDescriptor{}
	e.Step = func(int64) map[string]int64 {
		return map[string]int64{
			"foobar": 8 * units.MiB,
		}
	}
	simCase.Events = []EventDescriptor{e}

	// Checker description
	simCase.Checker = func(regions *core.RegionsInfo, _ []info.StoreStats) bool {
		for i := 1; i <= totalStore; i++ {
			peerCount := regions.GetStoreRegionCount(uint64(i))
			if peerCount < 5 {
				return false
			}
		}
		return true
	}
	return &simCase
}
