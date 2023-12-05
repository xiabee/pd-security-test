// Copyright 2020 TiKV Project Authors.
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

package filter

import (
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
)

// StoreComparer compares 2 stores. Often used for StoreCandidates to
// sort candidate stores.
type StoreComparer func(a, b *core.StoreInfo) int

// RegionScoreComparer creates a StoreComparer to sort store by region
// score.
func RegionScoreComparer(opt *config.PersistOptions) StoreComparer {
	return func(a, b *core.StoreInfo) int {
		sa := a.RegionScore(opt.GetRegionScoreFormulaVersion(), opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0, 0)
		sb := b.RegionScore(opt.GetRegionScoreFormulaVersion(), opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0, 0)
		switch {
		case sa > sb:
			return 1
		case sa < sb:
			return -1
		default:
			return 0
		}
	}
}

// IsolationComparer creates a StoreComparer to sort store by isolation score.
func IsolationComparer(locationLabels []string, regionStores []*core.StoreInfo) StoreComparer {
	return func(a, b *core.StoreInfo) int {
		sa := core.DistinctScore(locationLabels, regionStores, a)
		sb := core.DistinctScore(locationLabels, regionStores, b)
		switch {
		case sa > sb:
			return 1
		case sa < sb:
			return -1
		default:
			return 0
		}
	}
}
