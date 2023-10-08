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

package statistics

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/schedule/placement"
)

// RegionInfoProvider is an interface to provide the region information.
type RegionInfoProvider interface {
	// GetRegion returns the region information according to the given region ID.
	GetRegion(regionID uint64) *core.RegionInfo
}

// RegionStatisticType represents the type of the region's status.
type RegionStatisticType uint32

// region status type
const (
	MissPeer RegionStatisticType = 1 << iota
	ExtraPeer
	DownPeer
	PendingPeer
	OfflinePeer
	LearnerPeer
	EmptyRegion
	OversizedRegion
	UndersizedRegion
	WitnessLeader
)

var regionStatisticTypes = []RegionStatisticType{
	MissPeer,
	ExtraPeer,
	DownPeer,
	PendingPeer,
	OfflinePeer,
	LearnerPeer,
	EmptyRegion,
	OversizedRegion,
	UndersizedRegion,
	WitnessLeader,
}

const nonIsolation = "none"

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	regionMissPeerRegionCounter      = regionStatusGauge.WithLabelValues("miss-peer-region-count")
	regionExtraPeerRegionCounter     = regionStatusGauge.WithLabelValues("extra-peer-region-count")
	regionDownPeerRegionCounter      = regionStatusGauge.WithLabelValues("down-peer-region-count")
	regionPendingPeerRegionCounter   = regionStatusGauge.WithLabelValues("pending-peer-region-count")
	regionOfflinePeerRegionCounter   = regionStatusGauge.WithLabelValues("offline-peer-region-count")
	regionLearnerPeerRegionCounter   = regionStatusGauge.WithLabelValues("learner-peer-region-count")
	regionEmptyRegionCounter         = regionStatusGauge.WithLabelValues("empty-region-count")
	regionOversizedRegionCounter     = regionStatusGauge.WithLabelValues("oversized-region-count")
	regionUndersizedRegionCounter    = regionStatusGauge.WithLabelValues("undersized-region-count")
	regionWitnessLeaderRegionCounter = regionStatusGauge.WithLabelValues("witness-leader-region-count")
)

// RegionInfoWithTS is used to record the extra timestamp status of a region.
type RegionInfoWithTS struct {
	id                   uint64
	startMissVoterPeerTS int64
	startDownPeerTS      int64
}

// RegionStatistics is used to record the status of regions.
type RegionStatistics struct {
	sync.RWMutex
	rip         RegionInfoProvider
	conf        sc.CheckerConfigProvider
	stats       map[RegionStatisticType]map[uint64]*RegionInfoWithTS
	index       map[uint64]RegionStatisticType
	ruleManager *placement.RuleManager
}

// NewRegionStatistics creates a new RegionStatistics.
func NewRegionStatistics(
	rip RegionInfoProvider,
	conf sc.CheckerConfigProvider,
	ruleManager *placement.RuleManager,
) *RegionStatistics {
	r := &RegionStatistics{
		rip:         rip,
		conf:        conf,
		ruleManager: ruleManager,
		stats:       make(map[RegionStatisticType]map[uint64]*RegionInfoWithTS),
		index:       make(map[uint64]RegionStatisticType),
	}
	for _, typ := range regionStatisticTypes {
		r.stats[typ] = make(map[uint64]*RegionInfoWithTS)
	}
	return r
}

// GetRegionStatsByType gets the status of the region by types.
// The regions here need to be cloned, otherwise, it may cause data race problems.
func (r *RegionStatistics) GetRegionStatsByType(typ RegionStatisticType) []*core.RegionInfo {
	r.RLock()
	defer r.RUnlock()
	res := make([]*core.RegionInfo, 0, len(r.stats[typ]))
	for regionID := range r.stats[typ] {
		res = append(res, r.rip.GetRegion(regionID).Clone())
	}
	return res
}

// IsRegionStatsType returns whether the status of the region is the given type.
func (r *RegionStatistics) IsRegionStatsType(regionID uint64, typ RegionStatisticType) bool {
	r.RLock()
	defer r.RUnlock()
	_, exist := r.stats[typ][regionID]
	return exist
}

func (r *RegionStatistics) deleteEntry(deleteIndex RegionStatisticType, regionID uint64) {
	for typ := RegionStatisticType(1); typ <= deleteIndex; typ <<= 1 {
		if deleteIndex&typ != 0 {
			delete(r.stats[typ], regionID)
		}
	}
}

// RegionStatsNeedUpdate checks whether the region's status need to be updated
// due to some special state types.
func (r *RegionStatistics) RegionStatsNeedUpdate(region *core.RegionInfo) bool {
	regionID := region.GetID()
	if r.IsRegionStatsType(regionID, OversizedRegion) !=
		region.IsOversized(int64(r.conf.GetRegionMaxSize()), int64(r.conf.GetRegionMaxKeys())) {
		return true
	}
	return r.IsRegionStatsType(regionID, UndersizedRegion) !=
		region.NeedMerge(int64(r.conf.GetMaxMergeRegionSize()), int64(r.conf.GetMaxMergeRegionKeys()))
}

// Observe records the current regions' status.
func (r *RegionStatistics) Observe(region *core.RegionInfo, stores []*core.StoreInfo) {
	r.Lock()
	defer r.Unlock()
	var (
		desiredReplicas = r.conf.GetMaxReplicas()
		desiredVoters   = desiredReplicas
		peerTypeIndex   RegionStatisticType
		deleteIndex     RegionStatisticType
	)
	// Check if the region meets count requirements of its rules.
	if r.conf.IsPlacementRulesEnabled() {
		if !r.ruleManager.IsInitialized() {
			log.Warn("ruleManager haven't been initialized")
			return
		}
		desiredReplicas = 0
		desiredVoters = 0
		rules := r.ruleManager.GetRulesForApplyRegion(region)
		for _, rule := range rules {
			desiredReplicas += rule.Count
			if rule.Role != placement.Learner {
				desiredVoters += rule.Count
			}
		}
	}
	// Better to make sure once any of these conditions changes, it will trigger the heartbeat `save_cache`.
	// Otherwise, the state may be out-of-date for a long time, which needs another way to apply the change ASAP.
	// For example, see `RegionStatsNeedUpdate` above to know how `OversizedRegion` and `UndersizedRegion` are updated.
	conditions := map[RegionStatisticType]bool{
		MissPeer:    len(region.GetPeers()) < desiredReplicas,
		ExtraPeer:   len(region.GetPeers()) > desiredReplicas,
		DownPeer:    len(region.GetDownPeers()) > 0,
		PendingPeer: len(region.GetPendingPeers()) > 0,
		OfflinePeer: func() bool {
			for _, store := range stores {
				if store.IsRemoving() {
					peer := region.GetStorePeer(store.GetID())
					if peer != nil {
						return true
					}
				}
			}
			return false
		}(),
		LearnerPeer: len(region.GetLearners()) > 0,
		EmptyRegion: region.GetApproximateSize() <= core.EmptyRegionApproximateSize,
		OversizedRegion: region.IsOversized(
			int64(r.conf.GetRegionMaxSize()),
			int64(r.conf.GetRegionMaxKeys()),
		),
		UndersizedRegion: region.NeedMerge(
			int64(r.conf.GetMaxMergeRegionSize()),
			int64(r.conf.GetMaxMergeRegionKeys()),
		),
		WitnessLeader: region.GetLeader().GetIsWitness(),
	}
	// Check if the region meets any of the conditions and update the corresponding info.
	regionID := region.GetID()
	for typ, c := range conditions {
		if c {
			info := r.stats[typ][regionID]
			if info == nil {
				info = &RegionInfoWithTS{id: regionID}
			}
			if typ == DownPeer {
				if info.startDownPeerTS != 0 {
					regionDownPeerDuration.Observe(float64(time.Now().Unix() - info.startDownPeerTS))
				} else {
					info.startDownPeerTS = time.Now().Unix()
				}
			} else if typ == MissPeer && len(region.GetVoters()) < desiredVoters {
				if info.startMissVoterPeerTS != 0 {
					regionMissVoterPeerDuration.Observe(float64(time.Now().Unix() - info.startMissVoterPeerTS))
				} else {
					info.startMissVoterPeerTS = time.Now().Unix()
				}
			}

			r.stats[typ][regionID] = info
			peerTypeIndex |= typ
		}
	}
	// Remove the info if any of the conditions are not met any more.
	if oldIndex, ok := r.index[regionID]; ok {
		deleteIndex = oldIndex &^ peerTypeIndex
	}
	r.deleteEntry(deleteIndex, regionID)
	r.index[regionID] = peerTypeIndex
}

// ClearDefunctRegion is used to handle the overlap region.
func (r *RegionStatistics) ClearDefunctRegion(regionID uint64) {
	r.Lock()
	defer r.Unlock()
	if oldIndex, ok := r.index[regionID]; ok {
		r.deleteEntry(oldIndex, regionID)
	}
}

// Collect collects the metrics of the regions' status.
func (r *RegionStatistics) Collect() {
	r.RLock()
	defer r.RUnlock()
	regionMissPeerRegionCounter.Set(float64(len(r.stats[MissPeer])))
	regionExtraPeerRegionCounter.Set(float64(len(r.stats[ExtraPeer])))
	regionDownPeerRegionCounter.Set(float64(len(r.stats[DownPeer])))
	regionPendingPeerRegionCounter.Set(float64(len(r.stats[PendingPeer])))
	regionOfflinePeerRegionCounter.Set(float64(len(r.stats[OfflinePeer])))
	regionLearnerPeerRegionCounter.Set(float64(len(r.stats[LearnerPeer])))
	regionEmptyRegionCounter.Set(float64(len(r.stats[EmptyRegion])))
	regionOversizedRegionCounter.Set(float64(len(r.stats[OversizedRegion])))
	regionUndersizedRegionCounter.Set(float64(len(r.stats[UndersizedRegion])))
	regionWitnessLeaderRegionCounter.Set(float64(len(r.stats[WitnessLeader])))
}

// Reset resets the metrics of the regions' status.
func (r *RegionStatistics) Reset() {
	regionMissPeerRegionCounter.Set(0)
	regionExtraPeerRegionCounter.Set(0)
	regionDownPeerRegionCounter.Set(0)
	regionPendingPeerRegionCounter.Set(0)
	regionOfflinePeerRegionCounter.Set(0)
	regionLearnerPeerRegionCounter.Set(0)
	regionEmptyRegionCounter.Set(0)
	regionOversizedRegionCounter.Set(0)
	regionUndersizedRegionCounter.Set(0)
	regionWitnessLeaderRegionCounter.Set(0)
}

// LabelStatistics is the statistics of the level of labels.
type LabelStatistics struct {
	sync.RWMutex
	regionLabelStats map[uint64]string
	labelCounter     map[string]int
}

// NewLabelStatistics creates a new LabelStatistics.
func NewLabelStatistics() *LabelStatistics {
	return &LabelStatistics{
		regionLabelStats: make(map[uint64]string),
		labelCounter:     make(map[string]int),
	}
}

// Observe records the current label status.
func (l *LabelStatistics) Observe(region *core.RegionInfo, stores []*core.StoreInfo, labels []string) {
	regionID := region.GetID()
	regionIsolation := GetRegionLabelIsolation(stores, labels)
	l.Lock()
	defer l.Unlock()
	if label, ok := l.regionLabelStats[regionID]; ok {
		if label == regionIsolation {
			return
		}
		l.labelCounter[label]--
	}
	l.regionLabelStats[regionID] = regionIsolation
	l.labelCounter[regionIsolation]++
}

// Collect collects the metrics of the label status.
func (l *LabelStatistics) Collect() {
	l.RLock()
	defer l.RUnlock()
	for level, count := range l.labelCounter {
		regionLabelLevelGauge.WithLabelValues(level).Set(float64(count))
	}
}

// Reset resets the metrics of the label status.
func (l *LabelStatistics) Reset() {
	regionLabelLevelGauge.Reset()
}

// ClearDefunctRegion is used to handle the overlap region.
func (l *LabelStatistics) ClearDefunctRegion(regionID uint64) {
	l.Lock()
	defer l.Unlock()
	if label, ok := l.regionLabelStats[regionID]; ok {
		l.labelCounter[label]--
		delete(l.regionLabelStats, regionID)
	}
}

// GetLabelCounter is only used for tests.
func (l *LabelStatistics) GetLabelCounter() map[string]int {
	l.RLock()
	defer l.RUnlock()
	clonedLabelCounter := make(map[string]int, len(l.labelCounter))
	for k, v := range l.labelCounter {
		clonedLabelCounter[k] = v
	}
	return clonedLabelCounter
}

// GetRegionLabelIsolation returns the isolation level of the region.
func GetRegionLabelIsolation(stores []*core.StoreInfo, labels []string) string {
	if len(stores) == 0 || len(labels) == 0 {
		return nonIsolation
	}
	queueStores := [][]*core.StoreInfo{stores}
	for level, label := range labels {
		newQueueStores := make([][]*core.StoreInfo, 0, len(stores))
		for _, stores := range queueStores {
			notIsolatedStores := notIsolatedStoresWithLabel(stores, label)
			if len(notIsolatedStores) > 0 {
				newQueueStores = append(newQueueStores, notIsolatedStores...)
			}
		}
		queueStores = newQueueStores
		if len(queueStores) == 0 {
			return labels[level]
		}
	}
	return nonIsolation
}

func notIsolatedStoresWithLabel(stores []*core.StoreInfo, label string) [][]*core.StoreInfo {
	var emptyValueStores []*core.StoreInfo
	valueStoresMap := make(map[string][]*core.StoreInfo)

	for _, s := range stores {
		labelValue := s.GetLabelValue(label)
		if labelValue == "" {
			emptyValueStores = append(emptyValueStores, s)
		} else {
			valueStoresMap[labelValue] = append(valueStoresMap[labelValue], s)
		}
	}

	if len(valueStoresMap) == 0 {
		// Usually it is because all TiKVs lack this label.
		if len(emptyValueStores) > 1 {
			return [][]*core.StoreInfo{emptyValueStores}
		}
		return nil
	}

	var res [][]*core.StoreInfo
	if len(emptyValueStores) == 0 {
		// No TiKV lacks this label.
		for _, stores := range valueStoresMap {
			if len(stores) > 1 {
				res = append(res, stores)
			}
		}
	} else {
		// Usually it is because some TiKVs lack this label.
		// The TiKVs in each label and the TiKVs without label form a group.
		for _, stores := range valueStoresMap {
			stores = append(stores, emptyValueStores...)
			res = append(res, stores)
		}
	}
	return res
}
