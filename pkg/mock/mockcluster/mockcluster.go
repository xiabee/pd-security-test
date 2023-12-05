// Copyright 2019 TiKV Project Authors.
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

package mockcluster

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/core/storelimit"
	"github.com/tikv/pd/server/kv"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/statistics"
	"github.com/tikv/pd/server/versioninfo"
)

const (
	defaultStoreCapacity = 100 * (1 << 30) // 100GiB
	defaultRegionSize    = 96 * (1 << 20)  // 96MiB
	mb                   = (1 << 20)       // 1MiB
)

// Cluster is used to mock clusterInfo for test use.
type Cluster struct {
	*core.BasicCluster
	*mockid.IDAllocator
	*placement.RuleManager
	*statistics.HotStat
	*config.PersistOptions
	ID               uint64
	suspectRegions   map[uint64]struct{}
	disabledFeatures map[versioninfo.Feature]struct{}
}

// NewCluster creates a new Cluster
func NewCluster(ctx context.Context, opts *config.PersistOptions) *Cluster {
	mockQuit := make(chan struct{})
	clus := &Cluster{
		BasicCluster:     core.NewBasicCluster(),
		IDAllocator:      mockid.NewIDAllocator(),
		HotStat:          statistics.NewHotStat(ctx, mockQuit),
		PersistOptions:   opts,
		suspectRegions:   map[uint64]struct{}{},
		disabledFeatures: make(map[versioninfo.Feature]struct{}),
	}
	if clus.PersistOptions.GetReplicationConfig().EnablePlacementRules {
		clus.initRuleManager()
	}
	return clus
}

// GetOpts returns the cluster configuration.
func (mc *Cluster) GetOpts() *config.PersistOptions {
	return mc.PersistOptions
}

// AllocID allocs a new unique ID.
func (mc *Cluster) AllocID() (uint64, error) {
	return mc.Alloc()
}

// ScanRegions scans region with start key, until number greater than limit.
func (mc *Cluster) ScanRegions(startKey, endKey []byte, limit int) []*core.RegionInfo {
	return mc.Regions.ScanRange(startKey, endKey, limit)
}

// LoadRegion puts region info without leader
func (mc *Cluster) LoadRegion(regionID uint64, followerIds ...uint64) {
	//  regions load from etcd will have no leader
	r := mc.newMockRegionInfo(regionID, 0, followerIds...).Clone(core.WithLeader(nil))
	mc.PutRegion(r)
}

// GetStoresLoads gets stores load statistics.
func (mc *Cluster) GetStoresLoads() map[uint64][]float64 {
	mc.HotStat.FilterUnhealthyStore(mc)
	return mc.HotStat.GetStoresLoads()
}

// GetStoreRegionCount gets region count with a given store.
func (mc *Cluster) GetStoreRegionCount(storeID uint64) int {
	return mc.Regions.GetStoreRegionCount(storeID)
}

// GetStore gets a store with a given store ID.
func (mc *Cluster) GetStore(storeID uint64) *core.StoreInfo {
	return mc.Stores.GetStore(storeID)
}

// IsRegionHot checks if the region is hot.
func (mc *Cluster) IsRegionHot(region *core.RegionInfo) bool {
	return mc.HotCache.IsRegionHot(region, mc.GetHotRegionCacheHitsThreshold())
}

// RegionReadStats returns hot region's read stats.
// The result only includes peers that are hot enough.
func (mc *Cluster) RegionReadStats() map[uint64][]*statistics.HotPeerStat {
	// We directly use threshold for read stats for mockCluster
	return mc.HotCache.RegionStats(statistics.ReadFlow, mc.GetHotRegionCacheHitsThreshold())
}

// RegionWriteStats returns hot region's write stats.
// The result only includes peers that are hot enough.
func (mc *Cluster) RegionWriteStats() map[uint64][]*statistics.HotPeerStat {
	return mc.HotCache.RegionStats(statistics.WriteFlow, mc.GetHotRegionCacheHitsThreshold())
}

// HotRegionsFromStore picks hot regions in specify store.
func (mc *Cluster) HotRegionsFromStore(store uint64, kind statistics.FlowKind) []*core.RegionInfo {
	stats := mc.HotCache.HotRegionsFromStore(store, kind, mc.GetHotRegionCacheHitsThreshold())
	regions := make([]*core.RegionInfo, 0, len(stats))
	for _, stat := range stats {
		region := mc.GetRegion(stat.RegionID)
		if region != nil {
			regions = append(regions, region)
		}
	}
	return regions
}

// AllocPeer allocs a new peer on a store.
func (mc *Cluster) AllocPeer(storeID uint64) (*metapb.Peer, error) {
	peerID, err := mc.AllocID()
	if err != nil {
		log.Error("failed to alloc peer", errs.ZapError(err))
		return nil, err
	}
	peer := &metapb.Peer{
		Id:      peerID,
		StoreId: storeID,
	}
	return peer, nil
}

func (mc *Cluster) initRuleManager() {
	if mc.RuleManager == nil {
		mc.RuleManager = placement.NewRuleManager(core.NewStorage(kv.NewMemoryKV()), mc)
		mc.RuleManager.Initialize(int(mc.GetReplicationConfig().MaxReplicas), mc.GetReplicationConfig().LocationLabels)
	}
}

// FitRegion fits a region to the rules it matches.
func (mc *Cluster) FitRegion(region *core.RegionInfo) *placement.RegionFit {
	return mc.RuleManager.FitRegion(mc.BasicCluster, region)
}

// GetRuleManager returns the ruleManager of the cluster.
func (mc *Cluster) GetRuleManager() *placement.RuleManager {
	return mc.RuleManager
}

// SetStoreUp sets store state to be up.
func (mc *Cluster) SetStoreUp(storeID uint64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.UpStore(),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutStore(newStore)
}

// SetStoreDisconnect changes a store's state to disconnected.
func (mc *Cluster) SetStoreDisconnect(storeID uint64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.UpStore(),
		core.SetLastHeartbeatTS(time.Now().Add(-time.Second*30)),
	)
	mc.PutStore(newStore)
}

// SetStoreDown sets store down.
func (mc *Cluster) SetStoreDown(storeID uint64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.UpStore(),
		core.SetLastHeartbeatTS(time.Time{}),
	)
	mc.PutStore(newStore)
}

// SetStoreOffline sets store state to be offline.
func (mc *Cluster) SetStoreOffline(storeID uint64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(core.OfflineStore(false))
	mc.PutStore(newStore)
}

// SetStoreBusy sets store busy.
func (mc *Cluster) SetStoreBusy(storeID uint64, busy bool) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.IsBusy = busy
	newStore := store.Clone(
		core.SetStoreStats(newStats),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutStore(newStore)
}

// AddLeaderStore adds store with specified count of leader.
func (mc *Cluster) AddLeaderStore(storeID uint64, leaderCount int, leaderSizes ...int64) {
	stats := &pdpb.StoreStats{}
	stats.Capacity = defaultStoreCapacity
	stats.UsedSize = uint64(leaderCount) * defaultRegionSize
	stats.Available = stats.Capacity - uint64(leaderCount)*defaultRegionSize
	var leaderSize int64
	if len(leaderSizes) != 0 {
		leaderSize = leaderSizes[0]
	} else {
		leaderSize = int64(leaderCount) * defaultRegionSize / mb
	}

	store := core.NewStoreInfo(
		&metapb.Store{Id: storeID},
		core.SetStoreStats(stats),
		core.SetLeaderCount(leaderCount),
		core.SetLeaderSize(leaderSize),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.SetStoreLimit(storeID, storelimit.AddPeer, 60)
	mc.SetStoreLimit(storeID, storelimit.RemovePeer, 60)
	mc.PutStore(store)
}

// AddRegionStore adds store with specified count of region.
func (mc *Cluster) AddRegionStore(storeID uint64, regionCount int) {
	stats := &pdpb.StoreStats{}
	stats.Capacity = defaultStoreCapacity
	stats.UsedSize = uint64(regionCount) * defaultRegionSize
	stats.Available = stats.Capacity - uint64(regionCount)*defaultRegionSize
	store := core.NewStoreInfo(
		&metapb.Store{Id: storeID, Labels: []*metapb.StoreLabel{
			{
				Key:   "ID",
				Value: fmt.Sprintf("%v", storeID),
			},
		}},
		core.SetStoreStats(stats),
		core.SetRegionCount(regionCount),
		core.SetRegionSize(int64(regionCount)*defaultRegionSize/mb),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.SetStoreLimit(storeID, storelimit.AddPeer, 60)
	mc.SetStoreLimit(storeID, storelimit.RemovePeer, 60)
	mc.PutStore(store)
}

// AddRegionStoreWithLeader adds store with specified count of region and leader.
func (mc *Cluster) AddRegionStoreWithLeader(storeID uint64, regionCount int, leaderCounts ...int) {
	leaderCount := regionCount
	if len(leaderCounts) != 0 {
		leaderCount = leaderCounts[0]
	}
	mc.AddRegionStore(storeID, regionCount)
	for i := 0; i < leaderCount; i++ {
		id, _ := mc.AllocID()
		mc.AddLeaderRegion(id, storeID)
	}
}

// AddLabelsStore adds store with specified count of region and labels.
func (mc *Cluster) AddLabelsStore(storeID uint64, regionCount int, labels map[string]string) {
	newLabels := make([]*metapb.StoreLabel, 0, len(labels))
	for k, v := range labels {
		newLabels = append(newLabels, &metapb.StoreLabel{Key: k, Value: v})
	}
	stats := &pdpb.StoreStats{}
	stats.Capacity = defaultStoreCapacity
	stats.Available = stats.Capacity - uint64(regionCount)*defaultRegionSize
	stats.UsedSize = uint64(regionCount) * defaultRegionSize
	store := core.NewStoreInfo(
		&metapb.Store{
			Id:     storeID,
			Labels: newLabels,
		},
		core.SetStoreStats(stats),
		core.SetRegionCount(regionCount),
		core.SetRegionSize(int64(regionCount)*defaultRegionSize/mb),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.SetStoreLimit(storeID, storelimit.AddPeer, 60)
	mc.SetStoreLimit(storeID, storelimit.RemovePeer, 60)
	mc.PutStore(store)
}

// AddLeaderRegion adds region with specified leader and followers.
func (mc *Cluster) AddLeaderRegion(regionID uint64, leaderStoreID uint64, followerStoreIDs ...uint64) *core.RegionInfo {
	origin := mc.newMockRegionInfo(regionID, leaderStoreID, followerStoreIDs...)
	region := origin.Clone(core.SetApproximateSize(defaultRegionSize/mb), core.SetApproximateKeys(10))
	mc.PutRegion(region)
	return region
}

// AddRegionWithLearner adds region with specified leader, followers and learners.
func (mc *Cluster) AddRegionWithLearner(regionID uint64, leaderStoreID uint64, followerStoreIDs, learnerStoreIDs []uint64) *core.RegionInfo {
	origin := mc.MockRegionInfo(regionID, leaderStoreID, followerStoreIDs, learnerStoreIDs, nil)
	region := origin.Clone(core.SetApproximateSize(defaultRegionSize/mb), core.SetApproximateKeys(10))
	mc.PutRegion(region)
	return region
}

// AddLeaderRegionWithRange adds region with specified leader, followers and key range.
func (mc *Cluster) AddLeaderRegionWithRange(regionID uint64, startKey string, endKey string, leaderID uint64, followerIds ...uint64) {
	o := mc.newMockRegionInfo(regionID, leaderID, followerIds...)
	r := o.Clone(
		core.WithStartKey([]byte(startKey)),
		core.WithEndKey([]byte(endKey)),
	)
	mc.PutRegion(r)
}

// AddRegionWithReadInfo adds region with specified leader, followers and read info.
func (mc *Cluster) AddRegionWithReadInfo(
	regionID uint64, leaderID uint64,
	readBytes, readKeys uint64,
	reportInterval uint64,
	followerIds []uint64, filledNums ...int) []*statistics.HotPeerStat {
	r := mc.newMockRegionInfo(regionID, leaderID, followerIds...)
	r = r.Clone(core.SetReadBytes(readBytes))
	r = r.Clone(core.SetReadKeys(readKeys))
	r = r.Clone(core.SetReportInterval(reportInterval))
	filledNum := mc.HotCache.GetFilledPeriod(statistics.ReadFlow)
	if len(filledNums) > 0 {
		filledNum = filledNums[0]
	}

	var items []*statistics.HotPeerStat
	for i := 0; i < filledNum; i++ {
		items = mc.CheckRegionRead(r)
		for _, item := range items {
			mc.HotCache.Update(item)
		}
	}
	mc.PutRegion(r)
	return items
}

// AddRegionWithPeerReadInfo adds region with specified peer read info.
func (mc *Cluster) AddRegionWithPeerReadInfo(regionID, leaderID, targetStoreID, readBytes, readKeys, reportInterval uint64,
	followerIds []uint64, filledNums ...int) []*statistics.HotPeerStat {
	r := mc.newMockRegionInfo(regionID, leaderID, followerIds...)
	r = r.Clone(core.SetReadBytes(readBytes), core.SetReadKeys(readKeys), core.SetReportInterval(reportInterval))
	filledNum := mc.HotCache.GetFilledPeriod(statistics.ReadFlow)
	if len(filledNums) > 0 {
		filledNum = filledNums[0]
	}
	var items []*statistics.HotPeerStat
	for i := 0; i < filledNum; i++ {
		items = mc.CheckRegionRead(r)
		for _, item := range items {
			if item.StoreID == targetStoreID {
				mc.HotCache.Update(item)
			}
		}
	}
	mc.PutRegion(r)
	return items
}

// AddRegionLeaderWithReadInfo add region leader read info
func (mc *Cluster) AddRegionLeaderWithReadInfo(
	regionID uint64, leaderID uint64,
	readBytes, readKeys uint64,
	reportInterval uint64,
	followerIds []uint64, filledNums ...int) []*statistics.HotPeerStat {
	r := mc.newMockRegionInfo(regionID, leaderID, followerIds...)
	r = r.Clone(core.SetReadBytes(readBytes))
	r = r.Clone(core.SetReadKeys(readKeys))
	r = r.Clone(core.SetReportInterval(reportInterval))
	filledNum := mc.HotCache.GetFilledPeriod(statistics.ReadFlow)
	if len(filledNums) > 0 {
		filledNum = filledNums[0]
	}

	var items []*statistics.HotPeerStat
	for i := 0; i < filledNum; i++ {
		items = mc.CheckRegionLeaderRead(r)
		for _, item := range items {
			mc.HotCache.Update(item)
		}
	}
	mc.PutRegion(r)
	return items
}

// AddLeaderRegionWithWriteInfo adds region with specified leader and peers write info.
func (mc *Cluster) AddLeaderRegionWithWriteInfo(
	regionID uint64, leaderID uint64,
	writtenBytes, writtenKeys uint64,
	reportInterval uint64,
	followerIds []uint64, filledNums ...int) []*statistics.HotPeerStat {
	r := mc.newMockRegionInfo(regionID, leaderID, followerIds...)
	r = r.Clone(core.SetWrittenBytes(writtenBytes))
	r = r.Clone(core.SetWrittenKeys(writtenKeys))
	r = r.Clone(core.SetReportInterval(reportInterval))

	filledNum := mc.HotCache.GetFilledPeriod(statistics.WriteFlow)
	if len(filledNums) > 0 {
		filledNum = filledNums[0]
	}

	var items []*statistics.HotPeerStat
	for i := 0; i < filledNum; i++ {
		items = mc.CheckRegionWrite(r)
		for _, item := range items {
			mc.HotCache.Update(item)
		}
	}
	mc.PutRegion(r)
	return items
}

// UpdateStoreLeaderWeight updates store leader weight.
func (mc *Cluster) UpdateStoreLeaderWeight(storeID uint64, weight float64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(core.SetLeaderWeight(weight))
	mc.PutStore(newStore)
}

// SetStoreEvictLeader set store whether evict leader.
func (mc *Cluster) SetStoreEvictLeader(storeID uint64, enableEvictLeader bool) {
	store := mc.GetStore(storeID)
	if enableEvictLeader {
		mc.PutStore(store.Clone(core.PauseLeaderTransfer()))
	} else {
		mc.PutStore(store.Clone(core.ResumeLeaderTransfer()))
	}
}

// UpdateStoreRegionWeight updates store region weight.
func (mc *Cluster) UpdateStoreRegionWeight(storeID uint64, weight float64) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(core.SetRegionWeight(weight))
	mc.PutStore(newStore)
}

// UpdateStoreLeaderSize updates store leader size.
func (mc *Cluster) UpdateStoreLeaderSize(storeID uint64, size int64) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.Available = newStats.Capacity - uint64(store.GetLeaderSize())
	newStore := store.Clone(
		core.SetStoreStats(newStats),
		core.SetLeaderSize(size),
	)
	mc.PutStore(newStore)
}

// UpdateStoreRegionSize updates store region size.
func (mc *Cluster) UpdateStoreRegionSize(storeID uint64, size int64) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.Available = newStats.Capacity - uint64(store.GetRegionSize())
	newStore := store.Clone(
		core.SetStoreStats(newStats),
		core.SetRegionSize(size),
	)
	mc.PutStore(newStore)
}

// UpdateLeaderCount updates store leader count.
func (mc *Cluster) UpdateLeaderCount(storeID uint64, leaderCount int) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.SetLeaderCount(leaderCount),
		core.SetLeaderSize(int64(leaderCount)*defaultRegionSize/mb),
	)
	mc.PutStore(newStore)
}

// UpdateRegionCount updates store region count.
func (mc *Cluster) UpdateRegionCount(storeID uint64, regionCount int) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(
		core.SetRegionCount(regionCount),
		core.SetRegionSize(int64(regionCount)*defaultRegionSize/mb),
	)
	mc.PutStore(newStore)
}

// UpdateSnapshotCount updates store snapshot count.
func (mc *Cluster) UpdateSnapshotCount(storeID uint64, snapshotCount int) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.ReceivingSnapCount = uint32(snapshotCount)
	newStore := store.Clone(core.SetStoreStats(newStats))
	mc.PutStore(newStore)
}

// UpdatePendingPeerCount updates store pending peer count.
func (mc *Cluster) UpdatePendingPeerCount(storeID uint64, pendingPeerCount int) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(core.SetPendingPeerCount(pendingPeerCount))
	mc.PutStore(newStore)
}

// UpdateStorageRatio updates store storage ratio count.
func (mc *Cluster) UpdateStorageRatio(storeID uint64, usedRatio, availableRatio float64) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.Capacity = defaultStoreCapacity
	newStats.UsedSize = uint64(float64(newStats.Capacity) * usedRatio)
	newStats.Available = uint64(float64(newStats.Capacity) * availableRatio)
	newStore := store.Clone(core.SetStoreStats(newStats))
	mc.PutStore(newStore)
}

// UpdateStorageWrittenStats updates store written bytes.
func (mc *Cluster) UpdateStorageWrittenStats(storeID, bytesWritten, keysWritten uint64) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.BytesWritten = bytesWritten
	newStats.KeysWritten = keysWritten
	now := time.Now().Second()
	interval := &pdpb.TimeInterval{StartTimestamp: uint64(now - statistics.StoreHeartBeatReportInterval), EndTimestamp: uint64(now)}
	newStats.Interval = interval
	newStore := store.Clone(core.SetStoreStats(newStats))
	mc.Set(storeID, newStats)
	mc.PutStore(newStore)
}

// UpdateStorageReadStats updates store written bytes.
func (mc *Cluster) UpdateStorageReadStats(storeID, bytesWritten, keysWritten uint64) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.BytesRead = bytesWritten
	newStats.KeysRead = keysWritten
	now := time.Now().Second()
	interval := &pdpb.TimeInterval{StartTimestamp: uint64(now - statistics.StoreHeartBeatReportInterval), EndTimestamp: uint64(now)}
	newStats.Interval = interval
	newStore := store.Clone(core.SetStoreStats(newStats))
	mc.Set(storeID, newStats)
	mc.PutStore(newStore)
}

// UpdateStorageWrittenBytes updates store written bytes.
func (mc *Cluster) UpdateStorageWrittenBytes(storeID uint64, bytesWritten uint64) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.BytesWritten = bytesWritten
	newStats.KeysWritten = bytesWritten / 100
	now := time.Now().Second()
	interval := &pdpb.TimeInterval{StartTimestamp: uint64(now - statistics.StoreHeartBeatReportInterval), EndTimestamp: uint64(now)}
	newStats.Interval = interval
	newStore := store.Clone(core.SetStoreStats(newStats))
	mc.Set(storeID, newStats)
	mc.PutStore(newStore)
}

// UpdateStorageReadBytes updates store read bytes.
func (mc *Cluster) UpdateStorageReadBytes(storeID uint64, bytesRead uint64) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.BytesRead = bytesRead
	newStats.KeysRead = bytesRead / 100
	now := time.Now().Second()
	interval := &pdpb.TimeInterval{StartTimestamp: uint64(now - statistics.StoreHeartBeatReportInterval), EndTimestamp: uint64(now)}
	newStats.Interval = interval
	newStore := store.Clone(core.SetStoreStats(newStats))
	mc.Set(storeID, newStats)
	mc.PutStore(newStore)
}

// UpdateStorageWrittenKeys updates store written keys.
func (mc *Cluster) UpdateStorageWrittenKeys(storeID uint64, keysWritten uint64) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.KeysWritten = keysWritten
	newStats.BytesWritten = keysWritten * 100
	now := time.Now().Second()
	interval := &pdpb.TimeInterval{StartTimestamp: uint64(now - statistics.StoreHeartBeatReportInterval), EndTimestamp: uint64(now)}
	newStats.Interval = interval
	newStore := store.Clone(core.SetStoreStats(newStats))
	mc.Set(storeID, newStats)
	mc.PutStore(newStore)
}

// UpdateStorageReadKeys updates store read bytes.
func (mc *Cluster) UpdateStorageReadKeys(storeID uint64, keysRead uint64) {
	store := mc.GetStore(storeID)
	newStats := proto.Clone(store.GetStoreStats()).(*pdpb.StoreStats)
	newStats.KeysRead = keysRead
	newStats.BytesRead = keysRead * 100
	now := time.Now().Second()
	interval := &pdpb.TimeInterval{StartTimestamp: uint64(now - statistics.StoreHeartBeatReportInterval), EndTimestamp: uint64(now)}
	newStats.Interval = interval
	newStore := store.Clone(core.SetStoreStats(newStats))
	mc.Set(storeID, newStats)
	mc.PutStore(newStore)
}

// UpdateStoreStatus updates store status.
func (mc *Cluster) UpdateStoreStatus(id uint64) {
	leaderCount := mc.Regions.GetStoreLeaderCount(id)
	regionCount := mc.Regions.GetStoreRegionCount(id)
	pendingPeerCount := mc.Regions.GetStorePendingPeerCount(id)
	leaderSize := mc.Regions.GetStoreLeaderRegionSize(id)
	regionSize := mc.Regions.GetStoreRegionSize(id)
	store := mc.Stores.GetStore(id)
	stats := &pdpb.StoreStats{}
	stats.Capacity = defaultStoreCapacity
	stats.Available = stats.Capacity - uint64(store.GetRegionSize()*mb)
	stats.UsedSize = uint64(store.GetRegionSize() * mb)
	newStore := store.Clone(
		core.SetStoreStats(stats),
		core.SetLeaderCount(leaderCount),
		core.SetRegionCount(regionCount),
		core.SetPendingPeerCount(pendingPeerCount),
		core.SetLeaderSize(leaderSize),
		core.SetRegionSize(regionSize),
		core.SetLastHeartbeatTS(time.Now()),
	)
	mc.PutStore(newStore)
}

func (mc *Cluster) newMockRegionInfo(regionID uint64, leaderStoreID uint64, followerStoreIDs ...uint64) *core.RegionInfo {
	return mc.MockRegionInfo(regionID, leaderStoreID, followerStoreIDs, []uint64{}, nil)
}

// CheckLabelProperty checks label property.
func (mc *Cluster) CheckLabelProperty(typ string, labels []*metapb.StoreLabel) bool {
	for _, cfg := range mc.GetLabelPropertyConfig()[typ] {
		for _, l := range labels {
			if l.Key == cfg.Key && l.Value == cfg.Value {
				return true
			}
		}
	}
	return false
}

// PutRegionStores mocks method.
func (mc *Cluster) PutRegionStores(id uint64, stores ...uint64) {
	meta := &metapb.Region{
		Id:       id,
		StartKey: []byte(strconv.FormatUint(id, 10)),
		EndKey:   []byte(strconv.FormatUint(id+1, 10)),
	}
	for _, s := range stores {
		meta.Peers = append(meta.Peers, &metapb.Peer{StoreId: s})
	}
	mc.PutRegion(core.NewRegionInfo(meta, &metapb.Peer{StoreId: stores[0]}))
}

// PutStoreWithLabels mocks method.
func (mc *Cluster) PutStoreWithLabels(id uint64, labelPairs ...string) {
	labels := make(map[string]string)
	for i := 0; i < len(labelPairs); i += 2 {
		labels[labelPairs[i]] = labelPairs[i+1]
	}
	mc.AddLabelsStore(id, 0, labels)
}

// RemoveScheduler mocks method.
func (mc *Cluster) RemoveScheduler(name string) error {
	return nil
}

// MockRegionInfo returns a mock region
// If leaderStoreID is zero, the regions would have no leader
func (mc *Cluster) MockRegionInfo(regionID uint64, leaderStoreID uint64,
	followerStoreIDs, learnerStoreIDs []uint64, epoch *metapb.RegionEpoch) *core.RegionInfo {

	region := &metapb.Region{
		Id:          regionID,
		StartKey:    []byte(fmt.Sprintf("%20d", regionID)),
		EndKey:      []byte(fmt.Sprintf("%20d", regionID+1)),
		RegionEpoch: epoch,
	}
	var leader *metapb.Peer
	if leaderStoreID != 0 {
		leader, _ = mc.AllocPeer(leaderStoreID)
		region.Peers = append(region.Peers, leader)
	}
	for _, storeID := range followerStoreIDs {
		peer, _ := mc.AllocPeer(storeID)
		region.Peers = append(region.Peers, peer)
	}
	for _, storeID := range learnerStoreIDs {
		peer, _ := mc.AllocPeer(storeID)
		peer.Role = metapb.PeerRole_Learner
		region.Peers = append(region.Peers, peer)
	}
	return core.NewRegionInfo(region, leader)
}

// SetStoreLabel set the labels to the target store
func (mc *Cluster) SetStoreLabel(storeID uint64, labels map[string]string) {
	store := mc.GetStore(storeID)
	newLabels := make([]*metapb.StoreLabel, 0, len(labels))
	for k, v := range labels {
		newLabels = append(newLabels, &metapb.StoreLabel{Key: k, Value: v})
	}
	newStore := store.Clone(core.SetStoreLabels(newLabels))
	mc.PutStore(newStore)
}

// DisableFeature marks that these features are not supported in the cluster.
func (mc *Cluster) DisableFeature(fs ...versioninfo.Feature) {
	for _, f := range fs {
		mc.disabledFeatures[f] = struct{}{}
	}
}

// IsFeatureSupported checks if the feature is supported by current cluster.
func (mc *Cluster) IsFeatureSupported(f versioninfo.Feature) bool {
	_, ok := mc.disabledFeatures[f]
	return !ok
}

// AddSuspectRegions mock method
func (mc *Cluster) AddSuspectRegions(ids ...uint64) {
	for _, id := range ids {
		mc.suspectRegions[id] = struct{}{}
	}
}

// CheckRegionUnderSuspect only used for unit test
func (mc *Cluster) CheckRegionUnderSuspect(id uint64) bool {
	_, ok := mc.suspectRegions[id]
	return ok
}

// ResetSuspectRegions only used for unit test
func (mc *Cluster) ResetSuspectRegions() {
	mc.suspectRegions = map[uint64]struct{}{}
}

// GetRegionByKey get region by key
func (mc *Cluster) GetRegionByKey(regionKey []byte) *core.RegionInfo {
	return mc.SearchRegion(regionKey)
}

// SetStoreLastHeartbeatInterval set the last heartbeat to the target store
func (mc *Cluster) SetStoreLastHeartbeatInterval(storeID uint64, interval time.Duration) {
	store := mc.GetStore(storeID)
	newStore := store.Clone(core.SetLastHeartbeatTS(time.Now().Add(-interval)))
	mc.PutStore(newStore)
}

// CheckRegionRead checks region read info with all peers
func (mc *Cluster) CheckRegionRead(region *core.RegionInfo) []*statistics.HotPeerStat {
	items := make([]*statistics.HotPeerStat, 0)
	expiredItems := mc.HotCache.ExpiredReadItems(region)
	items = append(items, expiredItems...)
	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
	for _, peer := range region.GetPeers() {
		peerInfo := core.NewPeerInfo(peer, region.GetLoads(), interval)
		item := mc.HotCache.CheckReadPeerSync(peerInfo, region)
		if item != nil {
			items = append(items, item)
		}
	}
	return items
}

// CheckRegionWrite checks region write info with all peers
func (mc *Cluster) CheckRegionWrite(region *core.RegionInfo) []*statistics.HotPeerStat {
	items := make([]*statistics.HotPeerStat, 0)
	expiredItems := mc.HotCache.ExpiredWriteItems(region)
	items = append(items, expiredItems...)
	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
	for _, peer := range region.GetPeers() {
		peerInfo := core.NewPeerInfo(peer, region.GetLoads(), interval)
		item := mc.HotCache.CheckWritePeerSync(peerInfo, region)
		if item != nil {
			items = append(items, item)
		}
	}
	return items
}

// CheckRegionLeaderRead checks region read info with leader peer
func (mc *Cluster) CheckRegionLeaderRead(region *core.RegionInfo) []*statistics.HotPeerStat {
	items := make([]*statistics.HotPeerStat, 0)
	expiredItems := mc.HotCache.ExpiredReadItems(region)
	items = append(items, expiredItems...)
	reportInterval := region.GetInterval()
	interval := reportInterval.GetEndTimestamp() - reportInterval.GetStartTimestamp()
	peer := region.GetLeader()
	peerInfo := core.NewPeerInfo(peer, region.GetLoads(), interval)
	item := mc.HotCache.CheckReadPeerSync(peerInfo, region)
	if item != nil {
		items = append(items, item)
	}
	return items
}
