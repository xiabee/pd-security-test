// Copyright 2022 TiKV Project Authors.
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

package schedulers

import (
	"context"
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/statistics"
	"github.com/tikv/pd/server/storage"
	"github.com/tikv/pd/server/versioninfo"
)

func TestHotWriteRegionScheduleWithRevertRegionsDimSecond(t *testing.T) {
	// This is a test that searchRevertRegions finds a solution of rank -1.
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	sche, err := schedule.CreateScheduler(statistics.Write.String(), schedule.NewOperatorController(ctx, nil, nil), storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb := sche.(*hotScheduler)
	hb.conf.SetDstToleranceRatio(0.0)
	hb.conf.SetSrcToleranceRatio(0.0)
	hb.conf.SetRankFormulaVersion("v1")
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)
	hb.conf.WritePeerPriorities = []string{statistics.BytePriority, statistics.KeyPriority}

	tc.UpdateStorageWrittenStats(1, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 16*units.MiB*statistics.StoreHeartBeatReportInterval, 20*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 14*units.MiB*statistics.StoreHeartBeatReportInterval, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{6, []uint64{3, 2, 4}, 2 * units.MiB, 3 * units.MiB, 0},
		{7, []uint64{1, 4, 5}, 2 * units.MiB, 0.1 * units.MiB, 0},
	})
	// No operators can be generated when RankFormulaVersion == "v1".
	ops, _ := hb.Schedule(tc, false)
	re.Empty(ops)
	re.False(hb.searchRevertRegions[writePeer])

	hb.conf.SetRankFormulaVersion("v2")
	// searchRevertRegions becomes true after the first `Schedule`.
	ops, _ = hb.Schedule(tc, false)
	re.Empty(ops)
	re.True(hb.searchRevertRegions[writePeer])
	// Two operators can be generated when RankFormulaVersion == "v2".
	ops, _ = hb.Schedule(tc, false)
	/* The revert region is currently disabled for the -1 case.
	re.Len(ops, 2)
	testutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	testutil.CheckTransferPeer(re, ops[1], operator.OpHotRegion, 5, 2)
	*/
	re.Empty(ops)
	re.True(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)
	// When there is a better solution, there will only be one operator.
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{8, []uint64{3, 2, 4}, 0.5 * units.MiB, 3 * units.MiB, 0},
	})
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	testutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	re.False(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)
}

func TestHotWriteRegionScheduleWithRevertRegionsDimFirst(t *testing.T) {
	// This is a test that searchRevertRegions finds a solution of rank -3.
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	sche, err := schedule.CreateScheduler(statistics.Write.String(), schedule.NewOperatorController(ctx, nil, nil), storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb := sche.(*hotScheduler)
	hb.conf.SetDstToleranceRatio(0.0)
	hb.conf.SetSrcToleranceRatio(0.0)
	hb.conf.SetRankFormulaVersion("v1")
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)
	hb.conf.WritePeerPriorities = []string{statistics.BytePriority, statistics.KeyPriority}

	tc.UpdateStorageWrittenStats(1, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 20*units.MiB*statistics.StoreHeartBeatReportInterval, 14*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 10*units.MiB*statistics.StoreHeartBeatReportInterval, 16*units.MiB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{6, []uint64{3, 2, 4}, 3 * units.MiB, 1.8 * units.MiB, 0},
		{7, []uint64{1, 4, 5}, 0.1 * units.MiB, 2 * units.MiB, 0},
	})
	// One operator can be generated when RankFormulaVersion == "v1".
	ops, _ := hb.Schedule(tc, false)
	re.Len(ops, 1)
	testutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	re.False(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)

	hb.conf.SetRankFormulaVersion("v2")
	// searchRevertRegions becomes true after the first `Schedule`.
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	re.True(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)
	// Two operators can be generated when RankFormulaVersion == "v2".
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 2)
	testutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	testutil.CheckTransferPeer(re, ops[1], operator.OpHotRegion, 5, 2)
	re.True(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)
}

func TestHotWriteRegionScheduleWithRevertRegionsDimFirstOnly(t *testing.T) {
	// This is a test that searchRevertRegions finds a solution of rank -2.
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	sche, err := schedule.CreateScheduler(statistics.Write.String(), schedule.NewOperatorController(ctx, nil, nil), storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb := sche.(*hotScheduler)
	hb.conf.SetDstToleranceRatio(0.0)
	hb.conf.SetSrcToleranceRatio(0.0)
	hb.conf.SetRankFormulaVersion("v1")
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)
	hb.conf.WritePeerPriorities = []string{statistics.BytePriority, statistics.KeyPriority}

	tc.UpdateStorageWrittenStats(1, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 20*units.MiB*statistics.StoreHeartBeatReportInterval, 14*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 16*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 10*units.MiB*statistics.StoreHeartBeatReportInterval, 18*units.MiB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{6, []uint64{3, 2, 4}, 3 * units.MiB, 3 * units.MiB, 0},
		{7, []uint64{1, 4, 5}, 0.1 * units.MiB, 0.1 * units.MiB, 0},
	})
	// One operator can be generated when RankFormulaVersion == "v1".
	ops, _ := hb.Schedule(tc, false)
	re.Len(ops, 1)
	testutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	re.False(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)

	hb.conf.SetRankFormulaVersion("v2")
	// searchRevertRegions becomes true after the first `Schedule`.
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	re.True(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)
	// There is still the solution with one operator after that.
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	testutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	re.True(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)
	// Two operators can be generated when there is a better solution
	addRegionInfo(tc, statistics.Write, []testRegionInfo{
		{8, []uint64{1, 4, 5}, 0.1 * units.MiB, 3 * units.MiB, 0},
	})
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 2)
	testutil.CheckTransferPeer(re, ops[0], operator.OpHotRegion, 2, 5)
	testutil.CheckTransferPeer(re, ops[1], operator.OpHotRegion, 5, 2)
	re.True(hb.searchRevertRegions[writePeer])
	clearPendingInfluence(hb)
}

func TestHotReadRegionScheduleWithRevertRegionsDimSecond(t *testing.T) {
	// This is a test that searchRevertRegions finds a solution of rank -1.
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	sche, err := schedule.CreateScheduler(statistics.Read.String(), schedule.NewOperatorController(ctx, nil, nil), storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb := sche.(*hotScheduler)
	hb.conf.SetDstToleranceRatio(0.0)
	hb.conf.SetSrcToleranceRatio(0.0)
	hb.conf.SetRankFormulaVersion("v1")
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetClusterVersion(versioninfo.MinSupportedVersion(versioninfo.Version4_0))
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)
	hb.conf.ReadPriorities = []string{statistics.BytePriority, statistics.KeyPriority}

	tc.UpdateStorageReadStats(1, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 16*units.MiB*statistics.StoreHeartBeatReportInterval, 20*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(4, 15*units.MiB*statistics.StoreHeartBeatReportInterval, 15*units.MiB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(5, 14*units.MiB*statistics.StoreHeartBeatReportInterval, 10*units.MiB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{6, []uint64{2, 1, 5}, 2 * units.MiB, 3 * units.MiB, 0},
		{7, []uint64{5, 4, 2}, 2 * units.MiB, 0.1 * units.MiB, 0},
	})
	// No operators can be generated when RankFormulaVersion == "v1".
	ops, _ := hb.Schedule(tc, false)
	re.Empty(ops)
	re.False(hb.searchRevertRegions[readLeader])

	hb.conf.SetRankFormulaVersion("v2")
	// searchRevertRegions becomes true after the first `Schedule`.
	ops, _ = hb.Schedule(tc, false)
	re.Empty(ops)
	re.True(hb.searchRevertRegions[readLeader])
	// Two operators can be generated when RankFormulaVersion == "v2".
	ops, _ = hb.Schedule(tc, false)
	/* The revert region is currently disabled for the -1 case.
	re.Len(ops, 2)
	testutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 2, 5)
	testutil.CheckTransferLeader(re, ops[1], operator.OpHotRegion, 5, 2)
	*/
	re.Empty(ops)
	re.True(hb.searchRevertRegions[readLeader])
	clearPendingInfluence(hb)
	// When there is a better solution, there will only be one operator.
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{8, []uint64{2, 1, 5}, 0.5 * units.MiB, 3 * units.MiB, 0},
	})
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	testutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 2, 5)
	re.False(hb.searchRevertRegions[readLeader])
	clearPendingInfluence(hb)
}

func TestSkipUniformStore(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(statistics.Read.String(), schedule.NewOperatorController(ctx, nil, nil), storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetRankFormulaVersion("v2")
	hb.(*hotScheduler).conf.ReadPriorities = []string{statistics.BytePriority, statistics.KeyPriority}
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 20)
	tc.AddRegionStore(2, 20)
	tc.AddRegionStore(3, 20)
	tc.AddRegionStore(4, 20)
	tc.AddRegionStore(5, 20)

	// Case1: two dim are both enough uniform
	tc.UpdateStorageReadStats(1, 10.05*units.MB*statistics.StoreHeartBeatReportInterval, 10.05*units.MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 9.15*units.MB*statistics.StoreHeartBeatReportInterval, 9.15*units.MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 10.0*units.MB*statistics.StoreHeartBeatReportInterval, 10.0*units.MB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.3 * units.MB, 0.3 * units.MB, 0},
	})
	// when there is no uniform store filter, still schedule although the cluster is enough uniform
	stddevThreshold = 0.0
	ops, _ := hb.Schedule(tc, false)
	re.Len(ops, 1)
	testutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 1, 2)
	clearPendingInfluence(hb.(*hotScheduler))
	// when there is uniform store filter, not schedule
	stddevThreshold = 0.1
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 0)
	clearPendingInfluence(hb.(*hotScheduler))

	// Case2: the first dim is enough uniform, we should schedule the second dim
	tc.UpdateStorageReadStats(1, 10.15*units.MB*statistics.StoreHeartBeatReportInterval, 10.05*units.MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 9.25*units.MB*statistics.StoreHeartBeatReportInterval, 9.85*units.MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 9.85*units.MB*statistics.StoreHeartBeatReportInterval, 16.0*units.MB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.3 * units.MB, 0.3 * units.MB, 0},
		{2, []uint64{3, 2, 1}, 0.3 * units.MB, 2 * units.MB, 0},
	})
	// when there is no uniform store filter, still schedule although the first dim is enough uniform
	stddevThreshold = 0.0
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	testutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 1, 2)
	clearPendingInfluence(hb.(*hotScheduler))
	// when there is uniform store filter, schedule the second dim, which is no uniform
	stddevThreshold = 0.1
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	testutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 3, 2)
	clearPendingInfluence(hb.(*hotScheduler))

	// Case3: the second dim is enough uniform, we should schedule the first dim, although its rank is higher than the second dim
	tc.UpdateStorageReadStats(1, 10.05*units.MB*statistics.StoreHeartBeatReportInterval, 10.05*units.MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(2, 9.85*units.MB*statistics.StoreHeartBeatReportInterval, 9.45*units.MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(3, 16*units.MB*statistics.StoreHeartBeatReportInterval, 9.85*units.MB*statistics.StoreHeartBeatReportInterval)
	addRegionInfo(tc, statistics.Read, []testRegionInfo{
		{1, []uint64{1, 2, 3}, 0.3 * units.MB, 0.3 * units.MB, 0},
		{2, []uint64{3, 2, 1}, 2 * units.MB, 0.3 * units.MB, 0},
	})
	// when there is no uniform store filter, schedule the first dim, which is no uniform
	stddevThreshold = 0.0
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	testutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 3, 2)
	clearPendingInfluence(hb.(*hotScheduler))
	// when there is uniform store filter, schedule the first dim, which is no uniform
	stddevThreshold = 0.1
	ops, _ = hb.Schedule(tc, false)
	re.Len(ops, 1)
	testutil.CheckTransferLeader(re, ops[0], operator.OpHotRegion, 3, 2)
	clearPendingInfluence(hb.(*hotScheduler))
}

func TestHotReadRegionScheduleWithSmallHotRegion(t *testing.T) {
	// This is a test that we can schedule small hot region,
	// which is smaller than 20% of diff or 2% of low node. (#6645)
	// 20% is from `firstPriorityPerceivedRatio`, 2% is from `firstPriorityMinHotRatio`.
	// The byte of high node is 2000MB/s, the low node is 200MB/s.
	// The query of high node is 2000qps, the low node is 200qps.
	// There are all small hot regions in the cluster, which are smaller than 20% of diff or 2% of low node.
	re := require.New(t)
	emptyFunc := func(*mockcluster.Cluster, *hotScheduler) {}
	highLoad, lowLoad := uint64(2000), uint64(200)
	bigHotRegionByte := uint64(float64(lowLoad) * firstPriorityMinHotRatio * 10 * units.MiB * statistics.ReadReportInterval)
	bigHotRegionQuery := uint64(float64(lowLoad) * firstPriorityMinHotRatio * 10 * statistics.ReadReportInterval)

	// Case1: Before #6827, we only use minHotRatio, so cannot schedule small hot region in this case.
	// Because 10000 is larger than the length of hotRegions, so `filterHotPeers` will skip the topn calculation.
	origin := topnPosition
	topnPosition = 10000
	ops := checkHotReadRegionScheduleWithSmallHotRegion(re, highLoad, lowLoad, emptyFunc)
	re.Empty(ops)
	topnPosition = origin

	// Case2: After #6827, we use top10 as the threshold of minHotPeer.
	ops = checkHotReadRegionScheduleWithSmallHotRegion(re, highLoad, lowLoad, emptyFunc)
	re.Len(ops, 1)
	ops = checkHotReadRegionScheduleWithSmallHotRegion(re, lowLoad, highLoad, emptyFunc)
	re.Len(ops, 0)

	// Case3: If there is larger hot region, we will schedule it.
	hotRegionID := uint64(100)
	ops = checkHotReadRegionScheduleWithSmallHotRegion(re, highLoad, lowLoad, func(tc *mockcluster.Cluster, _ *hotScheduler) {
		tc.AddRegionWithReadInfo(hotRegionID, 1, bigHotRegionByte, 0, bigHotRegionQuery, statistics.ReadReportInterval, []uint64{2, 3})
	})
	re.Len(ops, 1)
	re.Equal(hotRegionID, ops[0].RegionID())

	// Case4: If there is larger hot region, but it need to cool down, we will schedule small hot region.
	ops = checkHotReadRegionScheduleWithSmallHotRegion(re, highLoad, lowLoad, func(tc *mockcluster.Cluster, _ *hotScheduler) {
		// just transfer leader
		tc.AddRegionWithReadInfo(hotRegionID, 2, bigHotRegionByte, 0, bigHotRegionQuery, statistics.ReadReportInterval, []uint64{1, 3})
		tc.AddRegionWithReadInfo(hotRegionID, 1, bigHotRegionByte, 0, bigHotRegionQuery, statistics.ReadReportInterval, []uint64{2, 3})
	})
	re.Len(ops, 1)
	re.NotEqual(hotRegionID, ops[0].RegionID())

	// Case5: If there is larger hot region, but it is pending, we will schedule small hot region.
	ops = checkHotReadRegionScheduleWithSmallHotRegion(re, highLoad, lowLoad, func(tc *mockcluster.Cluster, hb *hotScheduler) {
		tc.AddRegionWithReadInfo(hotRegionID, 1, bigHotRegionByte, 0, bigHotRegionQuery, statistics.ReadReportInterval, []uint64{2, 3})
		hb.regionPendings[hotRegionID] = &pendingInfluence{}
	})
	re.Len(ops, 1)
	re.NotEqual(hotRegionID, ops[0].RegionID())

	// Case5: If there are more than topnPosition hot regions, but them need to cool down,
	// we will schedule large hot region rather than small hot region, so there is no operator.
	topnPosition = 2
	ops = checkHotReadRegionScheduleWithSmallHotRegion(re, highLoad, lowLoad, func(tc *mockcluster.Cluster, _ *hotScheduler) {
		// just transfer leader
		tc.AddRegionWithReadInfo(hotRegionID, 2, bigHotRegionByte, 0, bigHotRegionQuery, statistics.ReadReportInterval, []uint64{1, 3})
		tc.AddRegionWithReadInfo(hotRegionID, 1, bigHotRegionByte, 0, bigHotRegionQuery, statistics.ReadReportInterval, []uint64{2, 3})
		// just transfer leader
		tc.AddRegionWithReadInfo(hotRegionID+1, 2, bigHotRegionByte, 0, bigHotRegionQuery, statistics.ReadReportInterval, []uint64{1, 3})
		tc.AddRegionWithReadInfo(hotRegionID+1, 1, bigHotRegionByte, 0, bigHotRegionQuery, statistics.ReadReportInterval, []uint64{2, 3})
	})
	re.Len(ops, 0)
	topnPosition = origin

	// Case6: If there are more than topnPosition hot regions, but them are pending,
	// we will schedule large hot region rather than small hot region, so there is no operator.
	topnPosition = 2
	ops = checkHotReadRegionScheduleWithSmallHotRegion(re, highLoad, lowLoad, func(tc *mockcluster.Cluster, hb *hotScheduler) {
		tc.AddRegionWithReadInfo(hotRegionID, 1, bigHotRegionByte, 0, bigHotRegionQuery, statistics.ReadReportInterval, []uint64{2, 3})
		hb.regionPendings[hotRegionID] = &pendingInfluence{}
		tc.AddRegionWithReadInfo(hotRegionID+1, 1, bigHotRegionByte, 0, bigHotRegionQuery, statistics.ReadReportInterval, []uint64{2, 3})
		hb.regionPendings[hotRegionID+1] = &pendingInfluence{}
	})
	re.Len(ops, 0)
	topnPosition = origin
}

func checkHotReadRegionScheduleWithSmallHotRegion(re *require.Assertions, highLoad, lowLoad uint64,
	addOtherRegions func(*mockcluster.Cluster, *hotScheduler)) []*operator.Operator {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	hb, err := schedule.CreateScheduler(statistics.Read.String(), schedule.NewOperatorController(ctx, nil, nil), storage.NewStorageWithMemoryBackend(), nil)
	re.NoError(err)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetRankFormulaVersion("v2")
	hb.(*hotScheduler).conf.ReadPriorities = []string{statistics.QueryPriority, statistics.BytePriority}
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(ctx, opt)
	tc.SetHotRegionCacheHitsThreshold(0)
	tc.AddRegionStore(1, 40)
	tc.AddRegionStore(2, 10)
	tc.AddRegionStore(3, 10)

	tc.UpdateStorageReadQuery(1, highLoad*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadQuery(2, lowLoad*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadQuery(3, (highLoad+lowLoad)/2*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadStats(1, highLoad*units.MiB*statistics.StoreHeartBeatReportInterval, 0)
	tc.UpdateStorageReadStats(2, lowLoad*units.MiB*statistics.StoreHeartBeatReportInterval, 0)
	tc.UpdateStorageReadStats(3, (highLoad+lowLoad)/2*units.MiB*statistics.StoreHeartBeatReportInterval, 0)

	smallHotPeerQuery := float64(lowLoad) * firstPriorityMinHotRatio * 0.9             // it's a small hot region than the firstPriorityMinHotRatio
	smallHotPeerByte := float64(lowLoad) * secondPriorityMinHotRatio * 0.9 * units.MiB // it's a small hot region than the secondPriorityMinHotRatio
	regions := make([]testRegionInfo, 0)
	for i := 10; i < 50; i++ {
		regions = append(regions, testRegionInfo{uint64(i), []uint64{1, 2, 3}, smallHotPeerByte, 0, smallHotPeerQuery})
		if i < 20 {
			regions = append(regions, testRegionInfo{uint64(i), []uint64{2, 1, 3}, smallHotPeerByte, 0, smallHotPeerQuery})
			regions = append(regions, testRegionInfo{uint64(i), []uint64{3, 1, 2}, smallHotPeerByte, 0, smallHotPeerQuery})
		}
	}
	addRegionInfo(tc, statistics.Read, regions)
	tc.SetHotRegionCacheHitsThreshold(1)
	addOtherRegions(tc, hb.(*hotScheduler))
	ops, _ := hb.Schedule(tc, false)
	return ops
}
