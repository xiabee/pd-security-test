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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/statistics"
	"go.uber.org/zap"
)

const (
	// adjustRatio is used to adjust TolerantSizeRatio according to region count.
	adjustRatio                  float64 = 0.005
	leaderTolerantSizeRatio      float64 = 5.0
	minTolerantSizeRatio         float64 = 1.0
	influenceAmp                 int64   = 100
	defaultMinRetryLimit                 = 1
	defaultRetryQuotaAttenuation         = 2
)

type balancePlan struct {
	kind              core.ScheduleKind
	cluster           opt.Cluster
	opInfluence       operator.OpInfluence
	tolerantSizeRatio float64

	source *core.StoreInfo
	target *core.StoreInfo
	region *core.RegionInfo

	sourceScore float64
	targetScore float64
}

func newBalancePlan(kind core.ScheduleKind, cluster opt.Cluster, opInfluence operator.OpInfluence) *balancePlan {
	return &balancePlan{
		kind:              kind,
		cluster:           cluster,
		opInfluence:       opInfluence,
		tolerantSizeRatio: adjustTolerantRatio(cluster, kind),
	}
}

func (p *balancePlan) GetOpInfluence(storeID uint64) int64 {
	return p.opInfluence.GetStoreInfluence(storeID).ResourceProperty(p.kind)
}

func (p *balancePlan) SourceStoreID() uint64 {
	return p.source.GetID()
}

func (p *balancePlan) SourceMetricLabel() string {
	return strconv.FormatUint(p.SourceStoreID(), 10)
}

func (p *balancePlan) TargetStoreID() uint64 {
	return p.target.GetID()
}

func (p *balancePlan) TargetMetricLabel() string {
	return strconv.FormatUint(p.TargetStoreID(), 10)
}

func (p *balancePlan) shouldBalance(scheduleName string) bool {
	// The reason we use max(regionSize, averageRegionSize) to check is:
	// 1. prevent moving small regions between stores with close scores, leading to unnecessary balance.
	// 2. prevent moving huge regions, leading to over balance.
	sourceID := p.source.GetID()
	targetID := p.target.GetID()
	tolerantResource := p.getTolerantResource()
	// to avoid schedule too much, if A's core greater than B and C a little
	// we want that A should be moved out one region not two
	sourceInfluence := p.GetOpInfluence(sourceID)
	// A->B, B's influence is positive , so B can become source schedule, it will move region from B to C
	if sourceInfluence > 0 {
		sourceInfluence = -sourceInfluence
	}
	// to avoid schedule too much, if A's score less than B and C in small range,
	// we want that A can be moved in one region not two
	targetInfluence := p.GetOpInfluence(targetID)
	// to avoid schedule call back
	// A->B, A's influence is negative, so A will be target, C may move region to A
	if targetInfluence < 0 {
		targetInfluence = -targetInfluence
	}
	opts := p.cluster.GetOpts()
	switch p.kind.Resource {
	case core.LeaderKind:
		sourceDelta, targetDelta := sourceInfluence-tolerantResource, targetInfluence+tolerantResource
		p.sourceScore = p.source.LeaderScore(p.kind.Policy, sourceDelta)
		p.targetScore = p.target.LeaderScore(p.kind.Policy, targetDelta)
	case core.RegionKind:
		sourceDelta, targetDelta := sourceInfluence*influenceAmp-tolerantResource, targetInfluence*influenceAmp+tolerantResource
		p.sourceScore = p.source.RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), sourceDelta)
		p.targetScore = p.target.RegionScore(opts.GetRegionScoreFormulaVersion(), opts.GetHighSpaceRatio(), opts.GetLowSpaceRatio(), targetDelta)
	}
	if opts.IsDebugMetricsEnabled() {
		opInfluenceStatus.WithLabelValues(scheduleName, strconv.FormatUint(sourceID, 10), "source").Set(float64(sourceInfluence))
		opInfluenceStatus.WithLabelValues(scheduleName, strconv.FormatUint(targetID, 10), "target").Set(float64(targetInfluence))
		tolerantResourceStatus.WithLabelValues(scheduleName, strconv.FormatUint(sourceID, 10), strconv.FormatUint(targetID, 10)).Set(float64(tolerantResource))
	}
	// Make sure after move, source score is still greater than target score.
	shouldBalance := p.sourceScore > p.targetScore

	if !shouldBalance {
		log.Debug("skip balance "+p.kind.Resource.String(),
			zap.String("scheduler", scheduleName), zap.Uint64("region-id", p.region.GetID()), zap.Uint64("source-store", sourceID), zap.Uint64("target-store", targetID),
			zap.Int64("source-size", p.source.GetRegionSize()), zap.Float64("source-score", p.sourceScore),
			zap.Int64("source-influence", sourceInfluence),
			zap.Int64("target-size", p.target.GetRegionSize()), zap.Float64("target-score", p.targetScore),
			zap.Int64("target-influence", targetInfluence),
			zap.Int64("average-region-size", p.cluster.GetAverageRegionSize()),
			zap.Int64("tolerant-resource", tolerantResource))
	}
	return shouldBalance
}

func (p *balancePlan) getTolerantResource() int64 {
	if p.kind.Resource == core.LeaderKind && p.kind.Policy == core.ByCount {
		return int64(p.tolerantSizeRatio)
	}
	regionSize := p.region.GetApproximateSize()
	if regionSize < p.cluster.GetAverageRegionSize() {
		regionSize = p.cluster.GetAverageRegionSize()
	}
	return int64(float64(regionSize) * p.tolerantSizeRatio)
}

func adjustTolerantRatio(cluster opt.Cluster, kind core.ScheduleKind) float64 {
	var tolerantSizeRatio float64
	switch c := cluster.(type) {
	case *schedule.RangeCluster:
		// range cluster use a separate configuration
		tolerantSizeRatio = c.GetTolerantSizeRatio()
	default:
		tolerantSizeRatio = cluster.GetOpts().GetTolerantSizeRatio()
	}
	if kind.Resource == core.LeaderKind && kind.Policy == core.ByCount {
		if tolerantSizeRatio == 0 {
			return leaderTolerantSizeRatio
		}
		return tolerantSizeRatio
	}

	if tolerantSizeRatio == 0 {
		var maxRegionCount float64
		stores := cluster.GetStores()
		for _, store := range stores {
			regionCount := float64(cluster.GetStoreRegionCount(store.GetID()))
			if maxRegionCount < regionCount {
				maxRegionCount = regionCount
			}
		}
		tolerantSizeRatio = maxRegionCount * adjustRatio
		if tolerantSizeRatio < minTolerantSizeRatio {
			tolerantSizeRatio = minTolerantSizeRatio
		}
	}
	return tolerantSizeRatio
}

func getKeyRanges(args []string) ([]core.KeyRange, error) {
	var ranges []core.KeyRange
	for len(args) > 1 {
		startKey, err := url.QueryUnescape(args[0])
		if err != nil {
			return nil, errs.ErrQueryUnescape.Wrap(err).FastGenWithCause()
		}
		endKey, err := url.QueryUnescape(args[1])
		if err != nil {
			return nil, errs.ErrQueryUnescape.Wrap(err).FastGenWithCause()
		}
		args = args[2:]
		ranges = append(ranges, core.NewKeyRange(startKey, endKey))
	}
	if len(ranges) == 0 {
		return []core.KeyRange{core.NewKeyRange("", "")}, nil
	}
	return ranges, nil
}

type pendingInfluence struct {
	op                *operator.Operator
	from, to          uint64
	origin            statistics.Influence
	maxZombieDuration time.Duration
}

func newPendingInfluence(op *operator.Operator, from, to uint64, infl statistics.Influence, maxZombieDur time.Duration) *pendingInfluence {
	return &pendingInfluence{
		op:                op,
		from:              from,
		to:                to,
		origin:            infl,
		maxZombieDuration: maxZombieDur,
	}
}

func stLdRate(dim int) func(ld *statistics.StoreLoad) float64 {
	return func(ld *statistics.StoreLoad) float64 {
		return ld.Loads[dim]
	}
}

func stLdCount(ld *statistics.StoreLoad) float64 {
	return ld.Count
}

type storeLoadCmp func(ld1, ld2 *statistics.StoreLoad) int

func negLoadCmp(cmp storeLoadCmp) storeLoadCmp {
	return func(ld1, ld2 *statistics.StoreLoad) int {
		return -cmp(ld1, ld2)
	}
}

func sliceLoadCmp(cmps ...storeLoadCmp) storeLoadCmp {
	return func(ld1, ld2 *statistics.StoreLoad) int {
		for _, cmp := range cmps {
			if r := cmp(ld1, ld2); r != 0 {
				return r
			}
		}
		return 0
	}
}

func stLdRankCmp(dim func(ld *statistics.StoreLoad) float64, rank func(value float64) int64) storeLoadCmp {
	return func(ld1, ld2 *statistics.StoreLoad) int {
		return rankCmp(dim(ld1), dim(ld2), rank)
	}
}

func rankCmp(a, b float64, rank func(value float64) int64) int {
	aRk, bRk := rank(a), rank(b)
	if aRk < bRk {
		return -1
	} else if aRk > bRk {
		return 1
	}
	return 0
}

type storeLPCmp func(lp1, lp2 *statistics.StoreLoadPred) int

func sliceLPCmp(cmps ...storeLPCmp) storeLPCmp {
	return func(lp1, lp2 *statistics.StoreLoadPred) int {
		for _, cmp := range cmps {
			if r := cmp(lp1, lp2); r != 0 {
				return r
			}
		}
		return 0
	}
}

func minLPCmp(ldCmp storeLoadCmp) storeLPCmp {
	return func(lp1, lp2 *statistics.StoreLoadPred) int {
		return ldCmp(lp1.Min(), lp2.Min())
	}
}

func maxLPCmp(ldCmp storeLoadCmp) storeLPCmp {
	return func(lp1, lp2 *statistics.StoreLoadPred) int {
		return ldCmp(lp1.Max(), lp2.Max())
	}
}

func diffCmp(ldCmp storeLoadCmp) storeLPCmp {
	return func(lp1, lp2 *statistics.StoreLoadPred) int {
		return ldCmp(lp1.Diff(), lp2.Diff())
	}
}

// storeCollector define the behavior of different engines of stores.
type storeCollector interface {
	// Engine returns the type of Store.
	Engine() string
	// Filter determines whether the Store needs to be handled by itself.
	Filter(info *statistics.StoreSummaryInfo, kind core.ResourceKind) bool
	// GetLoads obtains available loads from storeLoads and peerLoadSum according to rwTy and kind.
	GetLoads(storeLoads, peerLoadSum []float64, rwTy statistics.RWType, kind core.ResourceKind) (loads []float64)
}

type tikvCollector struct{}

func newTikvCollector() storeCollector {
	return tikvCollector{}
}

func (c tikvCollector) Engine() string {
	return core.EngineTiKV
}

func (c tikvCollector) Filter(info *statistics.StoreSummaryInfo, kind core.ResourceKind) bool {
	if info.IsTiFlash {
		return false
	}
	switch kind {
	case core.LeaderKind:
		return info.Store.AllowLeaderTransfer()
	case core.RegionKind:
		return true
	}
	return false
}

func (c tikvCollector) GetLoads(storeLoads, peerLoadSum []float64, rwTy statistics.RWType, kind core.ResourceKind) (loads []float64) {
	loads = make([]float64, statistics.DimLen)
	switch rwTy {
	case statistics.Read:
		loads[statistics.ByteDim] = storeLoads[statistics.StoreReadBytes]
		loads[statistics.KeyDim] = storeLoads[statistics.StoreReadKeys]
		loads[statistics.QueryDim] = storeLoads[statistics.StoreReadQuery]
	case statistics.Write:
		switch kind {
		case core.LeaderKind:
			// Use sum of hot peers to estimate leader-only byte rate.
			// For write requests, Write{Bytes, Keys} is applied to all Peers at the same time,
			// while the Leader and Follower are under different loads (usually the Leader consumes more CPU).
			// Write{QPS} does not require such processing.
			loads[statistics.ByteDim] = peerLoadSum[statistics.ByteDim]
			loads[statistics.KeyDim] = peerLoadSum[statistics.KeyDim]
			loads[statistics.QueryDim] = storeLoads[statistics.StoreWriteQuery]
		case core.RegionKind:
			loads[statistics.ByteDim] = storeLoads[statistics.StoreWriteBytes]
			loads[statistics.KeyDim] = storeLoads[statistics.StoreWriteKeys]
			// The `write-peer` does not have `QueryDim`
		}
	}
	return
}

type tiflashCollector struct {
	isTraceRegionFlow bool
}

func newTiFlashCollector(isTraceRegionFlow bool) storeCollector {
	return tiflashCollector{isTraceRegionFlow: isTraceRegionFlow}
}

func (c tiflashCollector) Engine() string {
	return core.EngineTiFlash
}

func (c tiflashCollector) Filter(info *statistics.StoreSummaryInfo, kind core.ResourceKind) bool {
	switch kind {
	case core.LeaderKind:
		return false
	case core.RegionKind:
		return info.IsTiFlash
	}
	return false
}

func (c tiflashCollector) GetLoads(storeLoads, peerLoadSum []float64, rwTy statistics.RWType, kind core.ResourceKind) (loads []float64) {
	loads = make([]float64, statistics.DimLen)
	switch rwTy {
	case statistics.Read:
		// TODO: Need TiFlash StoreHeartbeat support
	case statistics.Write:
		switch kind {
		case core.LeaderKind:
			// There is no Leader on TiFlash
		case core.RegionKind:
			// TiFlash is currently unable to report statistics in the same unit as Region,
			// so it uses the sum of Regions. If it is not accurate enough, use sum of hot peer.
			if c.isTraceRegionFlow {
				loads[statistics.ByteDim] = storeLoads[statistics.StoreRegionsWriteBytes]
				loads[statistics.KeyDim] = storeLoads[statistics.StoreRegionsWriteKeys]
			} else {
				loads[statistics.ByteDim] = peerLoadSum[statistics.ByteDim]
				loads[statistics.KeyDim] = peerLoadSum[statistics.KeyDim]
			}
			// The `wite-peer` does not have `QueryDim`
		}
	}
	return
}

// summaryStoresLoad Load information of all available stores.
// it will filter the hot peer and calculate the current and future stat(rate,count) for each store
func summaryStoresLoad(
	storeInfos map[uint64]*statistics.StoreSummaryInfo,
	storesLoads map[uint64][]float64,
	storeHotPeers map[uint64][]*statistics.HotPeerStat,
	isTraceRegionFlow bool,
	rwTy statistics.RWType,
	kind core.ResourceKind,
) map[uint64]*statistics.StoreLoadDetail {
	// loadDetail stores the storeID -> hotPeers stat and its current and future stat(rate,count)
	loadDetail := make(map[uint64]*statistics.StoreLoadDetail, len(storesLoads))

	tikvLoadDetail := summaryStoresLoadByEngine(
		storeInfos,
		storesLoads,
		storeHotPeers,
		rwTy, kind,
		newTikvCollector(),
	)
	tiflashLoadDetail := summaryStoresLoadByEngine(
		storeInfos,
		storesLoads,
		storeHotPeers,
		rwTy, kind,
		newTiFlashCollector(isTraceRegionFlow),
	)

	for _, detail := range append(tikvLoadDetail, tiflashLoadDetail...) {
		loadDetail[detail.GetID()] = detail
	}
	return loadDetail
}

func summaryStoresLoadByEngine(
	storeInfos map[uint64]*statistics.StoreSummaryInfo,
	storesLoads map[uint64][]float64,
	storeHotPeers map[uint64][]*statistics.HotPeerStat,
	rwTy statistics.RWType,
	kind core.ResourceKind,
	collector storeCollector,
) []*statistics.StoreLoadDetail {
	loadDetail := make([]*statistics.StoreLoadDetail, 0, len(storeInfos))
	allStoreLoadSum := make([]float64, statistics.DimLen)
	allStoreCount := 0
	allHotPeersCount := 0

	for _, info := range storeInfos {
		store := info.Store
		id := store.GetID()
		storeLoads, ok := storesLoads[id]
		if !ok || !collector.Filter(info, kind) {
			continue
		}

		// Find all hot peers first
		var hotPeers []*statistics.HotPeerStat
		peerLoadSum := make([]float64, statistics.DimLen)
		// TODO: To remove `filterHotPeers`, we need to:
		// HotLeaders consider `Write{Bytes,Keys}`, so when we schedule `writeLeader`, all peers are leader.
		for _, peer := range filterHotPeers(kind, storeHotPeers[id]) {
			for i := range peerLoadSum {
				peerLoadSum[i] += peer.GetLoad(statistics.GetRegionStatKind(rwTy, i))
			}
			hotPeers = append(hotPeers, peer.Clone())
		}
		{
			// Metric for debug.
			ty := "byte-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(peerLoadSum[statistics.ByteDim])
			ty = "key-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(peerLoadSum[statistics.KeyDim])
			ty = "query-rate-" + rwTy.String() + "-" + kind.String()
			hotPeerSummary.WithLabelValues(ty, fmt.Sprintf("%v", id)).Set(peerLoadSum[statistics.QueryDim])
		}

		loads := collector.GetLoads(storeLoads, peerLoadSum, rwTy, kind)
		for i := range allStoreLoadSum {
			allStoreLoadSum[i] += loads[i]
		}
		allStoreCount += 1
		allHotPeersCount += len(hotPeers)

		// Build store load prediction from current load and pending influence.
		stLoadPred := (&statistics.StoreLoad{
			Loads: loads,
			Count: float64(len(hotPeers)),
		}).ToLoadPred(rwTy, info.PendingSum)

		// Construct store load info.
		loadDetail = append(loadDetail, &statistics.StoreLoadDetail{
			Info:     info,
			LoadPred: stLoadPred,
			HotPeers: hotPeers,
		})
	}

	if allStoreCount == 0 {
		return loadDetail
	}

	expectCount := float64(allHotPeersCount) / float64(allStoreCount)
	expectLoads := make([]float64, len(allStoreLoadSum))
	for i := range expectLoads {
		expectLoads[i] = allStoreLoadSum[i] / float64(allStoreCount)
	}
	{
		// Metric for debug.
		engine := collector.Engine()
		ty := "exp-byte-rate-" + rwTy.String() + "-" + kind.String()
		hotPeerSummary.WithLabelValues(ty, engine).Set(expectLoads[statistics.ByteDim])
		ty = "exp-key-rate-" + rwTy.String() + "-" + kind.String()
		hotPeerSummary.WithLabelValues(ty, engine).Set(expectLoads[statistics.KeyDim])
		ty = "exp-query-rate-" + rwTy.String() + "-" + kind.String()
		hotPeerSummary.WithLabelValues(ty, engine).Set(expectLoads[statistics.QueryDim])
		ty = "exp-count-rate-" + rwTy.String() + "-" + kind.String()
		hotPeerSummary.WithLabelValues(ty, engine).Set(expectCount)
	}
	expect := statistics.StoreLoad{
		Loads: expectLoads,
		Count: float64(allHotPeersCount) / float64(allStoreCount),
	}
	for _, detail := range loadDetail {
		detail.LoadPred.Expect = expect
	}
	return loadDetail
}

func filterHotPeers(kind core.ResourceKind, peers []*statistics.HotPeerStat) []*statistics.HotPeerStat {
	ret := make([]*statistics.HotPeerStat, 0, len(peers))
	for _, peer := range peers {
		if kind != core.LeaderKind || peer.IsLeader() {
			ret = append(ret, peer)
		}
	}
	return ret
}

type retryQuota struct {
	initialLimit int
	minLimit     int
	attenuation  int

	limits map[uint64]int
}

func newRetryQuota(initialLimit, minLimit, attenuation int) *retryQuota {
	return &retryQuota{
		initialLimit: initialLimit,
		minLimit:     minLimit,
		attenuation:  attenuation,
		limits:       make(map[uint64]int),
	}
}

func (q *retryQuota) GetLimit(store *core.StoreInfo) int {
	id := store.GetID()
	if limit, ok := q.limits[id]; ok {
		return limit
	}
	q.limits[id] = q.initialLimit
	return q.initialLimit
}

func (q *retryQuota) ResetLimit(store *core.StoreInfo) {
	q.limits[store.GetID()] = q.initialLimit
}

func (q *retryQuota) Attenuate(store *core.StoreInfo) {
	newLimit := q.GetLimit(store) / q.attenuation
	if newLimit < q.minLimit {
		newLimit = q.minLimit
	}
	q.limits[store.GetID()] = newLimit
}

func (q *retryQuota) GC(keepStores []*core.StoreInfo) {
	set := make(map[uint64]struct{}, len(keepStores))
	for _, store := range keepStores {
		set[store.GetID()] = struct{}{}
	}
	for id := range q.limits {
		if _, ok := set[id]; !ok {
			delete(q.limits, id)
		}
	}
}
