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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checker

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/cache"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedule/filter"
	"github.com/tikv/pd/server/schedule/operator"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/versioninfo"
	"go.uber.org/zap"
)

var (
	errNoStoreToAdd        = errors.New("no store to add peer")
	errNoStoreToReplace    = errors.New("no store to replace peer")
	errPeerCannotBeLeader  = errors.New("peer cannot be leader")
	errPeerCannotBeWitness = errors.New("peer cannot be witness")
	errNoNewLeader         = errors.New("no new leader")
	errRegionNoLeader      = errors.New("region no leader")
)

const maxPendingListLen = 100000

// RuleChecker fix/improve region by placement rules.
type RuleChecker struct {
	PauseController
	cluster            schedule.Cluster
	ruleManager        *placement.RuleManager
	name               string
	regionWaitingList  cache.Cache
	pendingList        cache.Cache
	switchWitnessCache *cache.TTLUint64
	record             *recorder
}

// NewRuleChecker creates a checker instance.
func NewRuleChecker(ctx context.Context, cluster schedule.Cluster, ruleManager *placement.RuleManager, regionWaitingList cache.Cache) *RuleChecker {
	return &RuleChecker{
		cluster:            cluster,
		ruleManager:        ruleManager,
		name:               "rule-checker",
		regionWaitingList:  regionWaitingList,
		pendingList:        cache.NewDefaultCache(maxPendingListLen),
		switchWitnessCache: cache.NewIDTTL(ctx, time.Minute, cluster.GetOpts().GetSwitchWitnessInterval()),
		record:             newRecord(),
	}
}

// GetType returns RuleChecker's Type
func (c *RuleChecker) GetType() string {
	return "rule-checker"
}

// Check checks if the region matches placement rules and returns Operator to
// fix it.
func (c *RuleChecker) Check(region *core.RegionInfo) *operator.Operator {
	fit := c.cluster.GetRuleManager().FitRegion(c.cluster, region)
	return c.CheckWithFit(region, fit)
}

// CheckWithFit is similar with Checker with placement.RegionFit
func (c *RuleChecker) CheckWithFit(region *core.RegionInfo, fit *placement.RegionFit) (op *operator.Operator) {
	// checker is paused
	if c.IsPaused() {
		checkerCounter.WithLabelValues("rule_checker", "paused").Inc()
		return nil
	}
	// skip no leader region
	if region.GetLeader() == nil {
		checkerCounter.WithLabelValues("rule_checker", "region-no-leader").Inc()
		log.Debug("fail to check region", zap.Uint64("region-id", region.GetID()), zap.Error(errRegionNoLeader))
		return
	}
	// If the fit is fetched from cache, it seems that the region doesn't need cache
	if c.cluster.GetOpts().IsPlacementRulesCacheEnabled() && fit.IsCached() {
		failpoint.Inject("assertShouldNotCache", func() {
			panic("cached shouldn't be used")
		})
		checkerCounter.WithLabelValues("rule_checker", "get-cache").Inc()
		return nil
	}
	failpoint.Inject("assertShouldCache", func() {
		panic("cached should be used")
	})

	// the placement rule is disabled
	if fit == nil {
		return
	}

	// If the fit is calculated by FitRegion, which means we get a new fit result, thus we should
	// invalid the cache if it exists
	c.ruleManager.InvalidCache(region.GetID())

	checkerCounter.WithLabelValues("rule_checker", "check").Inc()
	c.record.refresh(c.cluster)

	if len(fit.RuleFits) == 0 {
		checkerCounter.WithLabelValues("rule_checker", "need-split").Inc()
		// If the region matches no rules, the most possible reason is it spans across
		// multiple rules.
		return nil
	}
	op, err := c.fixOrphanPeers(region, fit)
	if err != nil {
		log.Debug("fail to fix orphan peer", errs.ZapError(err))
	} else if op != nil {
		c.pendingList.Remove(region.GetID())
		return op
	}
	for _, rf := range fit.RuleFits {
		op, err := c.fixRulePeer(region, fit, rf)
		if err != nil {
			log.Debug("fail to fix rule peer", zap.String("rule-group", rf.Rule.GroupID), zap.String("rule-id", rf.Rule.ID), errs.ZapError(err))
			continue
		}
		if op != nil {
			c.pendingList.Remove(region.GetID())
			return op
		}
	}
	if c.cluster.GetOpts().IsPlacementRulesCacheEnabled() {
		if placement.ValidateFit(fit) && placement.ValidateRegion(region) && placement.ValidateStores(fit.GetRegionStores()) {
			// If there is no need to fix, we will cache the fit
			c.ruleManager.SetRegionFitCache(region, fit)
			checkerCounter.WithLabelValues("rule_checker", "set-cache").Inc()
		}
	}
	return nil
}

// RecordRegionPromoteToNonWitness put the recently switch non-witness region into cache. RuleChecker
// will skip switch it back to witness for a while.
func (c *RuleChecker) RecordRegionPromoteToNonWitness(regionID uint64) {
	c.switchWitnessCache.PutWithTTL(regionID, nil, c.cluster.GetOpts().GetSwitchWitnessInterval())
}

func (c *RuleChecker) isWitnessEnabled() bool {
	return versioninfo.IsFeatureSupported(c.cluster.GetOpts().GetClusterVersion(), versioninfo.SwitchWitness) &&
		c.cluster.GetOpts().IsWitnessAllowed()
}

func (c *RuleChecker) fixRulePeer(region *core.RegionInfo, fit *placement.RegionFit, rf *placement.RuleFit) (*operator.Operator, error) {
	// make up peers.
	if len(rf.Peers) < rf.Rule.Count {
		return c.addRulePeer(region, rf)
	}
	// fix down/offline peers.
	for _, peer := range rf.Peers {
		if c.isDownPeer(region, peer) {
			if c.isStoreDownTimeHitMaxDownTime(peer.GetStoreId()) {
				checkerCounter.WithLabelValues("rule_checker", "replace-down").Inc()
				return c.replaceUnexpectRulePeer(region, rf, fit, peer, downStatus)
			}
			// When witness placement rule is enabled, promotes the witness to voter when region has down peer.
			if c.isWitnessEnabled() {
				if witness, ok := c.hasAvailableWitness(region, peer); ok {
					checkerCounter.WithLabelValues("rule_checker", "promote-witness").Inc()
					return operator.CreateNonWitnessPeerOperator("promote-witness", c.cluster, region, witness)
				}
			}
		}
		if c.isOfflinePeer(peer) {
			checkerCounter.WithLabelValues("rule_checker", "replace-offline").Inc()
			return c.replaceUnexpectRulePeer(region, rf, fit, peer, offlineStatus)
		}
	}
	// fix loose matched peers.
	for _, peer := range rf.PeersWithDifferentRole {
		op, err := c.fixLooseMatchPeer(region, fit, rf, peer)
		if err != nil {
			return nil, err
		}
		if op != nil {
			return op, nil
		}
	}
	return c.fixBetterLocation(region, rf)
}

func (c *RuleChecker) addRulePeer(region *core.RegionInfo, rf *placement.RuleFit) (*operator.Operator, error) {
	checkerCounter.WithLabelValues("rule_checker", "add-rule-peer").Inc()
	ruleStores := c.getRuleFitStores(rf)
	store, filterByTempState := c.strategy(region, rf.Rule).SelectStoreToAdd(ruleStores)
	if store == 0 {
		checkerCounter.WithLabelValues("rule_checker", "no-store-add").Inc()
		c.handleFilterState(region, filterByTempState)
		return nil, errNoStoreToAdd
	}
	isWitness := rf.Rule.IsWitness
	if !c.isWitnessEnabled() {
		isWitness = false
	}
	peer := &metapb.Peer{StoreId: store, Role: rf.Rule.Role.MetaPeerRole(), IsWitness: isWitness}
	op, err := operator.CreateAddPeerOperator("add-rule-peer", c.cluster, region, peer, operator.OpReplica)
	if err != nil {
		return nil, err
	}
	op.SetPriorityLevel(core.High)
	return op, nil
}

// The peer's store may in Offline or Down, need to be replace.
func (c *RuleChecker) replaceUnexpectRulePeer(region *core.RegionInfo, rf *placement.RuleFit, fit *placement.RegionFit, peer *metapb.Peer, status string) (*operator.Operator, error) {
	ruleStores := c.getRuleFitStores(rf)
	store, filterByTempState := c.strategy(region, rf.Rule).SelectStoreToFix(ruleStores, peer.GetStoreId())
	if store == 0 {
		checkerCounter.WithLabelValues("rule_checker", "no-store-replace").Inc()
		c.handleFilterState(region, filterByTempState)
		return nil, errNoStoreToReplace
	}
	var isWitness bool
	if c.isWitnessEnabled() {
		// No matter whether witness placement rule is enabled or disabled, when peer's downtime
		// exceeds the threshold(30min), add a witness and remove the down peer. Then witness is
		// promoted to non-witness gradually to improve availability.
		if status == "down" {
			isWitness = true
		} else {
			isWitness = rf.Rule.IsWitness
		}
	} else {
		isWitness = false
	}
	newPeer := &metapb.Peer{StoreId: store, Role: rf.Rule.Role.MetaPeerRole(), IsWitness: isWitness}
	//  pick the smallest leader store to avoid the Offline store be snapshot generator bottleneck.
	var newLeader *metapb.Peer
	if region.GetLeader().GetId() == peer.GetId() {
		minCount := uint64(math.MaxUint64)
		for _, p := range region.GetPeers() {
			count := c.record.getOfflineLeaderCount(p.GetStoreId())
			checkPeerhealth := func() bool {
				if p.GetId() == peer.GetId() {
					return true
				}
				if region.GetDownPeer(p.GetId()) != nil || region.GetPendingPeer(p.GetId()) != nil {
					return false
				}
				return c.allowLeader(fit, p)
			}
			if minCount > count && checkPeerhealth() {
				minCount = count
				newLeader = p
			}
		}
	}

	createOp := func() (*operator.Operator, error) {
		if newLeader != nil && newLeader.GetId() != peer.GetId() {
			return operator.CreateReplaceLeaderPeerOperator("replace-rule-"+status+"-leader-peer", c.cluster, region, operator.OpReplica, peer.StoreId, newPeer, newLeader)
		}
		return operator.CreateMovePeerOperator("replace-rule-"+status+"-peer", c.cluster, region, operator.OpReplica, peer.StoreId, newPeer)
	}
	op, err := createOp()
	if err != nil {
		return nil, err
	}
	if newLeader != nil {
		c.record.incOfflineLeaderCount(newLeader.GetStoreId())
	}
	op.SetPriorityLevel(core.High)
	return op, nil
}

func (c *RuleChecker) fixLooseMatchPeer(region *core.RegionInfo, fit *placement.RegionFit, rf *placement.RuleFit, peer *metapb.Peer) (*operator.Operator, error) {
	if core.IsLearner(peer) && rf.Rule.Role != placement.Learner {
		checkerCounter.WithLabelValues("rule_checker", "fix-peer-role").Inc()
		return operator.CreatePromoteLearnerOperator("fix-peer-role", c.cluster, region, peer)
	}
	if region.GetLeader().GetId() != peer.GetId() && rf.Rule.Role == placement.Leader {
		checkerCounter.WithLabelValues("rule_checker", "fix-leader-role").Inc()
		if c.allowLeader(fit, peer) {
			return operator.CreateTransferLeaderOperator("fix-leader-role", c.cluster, region, region.GetLeader().GetStoreId(), peer.GetStoreId(), []uint64{}, 0)
		}
		checkerCounter.WithLabelValues("rule_checker", "not-allow-leader")
		return nil, errPeerCannotBeLeader
	}
	if region.GetLeader().GetId() == peer.GetId() && rf.Rule.Role == placement.Follower {
		checkerCounter.WithLabelValues("rule_checker", "fix-follower-role").Inc()
		for _, p := range region.GetPeers() {
			if c.allowLeader(fit, p) {
				return operator.CreateTransferLeaderOperator("fix-follower-role", c.cluster, region, peer.GetStoreId(), p.GetStoreId(), []uint64{}, 0)
			}
		}
		checkerCounter.WithLabelValues("rule_checker", "no-new-leader").Inc()
		return nil, errNoNewLeader
	}
	if core.IsVoter(peer) && rf.Rule.Role == placement.Learner {
		checkerCounter.WithLabelValues("rule_checker", "demote-voter-role").Inc()
		return operator.CreateDemoteVoterOperator("fix-demote-voter", c.cluster, region, peer)
	}
	if region.GetLeader().GetId() == peer.GetId() && rf.Rule.IsWitness {
		return nil, errPeerCannotBeWitness
	}
	if !core.IsWitness(peer) && rf.Rule.IsWitness && c.isWitnessEnabled() {
		c.switchWitnessCache.UpdateTTL(c.cluster.GetOpts().GetSwitchWitnessInterval())
		if c.switchWitnessCache.Exists(region.GetID()) {
			checkerCounter.WithLabelValues("rule_checker", "recently-promote-to-non-witness").Inc()
			return nil, nil
		}
		if len(region.GetPendingPeers()) > 0 {
			checkerCounter.WithLabelValues("rule_checker", "cancel-switch-to-witness").Inc()
			return nil, nil
		}
		lv := "set-voter-witness"
		if core.IsLearner(peer) {
			lv = "set-learner-witness"
		}
		checkerCounter.WithLabelValues("rule_checker", lv).Inc()
		return operator.CreateWitnessPeerOperator("fix-witness-peer", c.cluster, region, peer)
	} else if core.IsWitness(peer) && (!rf.Rule.IsWitness || !c.isWitnessEnabled()) {
		lv := "set-voter-non-witness"
		if core.IsLearner(peer) {
			lv = "set-learner-non-witness"
		}
		checkerCounter.WithLabelValues("rule_checker", lv).Inc()
		return operator.CreateNonWitnessPeerOperator("fix-non-witness-peer", c.cluster, region, peer)
	}
	return nil, nil
}

func (c *RuleChecker) allowLeader(fit *placement.RegionFit, peer *metapb.Peer) bool {
	if core.IsLearner(peer) {
		return false
	}
	s := c.cluster.GetStore(peer.GetStoreId())
	if s == nil {
		return false
	}
	stateFilter := &filter.StoreStateFilter{ActionScope: "rule-checker", TransferLeader: true}
	if !stateFilter.Target(c.cluster.GetOpts(), s).IsOK() {
		return false
	}
	for _, rf := range fit.RuleFits {
		if (rf.Rule.Role == placement.Leader || rf.Rule.Role == placement.Voter) &&
			placement.MatchLabelConstraints(s, rf.Rule.LabelConstraints) {
			return true
		}
	}
	return false
}

func (c *RuleChecker) fixBetterLocation(region *core.RegionInfo, rf *placement.RuleFit) (*operator.Operator, error) {
	if len(rf.Rule.LocationLabels) == 0 || rf.Rule.Count <= 1 {
		return nil, nil
	}

	strategy := c.strategy(region, rf.Rule)
	ruleStores := c.getRuleFitStores(rf)
	oldStore := strategy.SelectStoreToRemove(ruleStores)
	if oldStore == 0 {
		return nil, nil
	}
	newStore, filterByTempState := strategy.SelectStoreToImprove(ruleStores, oldStore)
	if newStore == 0 {
		log.Debug("no replacement store", zap.Uint64("region-id", region.GetID()))
		c.handleFilterState(region, filterByTempState)
		return nil, nil
	}
	checkerCounter.WithLabelValues("rule_checker", "move-to-better-location").Inc()
	isWitness := rf.Rule.IsWitness
	if !c.isWitnessEnabled() {
		isWitness = false
	}
	newPeer := &metapb.Peer{StoreId: newStore, Role: rf.Rule.Role.MetaPeerRole(), IsWitness: isWitness}
	return operator.CreateMovePeerOperator("move-to-better-location", c.cluster, region, operator.OpReplica, oldStore, newPeer)
}

func (c *RuleChecker) fixOrphanPeers(region *core.RegionInfo, fit *placement.RegionFit) (*operator.Operator, error) {
	if len(fit.OrphanPeers) == 0 {
		return nil, nil
	}

	isUnhealthyPeer := func(id uint64) bool {
		for _, downPeer := range region.GetDownPeers() {
			if downPeer.Peer.GetId() == id {
				return true
			}
		}
		for _, pendingPeer := range region.GetPendingPeers() {
			if pendingPeer.GetId() == id {
				return true
			}
		}
		return false
	}

	isDisconnectedPeer := func(p *metapb.Peer) bool {
		// avoid to meet down store when fix orphan peers,
		// Isdisconnected is more strictly than IsUnhealthy.
		store := c.cluster.GetStore(p.GetStoreId())
		if store == nil {
			return true
		}
		return store.IsDisconnected()
	}

	checkDownPeer := func(peers []*metapb.Peer) (*metapb.Peer, bool) {
		for _, p := range peers {
			if isUnhealthyPeer(p.GetId()) {
				// make sure is down peer.
				if region.GetDownPeer(p.GetId()) != nil {
					return p, true
				}
				return nil, true
			}
			if isDisconnectedPeer(p) {
				return p, true
			}
		}
		return nil, false
	}

	// remove orphan peers only when all rules are satisfied (count+role) and all peers selected
	// by RuleFits is not pending or down.
	var pinDownPeer *metapb.Peer
	hasUnhealthyFit := false
	for _, rf := range fit.RuleFits {
		if !rf.IsSatisfied() {
			hasUnhealthyFit = true
			break
		}
		pinDownPeer, hasUnhealthyFit = checkDownPeer(rf.Peers)
		if hasUnhealthyFit {
			break
		}
	}

	// If hasUnhealthyFit is false, it is safe to delete the OrphanPeer.
	if !hasUnhealthyFit {
		checkerCounter.WithLabelValues("rule_checker", "remove-orphan-peer").Inc()
		return operator.CreateRemovePeerOperator("remove-orphan-peer", c.cluster, 0, region, fit.OrphanPeers[0].StoreId)
	}

	// try to use orphan peers to replace unhealthy down peers.
	for _, orphanPeer := range fit.OrphanPeers {
		if pinDownPeer != nil {
			if pinDownPeer.GetId() == orphanPeer.GetId() {
				continue
			}
			// make sure the orphan peer is healthy.
			if isUnhealthyPeer(orphanPeer.GetId()) || isDisconnectedPeer(orphanPeer) {
				continue
			}
			// no consider witness in this path.
			if pinDownPeer.GetIsWitness() || orphanPeer.GetIsWitness() {
				continue
			}
			// pinDownPeer's store should be disconnected, because we use more strict judge before.
			if !isDisconnectedPeer(pinDownPeer) {
				continue
			}
			// check if down peer can replace with orphan peer.
			dstStore := c.cluster.GetStore(orphanPeer.GetStoreId())
			if fit.Replace(pinDownPeer.GetStoreId(), dstStore) {
				destRole := pinDownPeer.GetRole()
				orphanPeerRole := orphanPeer.GetRole()
				checkerCounter.WithLabelValues("rule_checker", "replace-orphan-peer").Inc()
				switch {
				case orphanPeerRole == metapb.PeerRole_Learner && destRole == metapb.PeerRole_Voter:
					return operator.CreatePromoteLearnerOperatorAndRemovePeer("replace-down-peer-with-orphan-peer", c.cluster, region, orphanPeer, pinDownPeer)
				case orphanPeerRole == metapb.PeerRole_Voter && destRole == metapb.PeerRole_Learner:
					return operator.CreateDemoteLearnerOperatorAndRemovePeer("replace-down-peer-with-orphan-peer", c.cluster, region, orphanPeer, pinDownPeer)
				case orphanPeerRole == destRole && isDisconnectedPeer(pinDownPeer) && !dstStore.IsDisconnected():
					return operator.CreateRemovePeerOperator("remove-replaced-orphan-peer", c.cluster, 0, region, pinDownPeer.GetStoreId())
				default:
					// destRole should not same with orphanPeerRole. if role is same, it fit with orphanPeer should be better than now.
					// destRole never be leader, so we not consider it.
				}
			} else {
				checkerCounter.WithLabelValues("rule_checker", "replace-orphan-peer-no-fit").Inc()
			}
		}
	}

	// If hasUnhealthyFit is true, try to remove unhealthy orphan peers only if number of OrphanPeers is >= 2.
	// Ref https://github.com/tikv/pd/issues/4045
	if len(fit.OrphanPeers) >= 2 {
		hasHealthPeer := false
		var disconnectedPeer *metapb.Peer
		for _, orphanPeer := range fit.OrphanPeers {
			if isDisconnectedPeer(orphanPeer) {
				disconnectedPeer = orphanPeer
				break
			}
		}
		for _, orphanPeer := range fit.OrphanPeers {
			if isUnhealthyPeer(orphanPeer.GetId()) {
				checkerCounter.WithLabelValues("rule_checker", "remove-orphan-peer").Inc()
				return operator.CreateRemovePeerOperator("remove-unhealthy-orphan-peer", c.cluster, 0, region, orphanPeer.StoreId)
			}
			if hasHealthPeer {
				// there already exists a healthy orphan peer, so we can remove other orphan Peers.
				checkerCounter.WithLabelValues("rule_checker", "remove-orphan-peer").Inc()
				// if there exists a disconnected orphan peer, we will pick it to remove firstly.
				if disconnectedPeer != nil {
					return operator.CreateRemovePeerOperator("remove-orphan-peer", c.cluster, 0, region, disconnectedPeer.StoreId)
				}
				return operator.CreateRemovePeerOperator("remove-orphan-peer", c.cluster, 0, region, orphanPeer.StoreId)
			}
			hasHealthPeer = true
		}
	}
	checkerCounter.WithLabelValues("rule_checker", "skip-remove-orphan-peer").Inc()
	return nil, nil
}

func (c *RuleChecker) isDownPeer(region *core.RegionInfo, peer *metapb.Peer) bool {
	for _, stats := range region.GetDownPeers() {
		if stats.GetPeer().GetId() == peer.GetId() {
			storeID := peer.GetStoreId()
			store := c.cluster.GetStore(storeID)
			if store == nil {
				log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", storeID))
				return false
			}
			return true
		}
	}
	return false
}

func (c *RuleChecker) isStoreDownTimeHitMaxDownTime(storeID uint64) bool {
	store := c.cluster.GetStore(storeID)
	if store == nil {
		log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", storeID))
		return false
	}
	return store.DownTime() >= c.cluster.GetOpts().GetMaxStoreDownTime()
}

func (c *RuleChecker) isOfflinePeer(peer *metapb.Peer) bool {
	store := c.cluster.GetStore(peer.GetStoreId())
	if store == nil {
		log.Warn("lost the store, maybe you are recovering the PD cluster", zap.Uint64("store-id", peer.StoreId))
		return false
	}
	return !store.IsPreparing() && !store.IsServing()
}

func (c *RuleChecker) hasAvailableWitness(region *core.RegionInfo, peer *metapb.Peer) (*metapb.Peer, bool) {
	witnesses := region.GetWitnesses()
	if len(witnesses) == 0 {
		return nil, false
	}
	isAvailable := func(downPeers []*pdpb.PeerStats, witness *metapb.Peer) bool {
		for _, stats := range downPeers {
			if stats.GetPeer().GetId() == witness.GetId() {
				return false
			}
		}
		return c.cluster.GetStore(witness.GetStoreId()) != nil
	}
	downPeers := region.GetDownPeers()
	for _, witness := range witnesses {
		if witness.GetId() != peer.GetId() && isAvailable(downPeers, witness) {
			return witness, true
		}
	}
	return nil, false
}

func (c *RuleChecker) strategy(region *core.RegionInfo, rule *placement.Rule) *ReplicaStrategy {
	return &ReplicaStrategy{
		checkerName:    c.name,
		cluster:        c.cluster,
		isolationLevel: rule.IsolationLevel,
		locationLabels: rule.LocationLabels,
		region:         region,
		extraFilters:   []filter.Filter{filter.NewLabelConstraintFilter(c.name, rule.LabelConstraints)},
	}
}

func (c *RuleChecker) getRuleFitStores(rf *placement.RuleFit) []*core.StoreInfo {
	var stores []*core.StoreInfo
	for _, p := range rf.Peers {
		if s := c.cluster.GetStore(p.GetStoreId()); s != nil {
			stores = append(stores, s)
		}
	}
	return stores
}

func (c *RuleChecker) handleFilterState(region *core.RegionInfo, filterByTempState bool) {
	if filterByTempState {
		c.regionWaitingList.Put(region.GetID(), nil)
		c.pendingList.Remove(region.GetID())
	} else {
		c.pendingList.Put(region.GetID(), nil)
	}
}

type recorder struct {
	offlineLeaderCounter map[uint64]uint64
	lastUpdateTime       time.Time
}

func newRecord() *recorder {
	return &recorder{
		offlineLeaderCounter: make(map[uint64]uint64),
		lastUpdateTime:       time.Now(),
	}
}

func (o *recorder) getOfflineLeaderCount(storeID uint64) uint64 {
	return o.offlineLeaderCounter[storeID]
}

func (o *recorder) incOfflineLeaderCount(storeID uint64) {
	o.offlineLeaderCounter[storeID] += 1
	o.lastUpdateTime = time.Now()
}

// Offline is triggered manually and only appears when the node makes some adjustments. here is an operator timeout / 2.
var offlineCounterTTL = 5 * time.Minute

func (o *recorder) refresh(cluster schedule.Cluster) {
	// re-count the offlineLeaderCounter if the store is already tombstone or store is gone.
	if len(o.offlineLeaderCounter) > 0 && time.Since(o.lastUpdateTime) > offlineCounterTTL {
		needClean := false
		for _, storeID := range o.offlineLeaderCounter {
			store := cluster.GetStore(storeID)
			if store == nil || store.IsRemoved() {
				needClean = true
				break
			}
		}
		if needClean {
			o.offlineLeaderCounter = make(map[uint64]uint64)
		}
	}
}
