// Copyright 2023 TiKV Project Authors.
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

package handler

import (
	"bytes"
	"encoding/hex"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/schedule"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/schedule/scatter"
)

// Server is the interface for handler about schedule.
// TODO: remove it after GetCluster is unified between PD server and Scheduling server.
type Server interface {
	GetCoordinator() *schedule.Coordinator
	GetCluster() sche.SharedCluster
}

// Handler is a handler to handle http request about schedule.
type Handler struct {
	Server
}

// NewHandler creates a new handler.
func NewHandler(server Server) *Handler {
	return &Handler{
		Server: server,
	}
}

// GetOperatorController returns OperatorController.
func (h *Handler) GetOperatorController() (*operator.Controller, error) {
	co := h.GetCoordinator()
	if co == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	return co.GetOperatorController(), nil
}

// GetRegionScatterer returns RegionScatterer.
func (h *Handler) GetRegionScatterer() (*scatter.RegionScatterer, error) {
	co := h.GetCoordinator()
	if co == nil {
		return nil, errs.ErrNotBootstrapped.GenWithStackByArgs()
	}
	return co.GetRegionScatterer(), nil
}

// GetOperator returns the region operator.
func (h *Handler) GetOperator(regionID uint64) (*operator.Operator, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}

	op := c.GetOperator(regionID)
	if op == nil {
		return nil, errs.ErrOperatorNotFound
	}

	return op, nil
}

// GetOperatorStatus returns the status of the region operator.
func (h *Handler) GetOperatorStatus(regionID uint64) (*operator.OpWithStatus, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}

	op := c.GetOperatorStatus(regionID)
	if op == nil {
		return nil, errs.ErrOperatorNotFound
	}

	return op, nil
}

// RemoveOperator removes the region operator.
func (h *Handler) RemoveOperator(regionID uint64) error {
	c, err := h.GetOperatorController()
	if err != nil {
		return err
	}

	op := c.GetOperator(regionID)
	if op == nil {
		return errs.ErrOperatorNotFound
	}

	_ = c.RemoveOperator(op, operator.AdminStop)
	return nil
}

// GetOperators returns the running operators.
func (h *Handler) GetOperators() ([]*operator.Operator, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}
	return c.GetOperators(), nil
}

// GetWaitingOperators returns the waiting operators.
func (h *Handler) GetWaitingOperators() ([]*operator.Operator, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}
	return c.GetWaitingOperators(), nil
}

// GetAdminOperators returns the running admin operators.
func (h *Handler) GetAdminOperators() ([]*operator.Operator, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}
	return c.GetOperatorsOfKind(operator.OpAdmin), nil
}

// GetLeaderOperators returns the running leader operators.
func (h *Handler) GetLeaderOperators() ([]*operator.Operator, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}
	return c.GetOperatorsOfKind(operator.OpLeader), nil
}

// GetRegionOperators returns the running region operators.
func (h *Handler) GetRegionOperators() ([]*operator.Operator, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}
	return c.GetOperatorsOfKind(operator.OpRegion), nil
}

// GetHistory returns finished operators' history since start.
func (h *Handler) GetHistory(start time.Time) ([]operator.OpHistory, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}
	return c.GetHistory(start), nil
}

// GetRecords returns finished operators since start.
func (h *Handler) GetRecords(from time.Time) ([]*operator.OpRecord, error) {
	c, err := h.GetOperatorController()
	if err != nil {
		return nil, err
	}
	records := c.GetRecords(from)
	if len(records) == 0 {
		return nil, errs.ErrOperatorNotFound
	}
	return records, nil
}

// AddTransferLeaderOperator adds an operator to transfer leader to the store.
func (h *Handler) AddTransferLeaderOperator(regionID uint64, storeID uint64) error {
	c := h.GetCluster()
	region := c.GetRegion(regionID)
	if region == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(regionID)
	}

	newLeader := region.GetStoreVoter(storeID)
	if newLeader == nil {
		return errors.Errorf("region has no voter in store %v", storeID)
	}

	op, err := operator.CreateTransferLeaderOperator("admin-transfer-leader", c, region, region.GetLeader().GetStoreId(), newLeader.GetStoreId(), []uint64{}, operator.OpAdmin)
	if err != nil {
		log.Debug("fail to create transfer leader operator", errs.ZapError(err))
		return err
	}
	return h.addOperator(op)
}

// AddTransferRegionOperator adds an operator to transfer region to the stores.
func (h *Handler) AddTransferRegionOperator(regionID uint64, storeIDs map[uint64]placement.PeerRoleType) error {
	c := h.GetCluster()
	region := c.GetRegion(regionID)
	if region == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(regionID)
	}

	if c.GetSharedConfig().IsPlacementRulesEnabled() {
		// Cannot determine role without peer role when placement rules enabled. Not supported now.
		for _, role := range storeIDs {
			if len(role) == 0 {
				return errors.New("transfer region without peer role is not supported when placement rules enabled")
			}
		}
	}
	for id := range storeIDs {
		if err := checkStoreState(c, id); err != nil {
			return err
		}
	}

	roles := make(map[uint64]placement.PeerRoleType)
	for id, peerRole := range storeIDs {
		if peerRole == "" {
			peerRole = placement.Voter
		}
		roles[id] = peerRole
	}
	op, err := operator.CreateMoveRegionOperator("admin-move-region", c, region, operator.OpAdmin, roles)
	if err != nil {
		log.Debug("fail to create move region operator", errs.ZapError(err))
		return err
	}
	return h.addOperator(op)
}

// AddTransferPeerOperator adds an operator to transfer peer.
func (h *Handler) AddTransferPeerOperator(regionID uint64, fromStoreID, toStoreID uint64) error {
	c := h.GetCluster()
	region := c.GetRegion(regionID)
	if region == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(regionID)
	}

	oldPeer := region.GetStorePeer(fromStoreID)
	if oldPeer == nil {
		return errors.Errorf("region has no peer in store %v", fromStoreID)
	}

	if err := checkStoreState(c, toStoreID); err != nil {
		return err
	}

	newPeer := &metapb.Peer{StoreId: toStoreID, Role: oldPeer.GetRole(), IsWitness: oldPeer.GetIsWitness()}
	op, err := operator.CreateMovePeerOperator("admin-move-peer", c, region, operator.OpAdmin, fromStoreID, newPeer)
	if err != nil {
		log.Debug("fail to create move peer operator", errs.ZapError(err))
		return err
	}
	return h.addOperator(op)
}

// checkAdminAddPeerOperator checks adminAddPeer operator with given region ID and store ID.
func (h *Handler) checkAdminAddPeerOperator(regionID uint64, toStoreID uint64) (sche.SharedCluster, *core.RegionInfo, error) {
	c := h.GetCluster()
	region := c.GetRegion(regionID)
	if region == nil {
		return nil, nil, errs.ErrRegionNotFound.FastGenByArgs(regionID)
	}

	if region.GetStorePeer(toStoreID) != nil {
		return nil, nil, errors.Errorf("region already has peer in store %v", toStoreID)
	}

	if err := checkStoreState(c, toStoreID); err != nil {
		return nil, nil, err
	}

	return c, region, nil
}

// AddAddPeerOperator adds an operator to add peer.
func (h *Handler) AddAddPeerOperator(regionID uint64, toStoreID uint64) error {
	c, region, err := h.checkAdminAddPeerOperator(regionID, toStoreID)
	if err != nil {
		return err
	}

	newPeer := &metapb.Peer{StoreId: toStoreID}
	op, err := operator.CreateAddPeerOperator("admin-add-peer", c, region, newPeer, operator.OpAdmin)
	if err != nil {
		log.Debug("fail to create add peer operator", errs.ZapError(err))
		return err
	}
	return h.addOperator(op)
}

// AddAddLearnerOperator adds an operator to add learner.
func (h *Handler) AddAddLearnerOperator(regionID uint64, toStoreID uint64) error {
	c, region, err := h.checkAdminAddPeerOperator(regionID, toStoreID)
	if err != nil {
		return err
	}

	newPeer := &metapb.Peer{
		StoreId: toStoreID,
		Role:    metapb.PeerRole_Learner,
	}

	op, err := operator.CreateAddPeerOperator("admin-add-learner", c, region, newPeer, operator.OpAdmin)
	if err != nil {
		log.Debug("fail to create add learner operator", errs.ZapError(err))
		return err
	}
	return h.addOperator(op)
}

// AddRemovePeerOperator adds an operator to remove peer.
func (h *Handler) AddRemovePeerOperator(regionID uint64, fromStoreID uint64) error {
	c := h.GetCluster()
	region := c.GetRegion(regionID)
	if region == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(regionID)
	}

	if region.GetStorePeer(fromStoreID) == nil {
		return errors.Errorf("region has no peer in store %v", fromStoreID)
	}

	op, err := operator.CreateRemovePeerOperator("admin-remove-peer", c, operator.OpAdmin, region, fromStoreID)
	if err != nil {
		log.Debug("fail to create move peer operator", errs.ZapError(err))
		return err
	}
	return h.addOperator(op)
}

// AddMergeRegionOperator adds an operator to merge region.
func (h *Handler) AddMergeRegionOperator(regionID uint64, targetID uint64) error {
	c := h.GetCluster()
	region := c.GetRegion(regionID)
	if region == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(regionID)
	}

	target := c.GetRegion(targetID)
	if target == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(targetID)
	}

	if !filter.IsRegionHealthy(region) || !filter.IsRegionReplicated(c, region) {
		return errs.ErrRegionAbnormalPeer.FastGenByArgs(regionID)
	}

	if !filter.IsRegionHealthy(target) || !filter.IsRegionReplicated(c, target) {
		return errs.ErrRegionAbnormalPeer.FastGenByArgs(targetID)
	}

	// for the case first region (start key is nil) with the last region (end key is nil) but not adjacent
	if (!bytes.Equal(region.GetStartKey(), target.GetEndKey()) || len(region.GetStartKey()) == 0) &&
		(!bytes.Equal(region.GetEndKey(), target.GetStartKey()) || len(region.GetEndKey()) == 0) {
		return errs.ErrRegionNotAdjacent
	}

	ops, err := operator.CreateMergeRegionOperator("admin-merge-region", c, region, target, operator.OpAdmin)
	if err != nil {
		log.Debug("fail to create merge region operator", errs.ZapError(err))
		return err
	}
	return h.addOperator(ops...)
}

// AddSplitRegionOperator adds an operator to split a region.
func (h *Handler) AddSplitRegionOperator(regionID uint64, policyStr string, keys []string) error {
	c := h.GetCluster()
	region := c.GetRegion(regionID)
	if region == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(regionID)
	}

	policy, ok := pdpb.CheckPolicy_value[strings.ToUpper(policyStr)]
	if !ok {
		return errors.Errorf("check policy %s is not supported", policyStr)
	}

	var splitKeys [][]byte
	if pdpb.CheckPolicy(policy) == pdpb.CheckPolicy_USEKEY {
		for i := range keys {
			k, err := hex.DecodeString(keys[i])
			if err != nil {
				return errors.Errorf("split key %s is not in hex format", keys[i])
			}
			splitKeys = append(splitKeys, k)
		}
	}

	op, err := operator.CreateSplitRegionOperator("admin-split-region", region, operator.OpAdmin, pdpb.CheckPolicy(policy), splitKeys)
	if err != nil {
		return err
	}

	return h.addOperator(op)
}

// AddScatterRegionOperator adds an operator to scatter a region.
func (h *Handler) AddScatterRegionOperator(regionID uint64, group string) error {
	c := h.GetCluster()
	region := c.GetRegion(regionID)
	if region == nil {
		return errs.ErrRegionNotFound.FastGenByArgs(regionID)
	}

	if c.IsRegionHot(region) {
		return errors.Errorf("region %d is a hot region", regionID)
	}

	s, err := h.GetRegionScatterer()
	if err != nil {
		return err
	}

	op, err := s.Scatter(region, group, false)
	if err != nil {
		return err
	}

	if op == nil {
		return nil
	}
	return h.addOperator(op)
}

// AddScatterRegionsOperators add operators to scatter regions and return the processed percentage and error
func (h *Handler) AddScatterRegionsOperators(regionIDs []uint64, startRawKey, endRawKey, group string, retryLimit int) (int, error) {
	s, err := h.GetRegionScatterer()
	if err != nil {
		return 0, err
	}
	opsCount := 0
	var failures map[uint64]error
	// If startKey and endKey are both defined, use them first.
	if len(startRawKey) > 0 && len(endRawKey) > 0 {
		startKey, err := hex.DecodeString(startRawKey)
		if err != nil {
			return 0, err
		}
		endKey, err := hex.DecodeString(endRawKey)
		if err != nil {
			return 0, err
		}
		opsCount, failures, err = s.ScatterRegionsByRange(startKey, endKey, group, retryLimit)
		if err != nil {
			return 0, err
		}
	} else {
		opsCount, failures, err = s.ScatterRegionsByID(regionIDs, group, retryLimit, false)
		if err != nil {
			return 0, err
		}
	}
	percentage := 100
	if len(failures) > 0 {
		percentage = 100 - 100*len(failures)/(opsCount+len(failures))
	}
	return percentage, nil
}

func (h *Handler) addOperator(ops ...*operator.Operator) error {
	oc, err := h.GetOperatorController()
	if err != nil {
		return err
	}

	if ok := oc.AddOperator(ops...); !ok {
		return errors.WithStack(errs.ErrAddOperator)
	}
	return nil
}

func checkStoreState(c sche.SharedCluster, storeID uint64) error {
	store := c.GetStore(storeID)
	if store == nil {
		return errs.ErrStoreNotFound.FastGenByArgs(storeID)
	}
	if store.IsRemoved() {
		return errs.ErrStoreRemoved.FastGenByArgs(storeID)
	}
	if store.IsUnhealthy() {
		return errs.ErrStoreUnhealthy.FastGenByArgs(storeID)
	}
	return nil
}
