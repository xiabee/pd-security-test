// Copyright 2021 TiKV Project Authors.
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
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	"github.com/tikv/pd/pkg/errs"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/filter"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/statistics"
	"github.com/tikv/pd/pkg/statistics/utils"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/unrolled/render"
	"go.uber.org/zap"
)

const (
	// GrantHotRegionName is grant hot region scheduler name.
	GrantHotRegionName = "grant-hot-region-scheduler"
	// GrantHotRegionType is grant hot region scheduler type.
	GrantHotRegionType = "grant-hot-region"
)

var (
	// WithLabelValues is a heavy operation, define variable to avoid call it every time.
	grantHotRegionCounter     = schedulerCounter.WithLabelValues(GrantHotRegionName, "schedule")
	grantHotRegionSkipCounter = schedulerCounter.WithLabelValues(GrantHotRegionName, "skip")
)

type grantHotRegionSchedulerConfig struct {
	syncutil.RWMutex
	storage       endpoint.ConfigStorage
	cluster       *core.BasicCluster
	StoreIDs      []uint64 `json:"store-id"`
	StoreLeaderID uint64   `json:"store-leader-id"`
}

func (conf *grantHotRegionSchedulerConfig) setStore(leaderID uint64, peers []uint64) bool {
	conf.Lock()
	defer conf.Unlock()
	ret := slice.AnyOf(peers, func(i int) bool {
		return leaderID == peers[i]
	})
	if ret {
		conf.StoreLeaderID = leaderID
		conf.StoreIDs = peers
	}
	return ret
}

func (conf *grantHotRegionSchedulerConfig) GetStoreLeaderID() uint64 {
	conf.RLock()
	defer conf.RUnlock()
	return conf.StoreLeaderID
}

func (conf *grantHotRegionSchedulerConfig) SetStoreLeaderID(id uint64) {
	conf.Lock()
	defer conf.Unlock()
	conf.StoreLeaderID = id
}

func (conf *grantHotRegionSchedulerConfig) Clone() *grantHotRegionSchedulerConfig {
	conf.RLock()
	defer conf.RUnlock()
	newStoreIDs := make([]uint64, len(conf.StoreIDs))
	copy(newStoreIDs, conf.StoreIDs)
	return &grantHotRegionSchedulerConfig{
		StoreIDs:      newStoreIDs,
		StoreLeaderID: conf.StoreLeaderID,
	}
}

func (conf *grantHotRegionSchedulerConfig) Persist() error {
	name := conf.getSchedulerName()
	conf.RLock()
	defer conf.RUnlock()
	data, err := EncodeConfig(conf)
	if err != nil {
		return err
	}
	return conf.storage.SaveSchedulerConfig(name, data)
}

func (conf *grantHotRegionSchedulerConfig) getSchedulerName() string {
	return GrantHotRegionName
}

func (conf *grantHotRegionSchedulerConfig) has(storeID uint64) bool {
	conf.RLock()
	defer conf.RUnlock()
	return slice.AnyOf(conf.StoreIDs, func(i int) bool {
		return storeID == conf.StoreIDs[i]
	})
}

func (conf *grantHotRegionSchedulerConfig) getStoreIDs() []uint64 {
	conf.RLock()
	defer conf.RUnlock()
	storeIDs := make([]uint64, len(conf.StoreIDs))
	copy(storeIDs, conf.StoreIDs)
	return storeIDs
}

// grantLeaderScheduler transfers all hot peers to peers  and transfer leader to the fixed store
type grantHotRegionScheduler struct {
	*baseHotScheduler
	conf    *grantHotRegionSchedulerConfig
	handler http.Handler
}

// newGrantHotRegionScheduler creates an admin scheduler that transfers hot region peer to fixed store and hot region leader to one store.
func newGrantHotRegionScheduler(opController *operator.Controller, conf *grantHotRegionSchedulerConfig) *grantHotRegionScheduler {
	base := newBaseHotScheduler(opController)
	handler := newGrantHotRegionHandler(conf)
	ret := &grantHotRegionScheduler{
		baseHotScheduler: base,
		conf:             conf,
		handler:          handler,
	}
	return ret
}

func (s *grantHotRegionScheduler) GetName() string {
	return GrantHotRegionName
}

func (s *grantHotRegionScheduler) GetType() string {
	return GrantHotRegionType
}

func (s *grantHotRegionScheduler) EncodeConfig() ([]byte, error) {
	return EncodeConfig(s.conf)
}

func (s *grantHotRegionScheduler) ReloadConfig() error {
	s.conf.Lock()
	defer s.conf.Unlock()
	cfgData, err := s.conf.storage.LoadSchedulerConfig(s.GetName())
	if err != nil {
		return err
	}
	if len(cfgData) == 0 {
		return nil
	}
	newCfg := &grantHotRegionSchedulerConfig{}
	if err := DecodeConfig([]byte(cfgData), newCfg); err != nil {
		return err
	}
	s.conf.StoreIDs = newCfg.StoreIDs
	s.conf.StoreLeaderID = newCfg.StoreLeaderID
	return nil
}

// IsScheduleAllowed returns whether the scheduler is allowed to schedule.
// TODO it should check if there is any scheduler such as evict or hot region scheduler
func (s *grantHotRegionScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	conf := cluster.GetSchedulerConfig()
	regionAllowed := s.OpController.OperatorCount(operator.OpRegion) < conf.GetRegionScheduleLimit()
	leaderAllowed := s.OpController.OperatorCount(operator.OpLeader) < conf.GetLeaderScheduleLimit()
	if !regionAllowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpRegion.String()).Inc()
	}
	if !leaderAllowed {
		operator.OperatorLimitCounter.WithLabelValues(s.GetType(), operator.OpLeader.String()).Inc()
	}
	return regionAllowed && leaderAllowed
}

func (s *grantHotRegionScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

type grantHotRegionHandler struct {
	rd     *render.Render
	config *grantHotRegionSchedulerConfig
}

func (handler *grantHotRegionHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	var input map[string]any
	if err := apiutil.ReadJSONRespondError(handler.rd, w, r.Body, &input); err != nil {
		return
	}
	ids, ok := input["store-id"].(string)
	if !ok {
		_ = handler.rd.JSON(w, http.StatusBadRequest, errs.ErrSchedulerConfig)
		return
	}
	storeIDs := make([]uint64, 0)
	for _, v := range strings.Split(ids, ",") {
		id, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			_ = handler.rd.JSON(w, http.StatusBadRequest, errs.ErrBytesToUint64)
			return
		}
		storeIDs = append(storeIDs, id)
	}
	leaderID, err := strconv.ParseUint(input["store-leader-id"].(string), 10, 64)
	if err != nil {
		_ = handler.rd.JSON(w, http.StatusBadRequest, errs.ErrBytesToUint64)
		return
	}
	if !handler.config.setStore(leaderID, storeIDs) {
		_ = handler.rd.JSON(w, http.StatusBadRequest, errs.ErrSchedulerConfig)
		return
	}

	if err = handler.config.Persist(); err != nil {
		handler.config.SetStoreLeaderID(0)
		_ = handler.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = handler.rd.JSON(w, http.StatusOK, nil)
}

func (handler *grantHotRegionHandler) ListConfig(w http.ResponseWriter, _ *http.Request) {
	conf := handler.config.Clone()
	_ = handler.rd.JSON(w, http.StatusOK, conf)
}

func newGrantHotRegionHandler(config *grantHotRegionSchedulerConfig) http.Handler {
	h := &grantHotRegionHandler{
		config: config,
		rd:     render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/config", h.UpdateConfig).Methods(http.MethodPost)
	router.HandleFunc("/list", h.ListConfig).Methods(http.MethodGet)
	return router
}

func (s *grantHotRegionScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	grantHotRegionCounter.Inc()
	rw := s.randomRWType()
	s.prepareForBalance(rw, cluster)
	return s.dispatch(rw, cluster), nil
}

func (s *grantHotRegionScheduler) dispatch(typ utils.RWType, cluster sche.SchedulerCluster) []*operator.Operator {
	stLoadInfos := s.stLoadInfos[buildResourceType(typ, constant.RegionKind)]
	infos := make([]*statistics.StoreLoadDetail, len(stLoadInfos))
	index := 0
	for _, info := range stLoadInfos {
		infos[index] = info
		index++
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].LoadPred.Current.Loads[utils.ByteDim] > infos[j].LoadPred.Current.Loads[utils.ByteDim]
	})
	return s.randomSchedule(cluster, infos)
}

func (s *grantHotRegionScheduler) randomSchedule(cluster sche.SchedulerCluster, srcStores []*statistics.StoreLoadDetail) (ops []*operator.Operator) {
	isLeader := s.r.Int()%2 == 1
	for _, srcStore := range srcStores {
		srcStoreID := srcStore.GetID()
		if isLeader {
			if s.conf.has(srcStoreID) || len(srcStore.HotPeers) < 1 {
				continue
			}
		} else {
			if !s.conf.has(srcStoreID) || srcStoreID == s.conf.GetStoreLeaderID() {
				continue
			}
		}

		for _, peer := range srcStore.HotPeers {
			if s.OpController.GetOperator(peer.RegionID) != nil {
				continue
			}
			op, err := s.transfer(cluster, peer.RegionID, srcStoreID, isLeader)
			if err != nil {
				log.Debug("fail to create grant hot region operator", zap.Uint64("region-id", peer.RegionID),
					zap.Uint64("src store id", srcStoreID), errs.ZapError(err))
				continue
			}
			return []*operator.Operator{op}
		}
	}
	grantHotRegionSkipCounter.Inc()
	return nil
}

func (s *grantHotRegionScheduler) transfer(cluster sche.SchedulerCluster, regionID uint64, srcStoreID uint64, isLeader bool) (op *operator.Operator, err error) {
	srcRegion := cluster.GetRegion(regionID)
	if srcRegion == nil || len(srcRegion.GetDownPeers()) != 0 || len(srcRegion.GetPendingPeers()) != 0 {
		return nil, errs.ErrRegionRuleNotFound
	}
	srcStore := cluster.GetStore(srcStoreID)
	if srcStore == nil {
		log.Error("failed to get the source store", zap.Uint64("store-id", srcStoreID), errs.ZapError(errs.ErrGetSourceStore))
		return nil, errs.ErrStoreNotFound
	}
	filters := []filter.Filter{
		filter.NewPlacementSafeguard(s.GetName(), cluster.GetSchedulerConfig(), cluster.GetBasicCluster(), cluster.GetRuleManager(), srcRegion, srcStore, nil),
	}

	storeIDs := s.conf.getStoreIDs()
	destStoreIDs := make([]uint64, 0, len(storeIDs))
	var candidate []uint64
	if isLeader {
		filters = append(filters, &filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true, OperatorLevel: constant.High})
		candidate = []uint64{s.conf.GetStoreLeaderID()}
	} else {
		filters = append(filters, &filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true, OperatorLevel: constant.High},
			filter.NewExcludedFilter(s.GetName(), srcRegion.GetStoreIDs(), srcRegion.GetStoreIDs()))
		candidate = storeIDs
	}
	for _, storeID := range candidate {
		store := cluster.GetStore(storeID)
		if !filter.Target(cluster.GetSchedulerConfig(), store, filters) {
			continue
		}
		destStoreIDs = append(destStoreIDs, storeID)
	}
	if len(destStoreIDs) == 0 {
		return nil, errs.ErrCheckerNotFound
	}

	srcPeer := srcStore.GetMeta()
	if srcPeer == nil {
		return nil, errs.ErrStoreNotFound
	}
	i := s.r.Int() % len(destStoreIDs)
	dstStore := &metapb.Peer{StoreId: destStoreIDs[i]}

	if isLeader {
		op, err = operator.CreateTransferLeaderOperator(GrantHotRegionType+"-leader", cluster, srcRegion, srcRegion.GetLeader().GetStoreId(), dstStore.StoreId, []uint64{}, operator.OpLeader)
	} else {
		op, err = operator.CreateMovePeerOperator(GrantHotRegionType+"-move", cluster, srcRegion, operator.OpRegion|operator.OpLeader, srcStore.GetID(), dstStore)
	}
	op.SetPriorityLevel(constant.High)
	return
}
