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
	"encoding/json"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/storage"
	"github.com/tikv/pd/pkg/utils/operatorutil"
)

type evictSlowStoreTestSuite struct {
	suite.Suite
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	es     Scheduler
	bs     Scheduler
	oc     *operator.Controller
}

func TestEvictSlowStoreTestSuite(t *testing.T) {
	suite.Run(t, new(evictSlowStoreTestSuite))
}

func (suite *evictSlowStoreTestSuite) SetupTest() {
	re := suite.Require()
	suite.cancel, _, suite.tc, suite.oc = prepareSchedulersTest()

	// Add stores 1, 2
	suite.tc.AddLeaderStore(1, 0)
	suite.tc.AddLeaderStore(2, 0)
	suite.tc.AddLeaderStore(3, 0)
	// Add regions 1, 2 with leaders in stores 1, 2
	suite.tc.AddLeaderRegion(1, 1, 2)
	suite.tc.AddLeaderRegion(2, 2, 1)
	suite.tc.UpdateLeaderCount(2, 16)

	storage := storage.NewStorageWithMemoryBackend()
	var err error
	suite.es, err = CreateScheduler(EvictSlowStoreType, suite.oc, storage, ConfigSliceDecoder(EvictSlowStoreType, []string{}), nil)
	re.NoError(err)
	suite.bs, err = CreateScheduler(BalanceLeaderType, suite.oc, storage, ConfigSliceDecoder(BalanceLeaderType, []string{}), nil)
	re.NoError(err)
}

func (suite *evictSlowStoreTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *evictSlowStoreTestSuite) TestEvictSlowStore() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap", "return(true)"))
	storeInfo := suite.tc.GetStore(1)
	newStoreInfo := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 100
	})
	suite.tc.PutStore(newStoreInfo)
	re.True(suite.es.IsScheduleAllowed(suite.tc))
	// Add evict leader scheduler to store 1
	ops, _ := suite.es.Schedule(suite.tc, false)
	operatorutil.CheckMultiTargetTransferLeader(re, ops[0], operator.OpLeader, 1, []uint64{2})
	re.Equal(EvictSlowStoreType, ops[0].Desc())
	// Cannot balance leaders to store 1
	ops, _ = suite.bs.Schedule(suite.tc, false)
	re.Empty(ops)
	newStoreInfo = storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 0
	})
	suite.tc.PutStore(newStoreInfo)
	// Evict leader scheduler of store 1 should be removed, then leader can be balanced to store 1
	ops, _ = suite.es.Schedule(suite.tc, false)
	re.Empty(ops)
	ops, _ = suite.bs.Schedule(suite.tc, false)
	operatorutil.CheckTransferLeader(re, ops[0], operator.OpLeader, 2, 1)

	// no slow store need to evict.
	ops, _ = suite.es.Schedule(suite.tc, false)
	re.Empty(ops)

	es2, ok := suite.es.(*evictSlowStoreScheduler)
	re.True(ok)
	re.Zero(es2.conf.evictStore())

	// check the value from storage.
	sches, vs, err := es2.conf.storage.LoadAllSchedulerConfigs()
	re.NoError(err)
	valueStr := ""
	for id, sche := range sches {
		if strings.EqualFold(sche, EvictSlowStoreName) {
			valueStr = vs[id]
		}
	}

	var persistValue evictSlowStoreSchedulerConfig
	err = json.Unmarshal([]byte(valueStr), &persistValue)
	re.NoError(err)
	re.Equal(es2.conf.EvictedStores, persistValue.EvictedStores)
	re.Zero(persistValue.evictStore())
	re.True(persistValue.readyForRecovery())
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/schedulers/transientRecoveryGap"))
}

func (suite *evictSlowStoreTestSuite) TestEvictSlowStorePrepare() {
	re := suite.Require()
	es2, ok := suite.es.(*evictSlowStoreScheduler)
	re.True(ok)
	re.Zero(es2.conf.evictStore())
	// prepare with no evict store.
	suite.es.PrepareConfig(suite.tc)

	es2.conf.setStoreAndPersist(1)
	re.Equal(uint64(1), es2.conf.evictStore())
	re.False(es2.conf.readyForRecovery())
	// prepare with evict store.
	suite.es.PrepareConfig(suite.tc)
}

func (suite *evictSlowStoreTestSuite) TestEvictSlowStorePersistFail() {
	re := suite.Require()
	persisFail := "github.com/tikv/pd/pkg/schedule/schedulers/persistFail"
	re.NoError(failpoint.Enable(persisFail, "return(true)"))

	storeInfo := suite.tc.GetStore(1)
	newStoreInfo := storeInfo.Clone(func(store *core.StoreInfo) {
		store.GetStoreStats().SlowScore = 100
	})
	suite.tc.PutStore(newStoreInfo)
	re.True(suite.es.IsScheduleAllowed(suite.tc))
	// Add evict leader scheduler to store 1
	ops, _ := suite.es.Schedule(suite.tc, false)
	re.Empty(ops)
	re.NoError(failpoint.Disable(persisFail))
	ops, _ = suite.es.Schedule(suite.tc, false)
	re.NotEmpty(ops)
}
