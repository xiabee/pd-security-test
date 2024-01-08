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

package scheduler_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/core"
	sc "github.com/tikv/pd/pkg/schedule/config"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/versioninfo"
	pdTests "github.com/tikv/pd/tests"
	ctl "github.com/tikv/pd/tools/pd-ctl/pdctl"
	"github.com/tikv/pd/tools/pd-ctl/tests"
)

type schedulerTestSuite struct {
	suite.Suite
	env               *pdTests.SchedulingTestEnvironment
	defaultSchedulers []string
}

func TestSchedulerTestSuite(t *testing.T) {
	suite.Run(t, new(schedulerTestSuite))
}

func (suite *schedulerTestSuite) SetupSuite() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/server/cluster/skipStoreConfigSync", `return(true)`))
	suite.env = pdTests.NewSchedulingTestEnvironment(suite.T())
	suite.defaultSchedulers = []string{
		"balance-leader-scheduler",
		"balance-region-scheduler",
		"balance-hot-region-scheduler",
		"balance-witness-scheduler",
		"transfer-witness-leader-scheduler",
		"evict-slow-store-scheduler",
	}
}

func (suite *schedulerTestSuite) TearDownSuite() {
	re := suite.Require()
	suite.env.Cleanup()
	re.NoError(failpoint.Disable("github.com/tikv/pd/server/cluster/skipStoreConfigSync"))
}

func (suite *schedulerTestSuite) TearDownTest() {
	cleanFunc := func(cluster *pdTests.TestCluster) {
		re := suite.Require()
		pdAddr := cluster.GetConfig().GetClientURL()
		cmd := ctl.GetRootCmd()

		var currentSchedulers []string
		mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "show"}, &currentSchedulers)
		for _, scheduler := range suite.defaultSchedulers {
			if slice.NoneOf(currentSchedulers, func(i int) bool {
				return currentSchedulers[i] == scheduler
			}) {
				echo := mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "add", scheduler}, nil)
				re.Contains(echo, "Success!")
			}
		}
		for _, scheduler := range currentSchedulers {
			if slice.NoneOf(suite.defaultSchedulers, func(i int) bool {
				return suite.defaultSchedulers[i] == scheduler
			}) {
				echo := mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "remove", scheduler}, nil)
				re.Contains(echo, "Success!")
			}
		}
	}
	suite.env.RunFuncInTwoModes(cleanFunc)
}

func (suite *schedulerTestSuite) TestScheduler() {
	suite.env.RunTestInTwoModes(suite.checkScheduler)
}

func (suite *schedulerTestSuite) checkScheduler(cluster *pdTests.TestCluster) {
	re := suite.Require()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	stores := []*metapb.Store{
		{
			Id:            1,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            2,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            3,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            4,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
	}

	mustUsage := func(args []string) {
		output, err := tests.ExecuteCommand(cmd, args...)
		re.NoError(err)
		re.Contains(string(output), "Usage")
	}

	checkSchedulerCommand := func(args []string, expected map[string]bool) {
		if args != nil {
			echo := mustExec(re, cmd, args, nil)
			re.Contains(echo, "Success!")
		}
		testutil.Eventually(re, func() bool {
			var schedulers []string
			mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "show"}, &schedulers)
			if len(schedulers) != len(expected) {
				return false
			}
			for _, scheduler := range schedulers {
				if _, ok := expected[scheduler]; !ok {
					return false
				}
			}
			return true
		})
	}

	checkSchedulerConfigCommand := func(expectedConfig map[string]interface{}, schedulerName string) {
		testutil.Eventually(re, func() bool {
			configInfo := make(map[string]interface{})
			mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", schedulerName}, &configInfo)
			return reflect.DeepEqual(expectedConfig, configInfo)
		})
	}

	leaderServer := cluster.GetLeaderServer()
	for _, store := range stores {
		pdTests.MustPutStore(re, cluster, store)
	}

	// note: because pdqsort is a unstable sort algorithm, set ApproximateSize for this region.
	pdTests.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"), core.SetApproximateSize(10))

	// scheduler show command
	expected := map[string]bool{
		"balance-region-scheduler":          true,
		"balance-leader-scheduler":          true,
		"balance-hot-region-scheduler":      true,
		"transfer-witness-leader-scheduler": true,
		"balance-witness-scheduler":         true,
		"evict-slow-store-scheduler":        true,
	}
	checkSchedulerCommand(nil, expected)

	// scheduler delete command
	args := []string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"}
	expected = map[string]bool{
		"balance-leader-scheduler":          true,
		"balance-hot-region-scheduler":      true,
		"transfer-witness-leader-scheduler": true,
		"balance-witness-scheduler":         true,
		"evict-slow-store-scheduler":        true,
	}
	checkSchedulerCommand(args, expected)

	// avoid the influence of the scheduler order
	schedulers := []string{"evict-leader-scheduler", "grant-leader-scheduler", "evict-leader-scheduler", "grant-leader-scheduler"}

	checkStorePause := func(changedStores []uint64, schedulerName string) {
		status := func() string {
			switch schedulerName {
			case "evict-leader-scheduler":
				return "paused"
			case "grant-leader-scheduler":
				return "resumed"
			default:
				re.Fail(fmt.Sprintf("unknown scheduler %s", schedulerName))
				return ""
			}
		}()
		for _, store := range stores {
			isStorePaused := !cluster.GetLeaderServer().GetRaftCluster().GetStore(store.GetId()).AllowLeaderTransfer()
			if slice.AnyOf(changedStores, func(i int) bool {
				return store.GetId() == changedStores[i]
			}) {
				re.True(isStorePaused,
					fmt.Sprintf("store %d should be %s with %s", store.GetId(), status, schedulerName))
			} else {
				re.False(isStorePaused,
					fmt.Sprintf("store %d should not be %s with %s", store.GetId(), status, schedulerName))
			}
			if sche := cluster.GetSchedulingPrimaryServer(); sche != nil {
				re.Equal(isStorePaused, !sche.GetCluster().GetStore(store.GetId()).AllowLeaderTransfer())
			}
		}
	}

	for idx := range schedulers {
		checkStorePause([]uint64{}, schedulers[idx])
		// scheduler add command
		args = []string{"-u", pdAddr, "scheduler", "add", schedulers[idx], "2"}
		expected = map[string]bool{
			"balance-leader-scheduler":          true,
			"balance-hot-region-scheduler":      true,
			schedulers[idx]:                     true,
			"transfer-witness-leader-scheduler": true,
			"balance-witness-scheduler":         true,
			"evict-slow-store-scheduler":        true,
		}
		checkSchedulerCommand(args, expected)

		// scheduler config show command
		expectedConfig := make(map[string]interface{})
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(expectedConfig, schedulers[idx])
		checkStorePause([]uint64{2}, schedulers[idx])

		// scheduler config update command
		args = []string{"-u", pdAddr, "scheduler", "config", schedulers[idx], "add-store", "3"}
		expected = map[string]bool{
			"balance-leader-scheduler":          true,
			"balance-hot-region-scheduler":      true,
			schedulers[idx]:                     true,
			"transfer-witness-leader-scheduler": true,
			"balance-witness-scheduler":         true,
			"evict-slow-store-scheduler":        true,
		}

		// check update success
		checkSchedulerCommand(args, expected)
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}, "3": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(expectedConfig, schedulers[idx])
		checkStorePause([]uint64{2, 3}, schedulers[idx])

		// scheduler delete command
		args = []string{"-u", pdAddr, "scheduler", "remove", schedulers[idx]}
		expected = map[string]bool{
			"balance-leader-scheduler":          true,
			"balance-hot-region-scheduler":      true,
			"transfer-witness-leader-scheduler": true,
			"balance-witness-scheduler":         true,
			"evict-slow-store-scheduler":        true,
		}
		checkSchedulerCommand(args, expected)
		checkStorePause([]uint64{}, schedulers[idx])

		// scheduler add command
		args = []string{"-u", pdAddr, "scheduler", "add", schedulers[idx], "2"}
		expected = map[string]bool{
			"balance-leader-scheduler":          true,
			"balance-hot-region-scheduler":      true,
			schedulers[idx]:                     true,
			"transfer-witness-leader-scheduler": true,
			"balance-witness-scheduler":         true,
			"evict-slow-store-scheduler":        true,
		}
		checkSchedulerCommand(args, expected)
		checkStorePause([]uint64{2}, schedulers[idx])

		// scheduler add command twice
		args = []string{"-u", pdAddr, "scheduler", "add", schedulers[idx], "4"}
		expected = map[string]bool{
			"balance-leader-scheduler":          true,
			"balance-hot-region-scheduler":      true,
			schedulers[idx]:                     true,
			"transfer-witness-leader-scheduler": true,
			"balance-witness-scheduler":         true,
			"evict-slow-store-scheduler":        true,
		}
		checkSchedulerCommand(args, expected)

		// check add success
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}, "4": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(expectedConfig, schedulers[idx])
		checkStorePause([]uint64{2, 4}, schedulers[idx])

		// scheduler remove command [old]
		args = []string{"-u", pdAddr, "scheduler", "remove", schedulers[idx] + "-4"}
		expected = map[string]bool{
			"balance-leader-scheduler":          true,
			"balance-hot-region-scheduler":      true,
			schedulers[idx]:                     true,
			"transfer-witness-leader-scheduler": true,
			"balance-witness-scheduler":         true,
			"evict-slow-store-scheduler":        true,
		}
		checkSchedulerCommand(args, expected)

		// check remove success
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(expectedConfig, schedulers[idx])
		checkStorePause([]uint64{2}, schedulers[idx])

		// scheduler remove command, when remove the last store, it should remove whole scheduler
		args = []string{"-u", pdAddr, "scheduler", "remove", schedulers[idx] + "-2"}
		expected = map[string]bool{
			"balance-leader-scheduler":          true,
			"balance-hot-region-scheduler":      true,
			"transfer-witness-leader-scheduler": true,
			"balance-witness-scheduler":         true,
			"evict-slow-store-scheduler":        true,
		}
		checkSchedulerCommand(args, expected)
		checkStorePause([]uint64{}, schedulers[idx])
	}

	// test shuffle region config
	checkSchedulerCommand([]string{"-u", pdAddr, "scheduler", "add", "shuffle-region-scheduler"}, map[string]bool{
		"balance-leader-scheduler":          true,
		"balance-hot-region-scheduler":      true,
		"shuffle-region-scheduler":          true,
		"transfer-witness-leader-scheduler": true,
		"balance-witness-scheduler":         true,
		"evict-slow-store-scheduler":        true,
	})
	var roles []string
	mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "shuffle-region-scheduler", "show-roles"}, &roles)
	re.Equal([]string{"leader", "follower", "learner"}, roles)
	echo := mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "shuffle-region-scheduler", "set-roles", "learner"}, nil)
	re.Contains(echo, "Success!")
	testutil.Eventually(re, func() bool {
		mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "shuffle-region-scheduler", "show-roles"}, &roles)
		return reflect.DeepEqual([]string{"learner"}, roles)
	})
	mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "shuffle-region-scheduler"}, &roles)
	re.Equal([]string{"learner"}, roles)

	// test grant hot region scheduler config
	checkSchedulerCommand([]string{"-u", pdAddr, "scheduler", "add", "grant-hot-region-scheduler", "1", "1,2,3"}, map[string]bool{
		"balance-leader-scheduler":          true,
		"balance-hot-region-scheduler":      true,
		"shuffle-region-scheduler":          true,
		"grant-hot-region-scheduler":        true,
		"transfer-witness-leader-scheduler": true,
		"balance-witness-scheduler":         true,
		"evict-slow-store-scheduler":        true,
	})
	var conf3 map[string]interface{}
	expected3 := map[string]interface{}{
		"store-id":        []interface{}{float64(1), float64(2), float64(3)},
		"store-leader-id": float64(1),
	}
	mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "grant-hot-region-scheduler"}, &conf3)
	re.Equal(expected3, conf3)

	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "grant-hot-region-scheduler", "set", "2", "1,2,3"}, nil)
	re.Contains(echo, "Success!")
	expected3["store-leader-id"] = float64(2)
	testutil.Eventually(re, func() bool {
		mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "grant-hot-region-scheduler"}, &conf3)
		return reflect.DeepEqual(expected3, conf3)
	})

	// test remove and add scheduler
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "add", "balance-region-scheduler"}, nil)
	re.Contains(echo, "Success!")
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"}, nil)
	re.Contains(echo, "Success!")
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"}, nil)
	re.NotContains(echo, "Success!")
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "add", "balance-region-scheduler"}, nil)
	re.Contains(echo, "Success!")
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "add", "evict-leader-scheduler", "1"}, nil)
	re.Contains(echo, "Success!")
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "remove", "evict-leader-scheduler-1"}, nil)
	re.Contains(echo, "Success!")
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "remove", "evict-leader-scheduler-1"}, nil)
	re.Contains(echo, "404")
	testutil.Eventually(re, func() bool { // wait for removed scheduler to be synced to scheduling server.
		echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "evict-leader-scheduler"}, nil)
		return strings.Contains(echo, "[404] scheduler not found")
	})

	// test hot region config
	expected1 := map[string]interface{}{
		"min-hot-byte-rate":          float64(100),
		"min-hot-key-rate":           float64(10),
		"min-hot-query-rate":         float64(10),
		"max-zombie-rounds":          float64(3),
		"max-peer-number":            float64(1000),
		"byte-rate-rank-step-ratio":  0.05,
		"key-rate-rank-step-ratio":   0.05,
		"query-rate-rank-step-ratio": 0.05,
		"count-rank-step-ratio":      0.01,
		"great-dec-ratio":            0.95,
		"minor-dec-ratio":            0.99,
		"src-tolerance-ratio":        1.05,
		"dst-tolerance-ratio":        1.05,
		"read-priorities":            []interface{}{"byte", "key"},
		"write-leader-priorities":    []interface{}{"key", "byte"},
		"write-peer-priorities":      []interface{}{"byte", "key"},
		"strict-picking-store":       "true",
		"enable-for-tiflash":         "true",
		"rank-formula-version":       "v2",
		"split-thresholds":           0.2,
	}
	checkHotSchedulerConfig := func(expect map[string]interface{}) {
		testutil.Eventually(re, func() bool {
			var conf1 map[string]interface{}
			mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
			return reflect.DeepEqual(expect, conf1)
		})
	}

	var conf map[string]interface{}
	mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "list"}, &conf)
	re.Equal(expected1, conf)
	mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "show"}, &conf)
	re.Equal(expected1, conf)
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "src-tolerance-ratio", "1.02"}, nil)
	re.Contains(echo, "Success!")
	expected1["src-tolerance-ratio"] = 1.02
	checkHotSchedulerConfig(expected1)

	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", "byte,key"}, nil)
	re.Contains(echo, "Success!")
	expected1["read-priorities"] = []interface{}{"byte", "key"}
	checkHotSchedulerConfig(expected1)

	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", "key"}, nil)
	re.Contains(echo, "Failed!")
	checkHotSchedulerConfig(expected1)
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", "key,byte"}, nil)
	re.Contains(echo, "Success!")
	expected1["read-priorities"] = []interface{}{"key", "byte"}
	checkHotSchedulerConfig(expected1)
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", "foo,bar"}, nil)
	re.Contains(echo, "Failed!")
	checkHotSchedulerConfig(expected1)
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", ""}, nil)
	re.Contains(echo, "Failed!")
	checkHotSchedulerConfig(expected1)
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", "key,key"}, nil)
	re.Contains(echo, "Failed!")
	checkHotSchedulerConfig(expected1)
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", "byte,byte"}, nil)
	re.Contains(echo, "Failed!")
	checkHotSchedulerConfig(expected1)
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "read-priorities", "key,key,byte"}, nil)
	re.Contains(echo, "Failed!")
	checkHotSchedulerConfig(expected1)

	// write-priorities is divided into write-leader-priorities and write-peer-priorities
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "write-priorities", "key,byte"}, nil)
	re.Contains(echo, "Failed!")
	re.Contains(echo, "Config item is not found.")
	checkHotSchedulerConfig(expected1)

	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "rank-formula-version", "v0"}, nil)
	re.Contains(echo, "Failed!")
	checkHotSchedulerConfig(expected1)
	expected1["rank-formula-version"] = "v2"
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "rank-formula-version", "v2"}, nil)
	re.Contains(echo, "Success!")
	checkHotSchedulerConfig(expected1)
	expected1["rank-formula-version"] = "v1"
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "rank-formula-version", "v1"}, nil)
	re.Contains(echo, "Success!")
	checkHotSchedulerConfig(expected1)

	expected1["forbid-rw-type"] = "read"
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "forbid-rw-type", "read"}, nil)
	re.Contains(echo, "Success!")
	checkHotSchedulerConfig(expected1)

	// test compatibility
	re.Equal("2.0.0", leaderServer.GetClusterVersion().String())
	for _, store := range stores {
		version := versioninfo.HotScheduleWithQuery
		store.Version = versioninfo.MinSupportedVersion(version).String()
		store.LastHeartbeat = time.Now().UnixNano()
		pdTests.MustPutStore(re, cluster, store)
	}
	re.Equal("5.2.0", leaderServer.GetClusterVersion().String())
	// After upgrading, we should not use query.
	checkHotSchedulerConfig(expected1)
	// cannot set qps as write-peer-priorities
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "write-peer-priorities", "query,byte"}, nil)
	re.Contains(echo, "query is not allowed to be set in priorities for write-peer-priorities")
	checkHotSchedulerConfig(expected1)

	// test remove and add
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "remove", "balance-hot-region-scheduler"}, nil)
	re.Contains(echo, "Success")
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "add", "balance-hot-region-scheduler"}, nil)
	re.Contains(echo, "Success")

	// test balance leader config
	conf = make(map[string]interface{})
	conf1 := make(map[string]interface{})
	mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-leader-scheduler", "show"}, &conf)
	re.Equal(4., conf["batch"])
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-leader-scheduler", "set", "batch", "3"}, nil)
	re.Contains(echo, "Success!")
	testutil.Eventually(re, func() bool {
		mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-leader-scheduler"}, &conf1)
		return conf1["batch"] == 3.
	})
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "add", "balance-leader-scheduler"}, nil)
	re.NotContains(echo, "Success!")
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "remove", "balance-leader-scheduler"}, nil)
	re.Contains(echo, "Success!")
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "remove", "balance-leader-scheduler"}, nil)
	re.Contains(echo, "404")
	re.Contains(echo, "PD:scheduler:ErrSchedulerNotFound]scheduler not found")
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "balance-leader-scheduler"}, nil)
	re.Contains(echo, "404")
	re.Contains(echo, "scheduler not found")
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "add", "balance-leader-scheduler"}, nil)
	re.Contains(echo, "Success!")

	// test evict-slow-store && evict-slow-trend schedulers config
	evictSlownessSchedulers := []string{"evict-slow-store-scheduler", "evict-slow-trend-scheduler"}
	for _, schedulerName := range evictSlownessSchedulers {
		echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "add", schedulerName}, nil)
		if strings.Contains(echo, "Success!") {
			re.Contains(echo, "Success!")
		} else {
			re.Contains(echo, "scheduler existed")
		}
		testutil.Eventually(re, func() bool {
			echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "show"}, nil)
			return strings.Contains(echo, schedulerName)
		})
		echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", schedulerName, "set", "recovery-duration", "100"}, nil)
		re.Contains(echo, "Success!")
		conf = make(map[string]interface{})
		testutil.Eventually(re, func() bool {
			mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", schedulerName, "show"}, &conf)
			return conf["recovery-duration"] == 100.
		})
		echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "remove", schedulerName}, nil)
		re.Contains(echo, "Success!")
		testutil.Eventually(re, func() bool {
			echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "show"}, nil)
			return !strings.Contains(echo, schedulerName)
		})
	}
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "add", "evict-slow-store-scheduler"}, nil)
	re.Contains(echo, "Success!")

	// test shuffle hot region scheduler
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "add", "shuffle-hot-region-scheduler"}, nil)
	re.Contains(echo, "Success!")
	testutil.Eventually(re, func() bool {
		echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "show"}, nil)
		return strings.Contains(echo, "shuffle-hot-region-scheduler")
	})
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "shuffle-hot-region-scheduler", "set", "limit", "127"}, nil)
	re.Contains(echo, "Success!")
	conf = make(map[string]interface{})
	testutil.Eventually(re, func() bool {
		mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "config", "shuffle-hot-region-scheduler", "show"}, &conf)
		return conf["limit"] == 127.
	})
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "remove", "shuffle-hot-region-scheduler"}, nil)
	re.Contains(echo, "Success!")
	testutil.Eventually(re, func() bool {
		echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "show"}, nil)
		return !strings.Contains(echo, "shuffle-hot-region-scheduler")
	})

	// test show scheduler with paused and disabled status.
	checkSchedulerWithStatusCommand := func(status string, expected []string) {
		testutil.Eventually(re, func() bool {
			var schedulers []string
			mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "show", "--status", status}, &schedulers)
			return reflect.DeepEqual(expected, schedulers)
		})
	}

	mustUsage([]string{"-u", pdAddr, "scheduler", "pause", "balance-leader-scheduler"})
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "pause", "balance-leader-scheduler", "60"}, nil)
	re.Contains(echo, "Success!")
	checkSchedulerWithStatusCommand("paused", []string{
		"balance-leader-scheduler",
	})
	result := make(map[string]interface{})
	testutil.Eventually(re, func() bool {
		mightExec(re, cmd, []string{"-u", pdAddr, "scheduler", "describe", "balance-leader-scheduler"}, &result)
		return len(result) != 0 && result["status"] == "paused" && result["summary"] == ""
	}, testutil.WithWaitFor(30*time.Second))

	mustUsage([]string{"-u", pdAddr, "scheduler", "resume", "balance-leader-scheduler", "60"})
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "resume", "balance-leader-scheduler"}, nil)
	re.Contains(echo, "Success!")
	checkSchedulerWithStatusCommand("paused", []string{})

	// set label scheduler to disabled manually.
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "add", "label-scheduler"}, nil)
	re.Contains(echo, "Success!")
	cfg := leaderServer.GetServer().GetScheduleConfig()
	origin := cfg.Schedulers
	cfg.Schedulers = sc.SchedulerConfigs{{Type: "label", Disable: true}}
	err := leaderServer.GetServer().SetScheduleConfig(*cfg)
	re.NoError(err)
	checkSchedulerWithStatusCommand("disabled", []string{"label-scheduler"})
	// reset Schedulers in ScheduleConfig
	cfg.Schedulers = origin
	err = leaderServer.GetServer().SetScheduleConfig(*cfg)
	re.NoError(err)
	checkSchedulerWithStatusCommand("disabled", []string{})
}

func (suite *schedulerTestSuite) TestSchedulerDiagnostic() {
	suite.env.RunTestInTwoModes(suite.checkSchedulerDiagnostic)
}

func (suite *schedulerTestSuite) checkSchedulerDiagnostic(cluster *pdTests.TestCluster) {
	re := suite.Require()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := ctl.GetRootCmd()

	checkSchedulerDescribeCommand := func(schedulerName, expectedStatus, expectedSummary string) {
		result := make(map[string]interface{})
		testutil.Eventually(re, func() bool {
			mightExec(re, cmd, []string{"-u", pdAddr, "scheduler", "describe", schedulerName}, &result)
			return len(result) != 0 && expectedStatus == result["status"] && expectedSummary == result["summary"]
		}, testutil.WithTickInterval(50*time.Millisecond))
	}

	stores := []*metapb.Store{
		{
			Id:            1,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            2,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            3,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
		{
			Id:            4,
			State:         metapb.StoreState_Up,
			LastHeartbeat: time.Now().UnixNano(),
		},
	}
	for _, store := range stores {
		pdTests.MustPutStore(re, cluster, store)
	}

	// note: because pdqsort is an unstable sort algorithm, set ApproximateSize for this region.
	pdTests.MustPutRegion(re, cluster, 1, 1, []byte("a"), []byte("b"), core.SetApproximateSize(10))

	echo := mustExec(re, cmd, []string{"-u", pdAddr, "config", "set", "enable-diagnostic", "true"}, nil)
	re.Contains(echo, "Success!")
	checkSchedulerDescribeCommand("balance-region-scheduler", "pending", "1 store(s) RegionNotMatchRule; ")

	// scheduler delete command
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"}, nil)
	re.Contains(echo, "Success!")
	checkSchedulerDescribeCommand("balance-region-scheduler", "disabled", "")

	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "pause", "balance-leader-scheduler", "60"}, nil)
	re.Contains(echo, "Success!")
	echo = mustExec(re, cmd, []string{"-u", pdAddr, "scheduler", "resume", "balance-leader-scheduler"}, nil)
	re.Contains(echo, "Success!")
	checkSchedulerDescribeCommand("balance-leader-scheduler", "normal", "")
}

func mustExec(re *require.Assertions, cmd *cobra.Command, args []string, v interface{}) string {
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	if v == nil {
		return string(output)
	}
	re.NoError(json.Unmarshal(output, v), string(output))
	return ""
}

func mightExec(re *require.Assertions, cmd *cobra.Command, args []string, v interface{}) {
	output, err := tests.ExecuteCommand(cmd, args...)
	re.NoError(err)
	if v == nil {
		return
	}
	json.Unmarshal(output, v)
}
