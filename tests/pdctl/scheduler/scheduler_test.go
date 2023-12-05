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

package scheduler_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&schedulerTestSuite{})

type schedulerTestSuite struct {
	context context.Context
	cancel  context.CancelFunc
}

func (s *schedulerTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
	s.context, s.cancel = context.WithCancel(context.Background())
}

func (s *schedulerTestSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *schedulerTestSuite) TestScheduler(c *C) {
	cluster, err := tests.NewTestCluster(s.context, 1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

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

	mustExec := func(args []string, v interface{}) string {
		output, err := pdctl.ExecuteCommand(cmd, args...)
		c.Assert(err, IsNil)
		if v == nil {
			return string(output)
		}
		c.Assert(json.Unmarshal(output, v), IsNil)
		return ""
	}

	checkSchedulerCommand := func(args []string, expected map[string]bool) {
		if args != nil {
			mustExec(args, nil)
		}
		var schedulers []string
		mustExec([]string{"-u", pdAddr, "scheduler", "show"}, &schedulers)
		for _, scheduler := range schedulers {
			c.Assert(expected[scheduler], Equals, true)
		}
	}

	checkSchedulerConfigCommand := func(args []string, expectedConfig map[string]interface{}, schedulerName string) {
		if args != nil {
			mustExec(args, nil)
		}
		configInfo := make(map[string]interface{})
		mustExec([]string{"-u", pdAddr, "scheduler", "config", schedulerName}, &configInfo)
		c.Assert(expectedConfig, DeepEquals, configInfo)
	}

	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	for _, store := range stores {
		pdctl.MustPutStore(c, leaderServer.GetServer(), store)
	}

	pdctl.MustPutRegion(c, cluster, 1, 1, []byte("a"), []byte("b"))
	defer cluster.Destroy()

	time.Sleep(3 * time.Second)

	// scheduler show command
	expected := map[string]bool{
		"balance-region-scheduler":     true,
		"balance-leader-scheduler":     true,
		"balance-hot-region-scheduler": true,
		"label-scheduler":              true,
	}
	checkSchedulerCommand(nil, expected)

	// scheduler delete command
	args := []string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"}
	expected = map[string]bool{
		"balance-leader-scheduler":     true,
		"balance-hot-region-scheduler": true,
		"label-scheduler":              true,
	}
	checkSchedulerCommand(args, expected)

	schedulers := []string{"evict-leader-scheduler", "grant-leader-scheduler"}

	for idx := range schedulers {
		// scheduler add command
		args = []string{"-u", pdAddr, "scheduler", "add", schedulers[idx], "2"}
		expected = map[string]bool{
			"balance-leader-scheduler":     true,
			"balance-hot-region-scheduler": true,
			"label-scheduler":              true,
			schedulers[idx]:                true,
		}
		checkSchedulerCommand(args, expected)

		// scheduler config show command
		expectedConfig := make(map[string]interface{})
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(nil, expectedConfig, schedulers[idx])

		// scheduler config update command
		args = []string{"-u", pdAddr, "scheduler", "config", schedulers[idx], "add-store", "3"}
		expected = map[string]bool{
			"balance-leader-scheduler":     true,
			"balance-hot-region-scheduler": true,
			"label-scheduler":              true,
			schedulers[idx]:                true,
		}
		checkSchedulerCommand(args, expected)

		// check update success
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}, "3": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(nil, expectedConfig, schedulers[idx])

		// scheduler delete command
		args = []string{"-u", pdAddr, "scheduler", "remove", schedulers[idx]}
		expected = map[string]bool{
			"balance-leader-scheduler":     true,
			"balance-hot-region-scheduler": true,
			"label-scheduler":              true,
		}
		checkSchedulerCommand(args, expected)

		// check the compactily
		// scheduler add command
		args = []string{"-u", pdAddr, "scheduler", "add", schedulers[idx], "2"}
		expected = map[string]bool{
			"balance-leader-scheduler":     true,
			"balance-hot-region-scheduler": true,
			"label-scheduler":              true,
			schedulers[idx]:                true,
		}
		checkSchedulerCommand(args, expected)

		// scheduler add command twice
		args = []string{"-u", pdAddr, "scheduler", "add", schedulers[idx], "4"}
		expected = map[string]bool{
			"balance-leader-scheduler":     true,
			"balance-hot-region-scheduler": true,
			"label-scheduler":              true,
			schedulers[idx]:                true,
		}
		checkSchedulerCommand(args, expected)

		// check add success
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}, "4": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(nil, expectedConfig, schedulers[idx])

		// scheduler remove command [old]
		args = []string{"-u", pdAddr, "scheduler", "remove", schedulers[idx] + "-4"}
		expected = map[string]bool{
			"balance-leader-scheduler":     true,
			"balance-hot-region-scheduler": true,
			"label-scheduler":              true,
			schedulers[idx]:                true,
		}
		checkSchedulerCommand(args, expected)

		// check remove success
		expectedConfig["store-id-ranges"] = map[string]interface{}{"2": []interface{}{map[string]interface{}{"end-key": "", "start-key": ""}}}
		checkSchedulerConfigCommand(nil, expectedConfig, schedulers[idx])

		// scheduler remove command, when remove the last store, it should remove whole scheduler
		args = []string{"-u", pdAddr, "scheduler", "remove", schedulers[idx] + "-2"}
		expected = map[string]bool{
			"balance-leader-scheduler":     true,
			"balance-hot-region-scheduler": true,
			"label-scheduler":              true,
		}
		checkSchedulerCommand(args, expected)

	}

	// test shuffle region config
	checkSchedulerCommand([]string{"-u", pdAddr, "scheduler", "add", "shuffle-region-scheduler"}, map[string]bool{
		"balance-leader-scheduler":     true,
		"balance-hot-region-scheduler": true,
		"label-scheduler":              true,
		"shuffle-region-scheduler":     true,
	})
	var roles []string
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "shuffle-region-scheduler", "show-roles"}, &roles)
	c.Assert(roles, DeepEquals, []string{"leader", "follower", "learner"})
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "shuffle-region-scheduler", "set-roles", "learner"}, nil)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "shuffle-region-scheduler", "show-roles"}, &roles)
	c.Assert(roles, DeepEquals, []string{"learner"})
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "shuffle-region-scheduler"}, &roles)
	c.Assert(roles, DeepEquals, []string{"learner"})

	// test balance region config
	echo := mustExec([]string{"-u", pdAddr, "scheduler", "add", "balance-region-scheduler"}, nil)
	c.Assert(strings.Contains(echo, "Success!"), IsTrue)
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"}, nil)
	c.Assert(strings.Contains(echo, "Success!"), IsTrue)
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "remove", "balance-region-scheduler"}, nil)
	c.Assert(strings.Contains(echo, "Success!"), IsFalse)
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "add", "evict-leader-scheduler", "1"}, nil)
	c.Assert(strings.Contains(echo, "Success!"), IsTrue)
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "remove", "evict-leader-scheduler-1"}, nil)
	c.Assert(strings.Contains(echo, "Success!"), IsTrue)
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "remove", "evict-leader-scheduler-1"}, nil)
	c.Assert(strings.Contains(echo, "404"), IsTrue)

	// test hot region config
	echo = mustExec([]string{"-u", pdAddr, "scheduler", "config", "evict-leader-scheduler"}, nil)
	c.Assert(strings.Contains(echo, "[404] scheduler not found"), IsTrue)
	var conf map[string]interface{}
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "list"}, &conf)
	expected1 := map[string]interface{}{
		"min-hot-byte-rate":         float64(100),
		"min-hot-key-rate":          float64(10),
		"max-zombie-rounds":         float64(3),
		"max-peer-number":           float64(1000),
		"byte-rate-rank-step-ratio": 0.05,
		"key-rate-rank-step-ratio":  0.05,
		"count-rank-step-ratio":     0.01,
		"great-dec-ratio":           0.95,
		"minor-dec-ratio":           0.99,
		"src-tolerance-ratio":       1.05,
		"dst-tolerance-ratio":       1.05,
	}
	c.Assert(conf, DeepEquals, expected1)
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler", "set", "src-tolerance-ratio", "1.02"}, nil)
	expected1["src-tolerance-ratio"] = 1.02
	var conf1 map[string]interface{}
	mustExec([]string{"-u", pdAddr, "scheduler", "config", "balance-hot-region-scheduler"}, &conf1)
	c.Assert(conf1, DeepEquals, expected1)

	// test show scheduler with paused and disabled status.
	checkSchedulerWithStatusCommand := func(args []string, status string, expected []string) {
		if args != nil {
			mustExec(args, nil)
		}
		var schedulers []string
		mustExec([]string{"-u", pdAddr, "scheduler", "show", "--status", status}, &schedulers)
		c.Assert(schedulers, DeepEquals, expected)
	}

	mustExec([]string{"-u", pdAddr, "scheduler", "pause", "balance-leader-scheduler", "60"}, nil)
	checkSchedulerWithStatusCommand(nil, "paused", []string{
		"balance-leader-scheduler",
	})
	mustExec([]string{"-u", pdAddr, "scheduler", "resume", "balance-leader-scheduler"}, nil)
	checkSchedulerWithStatusCommand(nil, "paused", nil)

	// set label scheduler to disabled manually.
	cfg := leaderServer.GetServer().GetScheduleConfig()
	origin := cfg.Schedulers
	cfg.Schedulers = config.SchedulerConfigs{{Type: "label", Disable: true}}
	err = leaderServer.GetServer().SetScheduleConfig(*cfg)
	c.Assert(err, IsNil)
	checkSchedulerWithStatusCommand(nil, "disabled", []string{"label-scheduler"})
	// reset Schedulers in ScheduleConfig
	cfg.Schedulers = origin
	err = leaderServer.GetServer().SetScheduleConfig(*cfg)
	c.Assert(err, IsNil)
	checkSchedulerWithStatusCommand(nil, "disabled", nil)
}
