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

package log_test

import (
	"context"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&logTestSuite{})

type logTestSuite struct{}

func (s *logTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *logTestSuite) TestLog(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster, err := tests.NewTestCluster(ctx, 1)
	c.Assert(err, IsNil)
	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pdAddr := cluster.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()

	store := &metapb.Store{
		Id:            1,
		State:         metapb.StoreState_Up,
		LastHeartbeat: time.Now().UnixNano(),
	}
	leaderServer := cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	svr := leaderServer.GetServer()
	pdctl.MustPutStore(c, svr, store)
	defer cluster.Destroy()

	var testCases = []struct {
		cmd    []string
		expect string
	}{
		// log [fatal|error|warn|info|debug]
		{
			cmd:    []string{"-u", pdAddr, "log", "fatal"},
			expect: "fatal",
		},
		{
			cmd:    []string{"-u", pdAddr, "log", "error"},
			expect: "error",
		},
		{
			cmd:    []string{"-u", pdAddr, "log", "warn"},
			expect: "warn",
		},
		{
			cmd:    []string{"-u", pdAddr, "log", "info"},
			expect: "info",
		},
		{
			cmd:    []string{"-u", pdAddr, "log", "debug"},
			expect: "debug",
		},
	}

	for _, testCase := range testCases {
		_, err = pdctl.ExecuteCommand(cmd, testCase.cmd...)
		c.Assert(err, IsNil)
		c.Assert(svr.GetConfig().Log.Level, Equals, testCase.expect)
	}
}
