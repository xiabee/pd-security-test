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

package health_test

import (
	"context"
	"encoding/json"
	"testing"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/api"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/pdctl"
	pdctlCmd "github.com/tikv/pd/tools/pd-ctl/pdctl"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&healthTestSuite{})

type healthTestSuite struct{}

func (s *healthTestSuite) SetUpSuite(c *C) {
	server.EnableZap = true
}

func (s *healthTestSuite) TestHealth(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc, err := tests.NewTestCluster(ctx, 3)
	c.Assert(err, IsNil)
	err = tc.RunInitialServers()
	c.Assert(err, IsNil)
	tc.WaitLeader()
	leaderServer := tc.GetServer(tc.GetLeader())
	c.Assert(leaderServer.BootstrapCluster(), IsNil)
	pdAddr := tc.GetConfig().GetClientURL()
	cmd := pdctlCmd.GetRootCmd()
	defer tc.Destroy()

	client := tc.GetEtcdClient()
	members, err := cluster.GetMembers(client)
	c.Assert(err, IsNil)
	healthMembers := cluster.CheckHealth(tc.GetHTTPClient(), members)
	healths := []api.Health{}
	for _, member := range members {
		h := api.Health{
			Name:       member.Name,
			MemberID:   member.MemberId,
			ClientUrls: member.ClientUrls,
			Health:     false,
		}
		if _, ok := healthMembers[member.GetMemberId()]; ok {
			h.Health = true
		}
		healths = append(healths, h)
	}

	// health command
	args := []string{"-u", pdAddr, "health"}
	output, err := pdctl.ExecuteCommand(cmd, args...)
	c.Assert(err, IsNil)
	h := make([]api.Health, len(healths))
	c.Assert(json.Unmarshal(output, &h), IsNil)
	c.Assert(err, IsNil)
	c.Assert(h, DeepEquals, healths)
}
