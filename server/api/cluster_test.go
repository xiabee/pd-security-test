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

package api

import (
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	tu "github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/cluster"
	"github.com/tikv/pd/server/config"
)

var _ = Suite(&testClusterSuite{})

type testClusterSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testClusterSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
}

func (s *testClusterSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testClusterSuite) TestCluster(c *C) {
	// Test get cluster status, and bootstrap cluster
	s.testGetClusterStatus(c)
	s.svr.GetPersistOptions().SetPlacementRuleEnabled(true)
	s.svr.GetPersistOptions().GetReplicationConfig().LocationLabels = []string{"host"}
	rm := s.svr.GetRaftCluster().GetRuleManager()
	rule := rm.GetRule("pd", "default")
	rule.LocationLabels = []string{"host"}
	rule.Count = 1
	rm.SetRule(rule)

	// Test set the config
	url := fmt.Sprintf("%s/cluster", s.urlPrefix)
	c1 := &metapb.Cluster{}
	err := tu.ReadGetJSON(c, testDialClient, url, c1)
	c.Assert(err, IsNil)

	c2 := &metapb.Cluster{}
	r := config.ReplicationConfig{
		MaxReplicas:          6,
		EnablePlacementRules: true,
	}
	c.Assert(s.svr.SetReplicationConfig(r), IsNil)
	err = tu.ReadGetJSON(c, testDialClient, url, c2)
	c.Assert(err, IsNil)

	c1.MaxPeerCount = 6
	c.Assert(c1, DeepEquals, c2)
	c.Assert(int(r.MaxReplicas), Equals, s.svr.GetRaftCluster().GetRuleManager().GetRule("pd", "default").Count)
}

func (s *testClusterSuite) testGetClusterStatus(c *C) {
	url := fmt.Sprintf("%s/cluster/status", s.urlPrefix)
	status := cluster.Status{}
	err := tu.ReadGetJSON(c, testDialClient, url, &status)
	c.Assert(err, IsNil)
	c.Assert(status.RaftBootstrapTime.IsZero(), IsTrue)
	c.Assert(status.IsInitialized, IsFalse)
	now := time.Now()
	mustBootstrapCluster(c, s.svr)
	err = tu.ReadGetJSON(c, testDialClient, url, &status)
	c.Assert(err, IsNil)
	c.Assert(status.RaftBootstrapTime.After(now), IsTrue)
	c.Assert(status.IsInitialized, IsFalse)
	s.svr.SetReplicationConfig(config.ReplicationConfig{MaxReplicas: 1})
	err = tu.ReadGetJSON(c, testDialClient, url, &status)
	c.Assert(err, IsNil)
	c.Assert(status.RaftBootstrapTime.After(now), IsTrue)
	c.Assert(status.IsInitialized, IsTrue)
}
