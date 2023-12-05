// Copyright 2018 TiKV Project Authors.
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

package watch_test

import (
	"context"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
)

func Test(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

var _ = Suite(&watchTestSuite{})

type watchTestSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *watchTestSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *watchTestSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *watchTestSuite) TestWatcher(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1, func(conf *config.Config, serverName string) { conf.AutoCompactionRetention = "1s" })
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pd1 := cluster.GetServer(cluster.GetLeader())
	c.Assert(pd1, NotNil)

	pd2, err := cluster.Join(s.ctx)
	c.Assert(err, IsNil)
	err = pd2.Run()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	time.Sleep(5 * time.Second)
	pd3, err := cluster.Join(s.ctx)
	c.Assert(err, IsNil)
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/delayWatcher", `pause`), IsNil)
	err = pd3.Run()
	c.Assert(err, IsNil)
	time.Sleep(200 * time.Millisecond)
	c.Assert(pd3.GetLeader().GetName(), Equals, pd1.GetConfig().Name)
	err = pd1.Stop()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	c.Assert(pd2.GetLeader().GetName(), Equals, pd2.GetConfig().Name)
	failpoint.Disable("github.com/tikv/pd/server/delayWatcher")
	testutil.WaitUntil(c, func(c *C) bool {
		return c.Check(pd3.GetLeader().GetName(), Equals, pd2.GetConfig().Name)
	})
	c.Succeed()
}

func (s *watchTestSuite) TestWatcherCompacted(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1, func(conf *config.Config, serverName string) { conf.AutoCompactionRetention = "1s" })
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()
	pd1 := cluster.GetServer(cluster.GetLeader())
	c.Assert(pd1, NotNil)
	client := pd1.GetEtcdClient()
	_, err = client.Put(context.Background(), "test", "v")
	c.Assert(err, IsNil)
	// wait compaction
	time.Sleep(2 * time.Second)
	pd2, err := cluster.Join(s.ctx)
	c.Assert(err, IsNil)
	err = pd2.Run()
	c.Assert(err, IsNil)
	testutil.WaitUntil(c, func(c *C) bool {
		return c.Check(pd2.GetLeader().GetName(), Equals, pd1.GetConfig().Name)
	})
	c.Succeed()
}
