// Copyright 2016 TiKV Project Authors.
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

package join

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
)

func TestJoin(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testJoinServerSuite{})

type testJoinServerSuite struct{}

// A PD joins itself.
func (s *testJoinServerSuite) TestPDJoinsItself(c *C) {
	cfg := server.NewTestSingleConfig(c)
	defer testutil.CleanServer(cfg.DataDir)
	cfg.Join = cfg.AdvertiseClientUrls
	c.Assert(PrepareJoinCluster(cfg), NotNil)
}
