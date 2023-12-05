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
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"encoding/json"
	"io"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/server/versioninfo"
)

var _ = Suite(&testStatusAPISuite{})

type testStatusAPISuite struct{}

func checkStatusResponse(c *C, body []byte) {
	got := status{}
	c.Assert(json.Unmarshal(body, &got), IsNil)
	c.Assert(got.BuildTS, Equals, versioninfo.PDBuildTS)
	c.Assert(got.GitHash, Equals, versioninfo.PDGitHash)
	c.Assert(got.Version, Equals, versioninfo.PDReleaseVersion)
}

func (s *testStatusAPISuite) TestStatus(c *C) {
	cfgs, _, clean := mustNewCluster(c, 3)
	defer clean()

	for _, cfg := range cfgs {
		addr := cfg.ClientUrls + apiPrefix + "/api/v1/status"
		resp, err := testDialClient.Get(addr)
		c.Assert(err, IsNil)
		buf, err := io.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		checkStatusResponse(c, buf)
		resp.Body.Close()
	}
}
