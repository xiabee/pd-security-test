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

package api

import (
	"encoding/json"
	"net/http"
	"strings"

	. "github.com/pingcap/check"
)

var _ = Suite(&testEtcdAPISuite{})

type testEtcdAPISuite struct{}

func (s *testEtcdAPISuite) TestGRPCGateway(c *C) {
	svr, clean := mustNewServer(c)
	defer clean()

	addr := svr.GetConfig().ClientUrls + "/v3/kv/put"
	putKey := map[string]string{"key": "Zm9v", "value": "YmFy"}
	v, _ := json.Marshal(putKey)
	err := postJSON(testDialClient, addr, v)
	c.Assert(err, IsNil)
	addr = svr.GetConfig().ClientUrls + "/v3/kv/range"
	getKey := map[string]string{"key": "Zm9v"}
	v, _ = json.Marshal(getKey)
	err = postJSON(testDialClient, addr, v, func(res []byte, code int) {
		c.Assert(strings.Contains(string(res), "Zm9v"), IsTrue)
		c.Assert(code, Equals, http.StatusOK)
	})
	c.Assert(err, IsNil)
}
