// Copyright 2021 TiKV Project Authors.
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
	"encoding/json"
	"fmt"
	"net/url"
	"sort"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/apiutil"
	tu "github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/schedule/labeler"
)

var _ = Suite(&testRegionLabelSuite{})

type testRegionLabelSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testRegionLabelSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/config/region-label/", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testRegionLabelSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testRegionLabelSuite) TestGetSet(c *C) {
	var resp []*labeler.LabelRule
	err := tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"rules", &resp)
	c.Assert(err, IsNil)
	c.Assert(resp, HasLen, 0)

	rules := []*labeler.LabelRule{
		{ID: "rule1", Labels: []labeler.RegionLabel{{Key: "k1", Value: "v1"}}, RuleType: "key-range", Data: makeKeyRanges("1234", "5678")},
		{ID: "rule2/a/b", Labels: []labeler.RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: makeKeyRanges("ab12", "cd12")},
		{ID: "rule3", Labels: []labeler.RegionLabel{{Key: "k3", Value: "v3"}}, RuleType: "key-range", Data: makeKeyRanges("abcd", "efef")},
	}
	ruleIDs := []string{"rule1", "rule2/a/b", "rule3"}
	for _, rule := range rules {
		data, _ := json.Marshal(rule)
		err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"rule", data, tu.StatusOK(c))
		c.Assert(err, IsNil)
	}
	for i, id := range ruleIDs {
		var rule labeler.LabelRule
		err = tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"rule/"+url.QueryEscape(id), &rule)
		c.Assert(err, IsNil)
		c.Assert(&rule, DeepEquals, rules[i])
	}

	err = tu.ReadGetJSONWithBody(c, testDialClient, s.urlPrefix+"rules/ids", []byte(`["rule1", "rule3"]`), &resp)
	c.Assert(err, IsNil)
	expects := []*labeler.LabelRule{rules[0], rules[2]}
	c.Assert(resp, DeepEquals, expects)

	_, err = apiutil.DoDelete(testDialClient, s.urlPrefix+"rule/"+url.QueryEscape("rule2/a/b"))
	c.Assert(err, IsNil)
	err = tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"rules", &resp)
	c.Assert(err, IsNil)
	sort.Slice(resp, func(i, j int) bool { return resp[i].ID < resp[j].ID })
	c.Assert(resp, DeepEquals, []*labeler.LabelRule{rules[0], rules[2]})

	patch := labeler.LabelRulePatch{
		SetRules: []*labeler.LabelRule{
			{ID: "rule2/a/b", Labels: []labeler.RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: makeKeyRanges("ab12", "cd12")},
		},
		DeleteRules: []string{"rule1"},
	}
	data, _ := json.Marshal(patch)
	err = tu.CheckPatchJSON(testDialClient, s.urlPrefix+"rules", data, tu.StatusOK(c))
	c.Assert(err, IsNil)
	err = tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"rules", &resp)
	c.Assert(err, IsNil)
	sort.Slice(resp, func(i, j int) bool { return resp[i].ID < resp[j].ID })
	c.Assert(resp, DeepEquals, []*labeler.LabelRule{rules[1], rules[2]})
}

func makeKeyRanges(keys ...string) []interface{} {
	var res []interface{}
	for i := 0; i < len(keys); i += 2 {
		res = append(res, map[string]interface{}{"start_key": keys[i], "end_key": keys[i+1]})
	}
	return res
}
