// Copyright 2020 TiKV Project Authors.
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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	. "github.com/pingcap/check"
	"github.com/tikv/pd/pkg/apiutil"
	tu "github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/schedule/placement"
)

var _ = Suite(&testRuleSuite{})

type testRuleSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testRuleSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/config", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
	PDServerCfg := s.svr.GetConfig().PDServerCfg
	PDServerCfg.KeyType = "raw"
	err := s.svr.SetPDServerConfig(PDServerCfg)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, s.urlPrefix, []byte(`{"enable-placement-rules":"true"}`), tu.StatusOK(c)), IsNil)
}

func (s *testRuleSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testRuleSuite) TearDownTest(c *C) {
	def := placement.GroupBundle{
		ID: "pd",
		Rules: []*placement.Rule{
			{GroupID: "pd", ID: "default", Role: "voter", Count: 3},
		},
	}
	data, err := json.Marshal([]placement.GroupBundle{def})
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/placement-rule", data, tu.StatusOK(c))
	c.Assert(err, IsNil)
}

func (s *testRuleSuite) TestSet(c *C) {
	rule := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	successData, err := json.Marshal(rule)
	c.Assert(err, IsNil)
	oldStartKey, err := hex.DecodeString(rule.StartKeyHex)
	c.Assert(err, IsNil)
	oldEndKey, err := hex.DecodeString(rule.EndKeyHex)
	c.Assert(err, IsNil)
	parseErrData := []byte("foo")
	rule1 := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "XXXX", EndKeyHex: "3333", Role: "voter", Count: 1}
	checkErrData, err := json.Marshal(rule1)
	c.Assert(err, IsNil)
	rule2 := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: -1}
	setErrData, err := json.Marshal(rule2)
	c.Assert(err, IsNil)
	rule3 := placement.Rule{GroupID: "a", ID: "10", StartKeyHex: "1111", EndKeyHex: "3333", Role: "follower", Count: 3}
	updateData, err := json.Marshal(rule3)
	c.Assert(err, IsNil)
	newStartKey, err := hex.DecodeString(rule.StartKeyHex)
	c.Assert(err, IsNil)
	newEndKey, err := hex.DecodeString(rule.EndKeyHex)
	c.Assert(err, IsNil)

	testcases := []struct {
		name        string
		rawData     []byte
		success     bool
		response    string
		popKeyRange map[string]struct{}
	}{
		{
			name:     "Set a new rule success",
			rawData:  successData,
			success:  true,
			response: "",
			popKeyRange: map[string]struct{}{
				hex.EncodeToString(oldStartKey): {},
				hex.EncodeToString(oldEndKey):   {},
			},
		},
		{
			name:     "Update an existed rule success",
			rawData:  updateData,
			success:  true,
			response: "",
			popKeyRange: map[string]struct{}{
				hex.EncodeToString(oldStartKey): {},
				hex.EncodeToString(oldEndKey):   {},
				hex.EncodeToString(newStartKey): {},
				hex.EncodeToString(newEndKey):   {},
			},
		},
		{
			name:    "Parse Json failed",
			rawData: parseErrData,
			success: false,
			response: `{
  "code": "input",
  "msg": "invalid character 'o' in literal false (expecting 'a')",
  "data": {
    "Offset": 2
  }
}
`,
		},
		{
			name:    "Check rule failed",
			rawData: checkErrData,
			success: false,
			response: `"[PD:hex:ErrHexDecodingString]decode string XXXX error"
`,
		},
		{
			name:    "Set Rule Failed",
			rawData: setErrData,
			success: false,
			response: `"[PD:placement:ErrRuleContent]invalid rule content, invalid count -1"
`,
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)
		// clear suspect keyRanges to prevent test case from others
		s.svr.GetRaftCluster().ClearSuspectKeyRanges()
		if testcase.success {
			err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/rule", testcase.rawData, tu.StatusOK(c))
			popKeyRangeMap := map[string]struct{}{}
			for i := 0; i < len(testcase.popKeyRange)/2; i++ {
				v, got := s.svr.GetRaftCluster().PopOneSuspectKeyRange()
				c.Assert(got, IsTrue)
				popKeyRangeMap[hex.EncodeToString(v[0])] = struct{}{}
				popKeyRangeMap[hex.EncodeToString(v[1])] = struct{}{}
			}
			c.Assert(len(popKeyRangeMap), Equals, len(testcase.popKeyRange))
			for k := range popKeyRangeMap {
				_, ok := testcase.popKeyRange[k]
				c.Assert(ok, IsTrue)
			}
		} else {
			err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/rule", testcase.rawData,
				tu.StatusNotOK(c),
				tu.StringEqual(c, testcase.response))
		}
		c.Assert(err, IsNil)
	}
}

func (s *testRuleSuite) TestGet(c *C) {
	rule := placement.Rule{GroupID: "a", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/rule", data, tu.StatusOK(c))
	c.Assert(err, IsNil)

	testcases := []struct {
		name  string
		rule  placement.Rule
		found bool
		code  int
	}{
		{
			name:  "found",
			rule:  rule,
			found: true,
			code:  200,
		},
		{
			name:  "not found",
			rule:  placement.Rule{GroupID: "a", ID: "30", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1},
			found: false,
			code:  404,
		},
	}
	for _, testcase := range testcases {
		c.Log(testcase.name)
		var resp placement.Rule
		url := fmt.Sprintf("%s/rule/%s/%s", s.urlPrefix, testcase.rule.GroupID, testcase.rule.ID)
		if testcase.found {
			err = tu.ReadGetJSON(c, testDialClient, url, &resp)
			compareRule(c, &resp, &testcase.rule)
		} else {
			err = tu.CheckGetJSON(testDialClient, url, nil, tu.Status(c, testcase.code))
		}
		c.Assert(err, IsNil)
	}
}

func (s *testRuleSuite) TestGetAll(c *C) {
	rule := placement.Rule{GroupID: "b", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/rule", data, tu.StatusOK(c))
	c.Assert(err, IsNil)

	var resp2 []*placement.Rule
	err = tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"/rules", &resp2)
	c.Assert(err, IsNil)
	c.Assert(len(resp2), GreaterEqual, 1)
}

func (s *testRuleSuite) TestSetAll(c *C) {
	rule1 := placement.Rule{GroupID: "a", ID: "12", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	rule2 := placement.Rule{GroupID: "b", ID: "12", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	rule3 := placement.Rule{GroupID: "a", ID: "12", StartKeyHex: "XXXX", EndKeyHex: "3333", Role: "voter", Count: 1}
	rule4 := placement.Rule{GroupID: "a", ID: "12", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: -1}
	rule5 := placement.Rule{GroupID: "pd", ID: "default", StartKeyHex: "", EndKeyHex: "", Role: "voter", Count: 1,
		LocationLabels: []string{"host"}}
	rule6 := placement.Rule{GroupID: "pd", ID: "default", StartKeyHex: "", EndKeyHex: "", Role: "voter", Count: 3}

	s.svr.GetPersistOptions().GetReplicationConfig().LocationLabels = []string{"host"}
	defaultRule := s.svr.GetRaftCluster().GetRuleManager().GetRule("pd", "default")
	defaultRule.LocationLabels = []string{"host"}
	s.svr.GetRaftCluster().GetRuleManager().SetRule(defaultRule)

	successData, err := json.Marshal([]*placement.Rule{&rule1, &rule2})
	c.Assert(err, IsNil)

	checkErrData, err := json.Marshal([]*placement.Rule{&rule1, &rule3})
	c.Assert(err, IsNil)

	setErrData, err := json.Marshal([]*placement.Rule{&rule1, &rule4})
	c.Assert(err, IsNil)

	defaultData, err := json.Marshal([]*placement.Rule{&rule1, &rule5})
	c.Assert(err, IsNil)

	recoverData, err := json.Marshal([]*placement.Rule{&rule1, &rule6})
	c.Assert(err, IsNil)

	testcases := []struct {
		name          string
		rawData       []byte
		success       bool
		response      string
		isDefaultRule bool
		count         int
	}{
		{
			name:          "Set rules successfully, with oldRules full of nil",
			rawData:       successData,
			success:       true,
			response:      "",
			isDefaultRule: false,
		},
		{
			name:          "Parse Json failed",
			rawData:       []byte("foo"),
			success:       false,
			isDefaultRule: false,
			response: `{
  "code": "input",
  "msg": "invalid character 'o' in literal false (expecting 'a')",
  "data": {
    "Offset": 2
  }
}
`,
		},
		{
			name:          "Check rule failed",
			rawData:       checkErrData,
			success:       false,
			isDefaultRule: false,
			response: `"[PD:hex:ErrHexDecodingString]decode string XXXX error"
`,
		},
		{
			name:          "Set Rule Failed",
			rawData:       setErrData,
			success:       false,
			isDefaultRule: false,
			response: `"[PD:placement:ErrRuleContent]invalid rule content, invalid count -1"
`,
		},
		{
			name:          "set default rule",
			rawData:       defaultData,
			success:       true,
			response:      "",
			isDefaultRule: true,
			count:         1,
		},
		{
			name:          "recover default rule",
			rawData:       recoverData,
			success:       true,
			response:      "",
			isDefaultRule: true,
			count:         3,
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)

		if testcase.success {
			err := tu.CheckPostJSON(testDialClient, s.urlPrefix+"/rules", testcase.rawData, tu.StatusOK(c))
			c.Assert(err, IsNil)
			if testcase.isDefaultRule {
				c.Assert(testcase.count, Equals, int(s.svr.GetPersistOptions().GetReplicationConfig().MaxReplicas))
			}
		} else {
			err := tu.CheckPostJSON(testDialClient, s.urlPrefix+"/rules", testcase.rawData,
				tu.StringEqual(c, testcase.response))
			c.Assert(err, IsNil)
		}
	}
}

func (s *testRuleSuite) TestGetAllByGroup(c *C) {
	rule := placement.Rule{GroupID: "c", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/rule", data, tu.StatusOK(c))
	c.Assert(err, IsNil)

	rule1 := placement.Rule{GroupID: "c", ID: "30", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	data, err = json.Marshal(rule1)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/rule", data, tu.StatusOK(c))
	c.Assert(err, IsNil)

	testcases := []struct {
		name    string
		groupID string
		count   int
	}{
		{
			name:    "found group c",
			groupID: "c",
			count:   2,
		},
		{
			name:    "not found d",
			groupID: "d",
			count:   0,
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)
		var resp []*placement.Rule
		url := fmt.Sprintf("%s/rules/group/%s", s.urlPrefix, testcase.groupID)
		err = tu.ReadGetJSON(c, testDialClient, url, &resp)
		c.Assert(err, IsNil)
		c.Assert(resp, HasLen, testcase.count)
		if testcase.count == 2 {
			compareRule(c, resp[0], &rule)
			compareRule(c, resp[1], &rule1)
		}
	}
}

func (s *testRuleSuite) TestGetAllByRegion(c *C) {
	rule := placement.Rule{GroupID: "e", ID: "20", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/rule", data, tu.StatusOK(c))
	c.Assert(err, IsNil)

	r := newTestRegionInfo(4, 1, []byte{0x22, 0x22}, []byte{0x33, 0x33})
	mustRegionHeartbeat(c, s.svr, r)

	testcases := []struct {
		name     string
		regionID string
		success  bool
		code     int
	}{
		{
			name:     "found region",
			regionID: "4",
			success:  true,
		},
		{
			name:     "parse regionId failed",
			regionID: "abc",
			success:  false,
			code:     400,
		},
		{
			name:     "region not found",
			regionID: "5",
			success:  false,
			code:     404,
		},
	}
	for _, testcase := range testcases {
		c.Log(testcase.name)
		var resp []*placement.Rule
		url := fmt.Sprintf("%s/rules/region/%s", s.urlPrefix, testcase.regionID)

		if testcase.success {
			err = tu.ReadGetJSON(c, testDialClient, url, &resp)
			for _, r := range resp {
				if r.GroupID == "e" {
					compareRule(c, r, &rule)
				}
			}
		} else {
			err = tu.CheckGetJSON(testDialClient, url, nil, tu.Status(c, testcase.code))
		}
		c.Assert(err, IsNil)
	}
}

func (s *testRuleSuite) TestGetAllByKey(c *C) {
	rule := placement.Rule{GroupID: "f", ID: "40", StartKeyHex: "8888", EndKeyHex: "9111", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/rule", data, tu.StatusOK(c))
	c.Assert(err, IsNil)

	testcases := []struct {
		name     string
		key      string
		success  bool
		respSize int
		code     int
	}{
		{
			name:     "key in range",
			key:      "8899",
			success:  true,
			respSize: 2,
		},
		{
			name:     "parse key failed",
			key:      "abc",
			success:  false,
			code:     400,
			respSize: 0,
		},
		{
			name:     "key out of range",
			key:      "9999",
			success:  true,
			respSize: 1,
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)
		var resp []*placement.Rule
		url := fmt.Sprintf("%s/rules/key/%s", s.urlPrefix, testcase.key)
		if testcase.success {
			err = tu.ReadGetJSON(c, testDialClient, url, &resp)
			c.Assert(resp, HasLen, testcase.respSize)
		} else {
			err = tu.CheckGetJSON(testDialClient, url, nil, tu.Status(c, testcase.code))
		}
		c.Assert(err, IsNil)
	}
}

func (s *testRuleSuite) TestDelete(c *C) {
	rule := placement.Rule{GroupID: "g", ID: "10", StartKeyHex: "8888", EndKeyHex: "9111", Role: "voter", Count: 1}
	data, err := json.Marshal(rule)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/rule", data, tu.StatusOK(c))
	c.Assert(err, IsNil)
	oldStartKey, err := hex.DecodeString(rule.StartKeyHex)
	c.Assert(err, IsNil)
	oldEndKey, err := hex.DecodeString(rule.EndKeyHex)
	c.Assert(err, IsNil)

	testcases := []struct {
		name        string
		groupID     string
		id          string
		popKeyRange map[string]struct{}
	}{
		{
			name:    "delete existed rule",
			groupID: "g",
			id:      "10",
			popKeyRange: map[string]struct{}{
				hex.EncodeToString(oldStartKey): {},
				hex.EncodeToString(oldEndKey):   {},
			},
		},
		{
			name:        "delete non-existed rule",
			groupID:     "g",
			id:          "15",
			popKeyRange: map[string]struct{}{},
		},
	}
	for _, testcase := range testcases {
		c.Log(testcase.name)
		url := fmt.Sprintf("%s/rule/%s/%s", s.urlPrefix, testcase.groupID, testcase.id)
		// clear suspect keyRanges to prevent test case from others
		s.svr.GetRaftCluster().ClearSuspectKeyRanges()
		statusCode, err := apiutil.DoDelete(testDialClient, url)
		c.Assert(err, IsNil)
		c.Assert(statusCode, Equals, http.StatusOK)
		if len(testcase.popKeyRange) > 0 {
			popKeyRangeMap := map[string]struct{}{}
			for i := 0; i < len(testcase.popKeyRange)/2; i++ {
				v, got := s.svr.GetRaftCluster().PopOneSuspectKeyRange()
				c.Assert(got, IsTrue)
				popKeyRangeMap[hex.EncodeToString(v[0])] = struct{}{}
				popKeyRangeMap[hex.EncodeToString(v[1])] = struct{}{}
			}
			c.Assert(len(popKeyRangeMap), Equals, len(testcase.popKeyRange))
			for k := range popKeyRangeMap {
				_, ok := testcase.popKeyRange[k]
				c.Assert(ok, IsTrue)
			}
		}
	}
}

func compareRule(c *C, r1 *placement.Rule, r2 *placement.Rule) {
	c.Assert(r1.GroupID, Equals, r2.GroupID)
	c.Assert(r1.ID, Equals, r2.ID)
	c.Assert(r1.StartKeyHex, Equals, r2.StartKeyHex)
	c.Assert(r1.EndKeyHex, Equals, r2.EndKeyHex)
	c.Assert(r1.Role, Equals, r2.Role)
	c.Assert(r1.Count, Equals, r2.Count)
}

func (s *testRuleSuite) TestBatch(c *C) {
	opt1 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "a", ID: "13", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1},
	}
	opt2 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "b", ID: "13", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1},
	}
	opt3 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "a", ID: "14", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1},
	}
	opt4 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "a", ID: "15", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: 1},
	}
	opt5 := placement.RuleOp{
		Action: placement.RuleOpDel,
		Rule:   &placement.Rule{GroupID: "a", ID: "14"},
	}
	opt6 := placement.RuleOp{
		Action:           placement.RuleOpDel,
		Rule:             &placement.Rule{GroupID: "b", ID: "1"},
		DeleteByIDPrefix: true,
	}
	opt7 := placement.RuleOp{
		Action: placement.RuleOpDel,
		Rule:   &placement.Rule{GroupID: "a", ID: "1"},
	}
	opt8 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "a", ID: "16", StartKeyHex: "XXXX", EndKeyHex: "3333", Role: "voter", Count: 1},
	}
	opt9 := placement.RuleOp{
		Action: placement.RuleOpAdd,
		Rule:   &placement.Rule{GroupID: "a", ID: "17", StartKeyHex: "1111", EndKeyHex: "3333", Role: "voter", Count: -1},
	}

	successData1, err := json.Marshal([]placement.RuleOp{opt1, opt2, opt3})
	c.Assert(err, IsNil)

	successData2, err := json.Marshal([]placement.RuleOp{opt5, opt7})
	c.Assert(err, IsNil)

	successData3, err := json.Marshal([]placement.RuleOp{opt4, opt6})
	c.Assert(err, IsNil)

	checkErrData, err := json.Marshal([]placement.RuleOp{opt8})
	c.Assert(err, IsNil)

	setErrData, err := json.Marshal([]placement.RuleOp{opt9})
	c.Assert(err, IsNil)

	testcases := []struct {
		name     string
		rawData  []byte
		success  bool
		response string
	}{
		{
			name:     "Batch adds successfully",
			rawData:  successData1,
			success:  true,
			response: "",
		},
		{
			name:     "Batch removes successfully",
			rawData:  successData2,
			success:  true,
			response: "",
		},
		{
			name:     "Batch add and remove successfully",
			rawData:  successData3,
			success:  true,
			response: "",
		},
		{
			name:    "Parse Json failed",
			rawData: []byte("foo"),
			success: false,
			response: `{
  "code": "input",
  "msg": "invalid character 'o' in literal false (expecting 'a')",
  "data": {
    "Offset": 2
  }
}
`,
		},
		{
			name:    "Check rule failed",
			rawData: checkErrData,
			success: false,
			response: `"[PD:hex:ErrHexDecodingString]decode string XXXX error"
`,
		},
		{
			name:    "Set Rule Failed",
			rawData: setErrData,
			success: false,
			response: `"[PD:placement:ErrRuleContent]invalid rule content, invalid count -1"
`,
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)
		if testcase.success {
			err := tu.CheckPostJSON(testDialClient, s.urlPrefix+"/rules/batch", testcase.rawData, tu.StatusOK(c))
			c.Assert(err, IsNil)
		} else {
			err := tu.CheckPostJSON(testDialClient, s.urlPrefix+"/rules/batch", testcase.rawData,
				tu.StatusNotOK(c),
				tu.StringEqual(c, testcase.response))
			c.Assert(err, IsNil)
		}
	}
}

func (s *testRuleSuite) TestBundle(c *C) {
	// GetAll
	b1 := placement.GroupBundle{
		ID: "pd",
		Rules: []*placement.Rule{
			{GroupID: "pd", ID: "default", Role: "voter", Count: 3},
		},
	}
	var bundles []placement.GroupBundle
	err := tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"/placement-rule", &bundles)
	c.Assert(err, IsNil)
	c.Assert(bundles, HasLen, 1)
	compareBundle(c, bundles[0], b1)

	// Set
	b2 := placement.GroupBundle{
		ID:       "foo",
		Index:    42,
		Override: true,
		Rules: []*placement.Rule{
			{GroupID: "foo", ID: "bar", Index: 1, Override: true, Role: "voter", Count: 1},
		},
	}
	data, err := json.Marshal(b2)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/placement-rule/foo", data, tu.StatusOK(c))
	c.Assert(err, IsNil)

	// Get
	var bundle placement.GroupBundle
	err = tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"/placement-rule/foo", &bundle)
	c.Assert(err, IsNil)
	compareBundle(c, bundle, b2)

	// GetAll again
	err = tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"/placement-rule", &bundles)
	c.Assert(err, IsNil)
	c.Assert(bundles, HasLen, 2)
	compareBundle(c, bundles[0], b1)
	compareBundle(c, bundles[1], b2)

	// Delete
	_, err = apiutil.DoDelete(testDialClient, s.urlPrefix+"/placement-rule/pd")
	c.Assert(err, IsNil)

	// GetAll again
	err = tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"/placement-rule", &bundles)
	c.Assert(err, IsNil)
	c.Assert(bundles, HasLen, 1)
	compareBundle(c, bundles[0], b2)

	// SetAll
	b2.Rules = append(b2.Rules, &placement.Rule{GroupID: "foo", ID: "baz", Index: 2, Role: "follower", Count: 1})
	b2.Index, b2.Override = 0, false
	b3 := placement.GroupBundle{ID: "foobar", Index: 100}
	data, err = json.Marshal([]placement.GroupBundle{b1, b2, b3})
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/placement-rule", data, tu.StatusOK(c))
	c.Assert(err, IsNil)

	// GetAll again
	err = tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"/placement-rule", &bundles)
	c.Assert(err, IsNil)
	c.Assert(bundles, HasLen, 3)
	compareBundle(c, bundles[0], b2)
	compareBundle(c, bundles[1], b1)
	compareBundle(c, bundles[2], b3)

	// Delete using regexp
	_, err = apiutil.DoDelete(testDialClient, s.urlPrefix+"/placement-rule/"+url.PathEscape("foo.*")+"?regexp")
	c.Assert(err, IsNil)

	// GetAll again
	err = tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"/placement-rule", &bundles)
	c.Assert(err, IsNil)
	c.Assert(bundles, HasLen, 1)
	compareBundle(c, bundles[0], b1)

	// Set
	id := "rule-without-group-id"
	b4 := placement.GroupBundle{
		Index: 4,
		Rules: []*placement.Rule{
			{ID: "bar", Index: 1, Override: true, Role: "voter", Count: 1},
		},
	}
	data, err = json.Marshal(b4)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/placement-rule/"+id, data, tu.StatusOK(c))
	c.Assert(err, IsNil)

	b4.ID = id
	b4.Rules[0].GroupID = b4.ID

	// Get
	err = tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"/placement-rule/"+id, &bundle)
	c.Assert(err, IsNil)
	compareBundle(c, bundle, b4)

	// GetAll again
	err = tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"/placement-rule", &bundles)
	c.Assert(err, IsNil)
	c.Assert(bundles, HasLen, 2)
	compareBundle(c, bundles[0], b1)
	compareBundle(c, bundles[1], b4)

	// SetAll
	b5 := placement.GroupBundle{
		ID:    "rule-without-group-id-2",
		Index: 5,
		Rules: []*placement.Rule{
			{ID: "bar", Index: 1, Override: true, Role: "voter", Count: 1},
		},
	}
	data, err = json.Marshal([]placement.GroupBundle{b1, b4, b5})
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/placement-rule", data, tu.StatusOK(c))
	c.Assert(err, IsNil)

	b5.Rules[0].GroupID = b5.ID

	// GetAll again
	err = tu.ReadGetJSON(c, testDialClient, s.urlPrefix+"/placement-rule", &bundles)
	c.Assert(err, IsNil)
	c.Assert(bundles, HasLen, 3)
	compareBundle(c, bundles[0], b1)
	compareBundle(c, bundles[1], b4)
	compareBundle(c, bundles[2], b5)
}

func (s *testRuleSuite) TestBundleBadRequest(c *C) {
	testCases := []struct {
		uri  string
		data string
		ok   bool
	}{
		{"/placement-rule/foo", `{"group_id":"foo"}`, true},
		{"/placement-rule/foo", `{"group_id":"bar"}`, false},
		{"/placement-rule/foo", `{"group_id":"foo", "rules": [{"group_id":"foo", "id":"baz", "role":"voter", "count":1}]}`, true},
		{"/placement-rule/foo", `{"group_id":"foo", "rules": [{"group_id":"bar", "id":"baz", "role":"voter", "count":1}]}`, false},
		{"/placement-rule", `[{"group_id":"foo", "rules": [{"group_id":"foo", "id":"baz", "role":"voter", "count":1}]}]`, true},
		{"/placement-rule", `[{"group_id":"foo", "rules": [{"group_id":"bar", "id":"baz", "role":"voter", "count":1}]}]`, false},
	}
	for _, tc := range testCases {
		err := tu.CheckPostJSON(testDialClient, s.urlPrefix+tc.uri, []byte(tc.data),
			func(_ []byte, code int) {
				c.Assert(code == http.StatusOK, Equals, tc.ok)
			})
		c.Assert(err, IsNil)
	}
}

func compareBundle(c *C, b1, b2 placement.GroupBundle) {
	c.Assert(b1.ID, Equals, b2.ID)
	c.Assert(b1.Index, Equals, b2.Index)
	c.Assert(b1.Override, Equals, b2.Override)
	c.Assert(len(b1.Rules), Equals, len(b2.Rules))
	for i := range b1.Rules {
		compareRule(c, b1.Rules[i], b2.Rules[i])
	}
}
