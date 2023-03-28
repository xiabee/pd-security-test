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

package labeler

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/storage"
	"github.com/tikv/pd/server/storage/endpoint"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testLabelerSuite{})

type testLabelerSuite struct {
	store   endpoint.RuleStorage
	labeler *RegionLabeler
}

func (s *testLabelerSuite) SetUpTest(c *C) {
	s.store = storage.NewStorageWithMemoryBackend()
	var err error
	s.labeler, err = NewRegionLabeler(context.Background(), s.store, time.Millisecond*10)
	c.Assert(err, IsNil)
}

func (s *testLabelerSuite) TestAdjustRule(c *C) {
	rule := LabelRule{
		ID: "foo",
		Labels: []RegionLabel{
			{Key: "k1", Value: "v1"},
		},
		RuleType: "key-range",
		Data:     makeKeyRanges("12abcd", "34cdef", "56abcd", "78cdef"),
	}
	err := rule.checkAndAdjust()
	c.Assert(err, IsNil)
	c.Assert(rule.Data.([]*KeyRangeRule), HasLen, 2)
	c.Assert(rule.Data.([]*KeyRangeRule)[0].StartKey, BytesEquals, []byte{0x12, 0xab, 0xcd})
	c.Assert(rule.Data.([]*KeyRangeRule)[0].EndKey, BytesEquals, []byte{0x34, 0xcd, 0xef})
	c.Assert(rule.Data.([]*KeyRangeRule)[1].StartKey, BytesEquals, []byte{0x56, 0xab, 0xcd})
	c.Assert(rule.Data.([]*KeyRangeRule)[1].EndKey, BytesEquals, []byte{0x78, 0xcd, 0xef})
}

func (s *testLabelerSuite) TestAdjustRule2(c *C) {
	ruleData := `{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":"", "end_key":""}]}`
	var rule LabelRule
	err := json.Unmarshal([]byte(ruleData), &rule)
	c.Assert(err, IsNil)
	err = rule.checkAndAdjust()
	c.Assert(err, IsNil)

	badRuleData := []string{
		// no id
		`{"id":"", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":"", "end_key":""}]}`,
		// no labels
		`{"id":"id", "labels": [], "rule_type":"key-range", "data": [{"start_key":"", "end_key":""}]}`,
		// empty label key
		`{"id":"id", "labels": [{"key": "", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":"", "end_key":""}]}`,
		// empty label value
		`{"id":"id", "labels": [{"key": "k1", "value": ""}], "rule_type":"key-range", "data": [{"start_key":"", "end_key":""}]}`,
		// unknown rule type
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"unknown", "data": [{"start_key":"", "end_key":""}]}`,
		// wrong rule content
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": 123}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": {"start_key":"", "end_key":""}}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": []}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":123, "end_key":""}]}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":"", "end_key":123}]}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":"123", "end_key":"abcd"}]}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":"abcd", "end_key":"123"}]}`,
		`{"id":"id", "labels": [{"key": "k1", "value": "v1"}], "rule_type":"key-range", "data": [{"start_key":"abcd", "end_key":"1234"}]}`,
	}
	for i, str := range badRuleData {
		var rule LabelRule
		err := json.Unmarshal([]byte(str), &rule)
		c.Assert(err, IsNil, Commentf("#%d", i))
		err = rule.checkAndAdjust()
		c.Assert(err, NotNil, Commentf("#%d", i))
	}
}

func (s *testLabelerSuite) TestGetSetRule(c *C) {
	rules := []*LabelRule{
		{ID: "rule1", Labels: []RegionLabel{{Key: "k1", Value: "v1"}}, RuleType: "key-range", Data: makeKeyRanges("1234", "5678")},
		{ID: "rule2", Labels: []RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: makeKeyRanges("ab12", "cd12")},
		{ID: "rule3", Labels: []RegionLabel{{Key: "k3", Value: "v3"}}, RuleType: "key-range", Data: makeKeyRanges("abcd", "efef")},
	}
	for _, r := range rules {
		err := s.labeler.SetLabelRule(r)
		c.Assert(err, IsNil)
	}

	allRules := s.labeler.GetAllLabelRules()
	sort.Slice(allRules, func(i, j int) bool { return allRules[i].ID < allRules[j].ID })
	c.Assert(allRules, DeepEquals, rules)

	byIDs, err := s.labeler.GetLabelRules([]string{"rule3", "rule1"})
	c.Assert(err, IsNil)
	c.Assert(byIDs, DeepEquals, []*LabelRule{rules[2], rules[0]})

	err = s.labeler.DeleteLabelRule("rule2")
	c.Assert(err, IsNil)
	c.Assert(s.labeler.GetLabelRule("rule2"), IsNil)
	byIDs, err = s.labeler.GetLabelRules([]string{"rule1", "rule2"})
	c.Assert(err, IsNil)
	c.Assert(byIDs, DeepEquals, []*LabelRule{rules[0]})

	// patch
	patch := LabelRulePatch{
		SetRules: []*LabelRule{
			{ID: "rule2", Labels: []RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: makeKeyRanges("ab12", "cd12")},
		},
		DeleteRules: []string{"rule1"},
	}
	err = s.labeler.Patch(patch)
	c.Assert(err, IsNil)
	allRules = s.labeler.GetAllLabelRules()
	sort.Slice(allRules, func(i, j int) bool { return allRules[i].ID < allRules[j].ID })
	for id, rule := range allRules {
		expectSameRules(c, rule, rules[id+1])
	}
}

func (s *testLabelerSuite) TestIndex(c *C) {
	rules := []*LabelRule{
		{ID: "rule0", Labels: []RegionLabel{{Key: "k1", Value: "v0"}}, RuleType: "key-range", Data: makeKeyRanges("", "")},
		{ID: "rule1", Index: 1, Labels: []RegionLabel{{Key: "k1", Value: "v1"}}, RuleType: "key-range", Data: makeKeyRanges("1234", "5678")},
		{ID: "rule2", Index: 2, Labels: []RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: makeKeyRanges("ab12", "cd12")},
		{ID: "rule3", Index: 1, Labels: []RegionLabel{{Key: "k2", Value: "v3"}}, RuleType: "key-range", Data: makeKeyRanges("abcd", "efef")},
	}
	for _, r := range rules {
		err := s.labeler.SetLabelRule(r)
		c.Assert(err, IsNil)
	}

	type testCase struct {
		start, end string
		labels     map[string]string
	}
	testCases := []testCase{
		{"", "1234", map[string]string{"k1": "v0"}},
		{"1234", "5678", map[string]string{"k1": "v1"}},
		{"ab12", "abcd", map[string]string{"k1": "v0", "k2": "v2"}},
		{"abcd", "cd12", map[string]string{"k1": "v0", "k2": "v2"}},
		{"cdef", "efef", map[string]string{"k1": "v0", "k2": "v3"}},
	}
	for _, tc := range testCases {
		start, _ := hex.DecodeString(tc.start)
		end, _ := hex.DecodeString(tc.end)
		region := core.NewTestRegionInfo(start, end)
		labels := s.labeler.GetRegionLabels(region)
		c.Assert(labels, HasLen, len(tc.labels))
		for _, l := range labels {
			c.Assert(l.Value, Equals, tc.labels[l.Key])
		}
		for _, k := range []string{"k1", "k2"} {
			c.Assert(s.labeler.GetRegionLabel(region, k), Equals, tc.labels[k])
		}
	}
}

func (s *testLabelerSuite) TestSaveLoadRule(c *C) {
	rules := []*LabelRule{
		{ID: "rule1", Labels: []RegionLabel{{Key: "k1", Value: "v1"}}, RuleType: "key-range", Data: makeKeyRanges("1234", "5678")},
		{ID: "rule2", Labels: []RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: makeKeyRanges("ab12", "cd12")},
		{ID: "rule3", Labels: []RegionLabel{{Key: "k3", Value: "v3"}}, RuleType: "key-range", Data: makeKeyRanges("abcd", "efef")},
	}
	for _, r := range rules {
		err := s.labeler.SetLabelRule(r)
		c.Assert(err, IsNil)
	}

	labeler, err := NewRegionLabeler(context.Background(), s.store, time.Millisecond*100)
	c.Assert(err, IsNil)
	for _, r := range rules {
		r2 := labeler.GetLabelRule(r.ID)
		expectSameRules(c, r2, r)
	}
}

func expectSameRegionLabels(c *C, r1, r2 *RegionLabel) {
	r1.checkAndAdjustExpire()
	r2.checkAndAdjustExpire()
	if len(r1.TTL) == 0 {
		c.Assert(r2, DeepEquals, r1)
	}

	r2.StartAt = r1.StartAt
	r2.checkAndAdjustExpire()

	c.Assert(r2, DeepEquals, r1)
}

func expectSameRules(c *C, r1, r2 *LabelRule) {
	c.Assert(r1.Labels, HasLen, len(r2.Labels))
	for id := 0; id < len(r1.Labels); id++ {
		expectSameRegionLabels(c, &r1.Labels[id], &r2.Labels[id])
	}

	c.Assert(r2, DeepEquals, r1)
}

func (s *testLabelerSuite) TestKeyRange(c *C) {
	rules := []*LabelRule{
		{ID: "rule1", Labels: []RegionLabel{{Key: "k1", Value: "v1"}}, RuleType: "key-range", Data: makeKeyRanges("1234", "5678")},
		{ID: "rule2", Labels: []RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: makeKeyRanges("ab12", "cd12")},
		{ID: "rule3", Labels: []RegionLabel{{Key: "k3", Value: "v3"}}, RuleType: "key-range", Data: makeKeyRanges("abcd", "efef")},
	}
	for _, r := range rules {
		err := s.labeler.SetLabelRule(r)
		c.Assert(err, IsNil)
	}

	type testCase struct {
		start, end string
		labels     map[string]string
	}
	testCases := []testCase{
		{"1234", "5678", map[string]string{"k1": "v1"}},
		{"1234", "aaaa", map[string]string{}},
		{"abcd", "abff", map[string]string{"k2": "v2", "k3": "v3"}},
		{"cd12", "dddd", map[string]string{"k3": "v3"}},
		{"ffee", "ffff", map[string]string{}},
	}
	for _, tc := range testCases {
		start, _ := hex.DecodeString(tc.start)
		end, _ := hex.DecodeString(tc.end)
		region := core.NewTestRegionInfo(start, end)
		labels := s.labeler.GetRegionLabels(region)
		c.Assert(labels, HasLen, len(tc.labels))
		for _, l := range labels {
			c.Assert(tc.labels[l.Key], Equals, l.Value)
		}
		for _, k := range []string{"k1", "k2", "k3"} {
			c.Assert(s.labeler.GetRegionLabel(region, k), Equals, tc.labels[k])
		}
	}
}

func (s *testLabelerSuite) TestLabelerRuleTTL(c *C) {
	rules := []*LabelRule{
		{
			ID: "rule1",
			Labels: []RegionLabel{
				{Key: "k1", Value: "v1"},
			},
			RuleType: "key-range",
			Data:     makeKeyRanges("1234", "5678")},
		{
			ID: "rule2",
			Labels: []RegionLabel{
				{Key: "k2", Value: "v2", TTL: "1s"}, // would expire first.
			},
			RuleType: "key-range",

			Data: makeKeyRanges("1234", "5678")},

		{
			ID:       "rule3",
			Labels:   []RegionLabel{{Key: "k3", Value: "v3", TTL: "1h"}},
			RuleType: "key-range",
			Data:     makeKeyRanges("1234", "5678")},
	}

	start, _ := hex.DecodeString("1234")
	end, _ := hex.DecodeString("5678")
	region := core.NewTestRegionInfo(start, end)
	// the region has no lable rule at the beginning.
	c.Assert(s.labeler.GetRegionLabels(region), HasLen, 0)

	// set rules for the region.
	for _, r := range rules {
		err := s.labeler.SetLabelRule(r)
		c.Assert(err, IsNil)
	}
	// get rule with "rule2".
	c.Assert(s.labeler.GetLabelRule("rule2"), NotNil)
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/schedule/labeler/regionLabelExpireSub1Minute", "return(true)"), IsNil)

	// rule2 should expire and only 2 labels left.
	labels := s.labeler.GetRegionLabels(region)
	c.Assert(labels, HasLen, 2)

	c.Assert(failpoint.Disable("github.com/tikv/pd/server/schedule/labeler/regionLabelExpireSub1Minute"), IsNil)
	// rule2 should be exist since `GetRegionLabels` won't clear it physically.
	s.checkRuleInMemoryAndStoage(c, "rule2", true)
	c.Assert(s.labeler.GetLabelRule("rule2"), IsNil)
	// rule2 should be physically clear.
	s.checkRuleInMemoryAndStoage(c, "rule2", false)

	c.Assert(s.labeler.GetRegionLabel(region, "k2"), Equals, "")

	c.Assert(s.labeler.GetLabelRule("rule3"), NotNil)
	c.Assert(s.labeler.GetLabelRule("rule1"), NotNil)
}

func (s *testLabelerSuite) checkRuleInMemoryAndStoage(c *C, ruleID string, exist bool) {
	c.Assert(s.labeler.labelRules[ruleID] != nil, Equals, exist)
	existInStorage := false
	s.labeler.storage.LoadRegionRules(func(k, v string) {
		if k == ruleID {
			existInStorage = true
		}
	})
	c.Assert(existInStorage, Equals, exist)
}

func (s *testLabelerSuite) TestGC(c *C) {
	// set gcInterval to 1 hour.
	store := storage.NewStorageWithMemoryBackend()
	labeler, err := NewRegionLabeler(context.Background(), store, time.Hour)
	c.Assert(err, IsNil)
	ttls := []string{"1ms", "1ms", "1ms", "5ms", "5ms", "10ms", "1h", "24h"}
	start, _ := hex.DecodeString("1234")
	end, _ := hex.DecodeString("5678")
	region := core.NewTestRegionInfo(start, end)
	// the region has no lable rule at the beginning.
	c.Assert(labeler.GetRegionLabels(region), HasLen, 0)

	labels := []RegionLabel{}
	for id, ttl := range ttls {
		labels = append(labels, RegionLabel{Key: fmt.Sprintf("k%d", id), Value: fmt.Sprintf("v%d", id), TTL: ttl})
		rule := &LabelRule{
			ID:       fmt.Sprintf("rule%d", id),
			Labels:   labels,
			RuleType: "key-range",
			Data:     makeKeyRanges("1234", "5678")}
		err := labeler.SetLabelRule(rule)
		c.Assert(err, IsNil)
	}

	c.Assert(labeler.labelRules, HasLen, len(ttls))

	// check all rules unitl some rule expired.
	for {
		time.Sleep(time.Millisecond * 5)
		labels := labeler.GetRegionLabels(region)
		if len(labels) != len(ttls) {
			break
		}
	}

	// no rule was cleared because the gc interval is big.
	c.Assert(labeler.labelRules, HasLen, len(ttls))

	labeler.checkAndClearExpiredLabels()

	labeler.RLock()
	currentRuleLen := len(labeler.labelRules)
	labeler.RUnlock()
	c.Assert(currentRuleLen <= 5, IsTrue)
}

func makeKeyRanges(keys ...string) []interface{} {
	var res []interface{}
	for i := 0; i < len(keys); i += 2 {
		res = append(res, map[string]interface{}{"start_key": keys[i], "end_key": keys[i+1]})
	}
	return res
}
