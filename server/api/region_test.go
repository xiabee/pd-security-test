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
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"sort"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/core"
)

var _ = Suite(&testRegionStructSuite{})

type testRegionStructSuite struct{}

func (s *testRegionStructSuite) TestPeer(c *C) {
	peers := []*metapb.Peer{
		{Id: 1, StoreId: 10, Role: metapb.PeerRole_Voter},
		{Id: 2, StoreId: 20, Role: metapb.PeerRole_Learner},
		{Id: 3, StoreId: 30, Role: metapb.PeerRole_IncomingVoter},
		{Id: 4, StoreId: 40, Role: metapb.PeerRole_DemotingVoter},
	}
	// float64 is the default numeric type for JSON
	expected := []map[string]interface{}{
		{"id": float64(1), "store_id": float64(10), "role_name": "Voter"},
		{"id": float64(2), "store_id": float64(20), "role": float64(1), "role_name": "Learner", "is_learner": true},
		{"id": float64(3), "store_id": float64(30), "role": float64(2), "role_name": "IncomingVoter"},
		{"id": float64(4), "store_id": float64(40), "role": float64(3), "role_name": "DemotingVoter"},
	}

	data, err := json.Marshal(fromPeerSlice(peers))
	c.Assert(err, IsNil)
	var ret []map[string]interface{}
	c.Assert(json.Unmarshal(data, &ret), IsNil)
	c.Assert(ret, DeepEquals, expected)
}

func (s *testRegionStructSuite) TestPeerStats(c *C) {
	peers := []*pdpb.PeerStats{
		{Peer: &metapb.Peer{Id: 1, StoreId: 10, Role: metapb.PeerRole_Voter}, DownSeconds: 0},
		{Peer: &metapb.Peer{Id: 2, StoreId: 20, Role: metapb.PeerRole_Learner}, DownSeconds: 1},
		{Peer: &metapb.Peer{Id: 3, StoreId: 30, Role: metapb.PeerRole_IncomingVoter}, DownSeconds: 2},
		{Peer: &metapb.Peer{Id: 4, StoreId: 40, Role: metapb.PeerRole_DemotingVoter}, DownSeconds: 3},
	}
	// float64 is the default numeric type for JSON
	expected := []map[string]interface{}{
		{"peer": map[string]interface{}{"id": float64(1), "store_id": float64(10), "role_name": "Voter"}},
		{"peer": map[string]interface{}{"id": float64(2), "store_id": float64(20), "role": float64(1), "role_name": "Learner", "is_learner": true}, "down_seconds": float64(1)},
		{"peer": map[string]interface{}{"id": float64(3), "store_id": float64(30), "role": float64(2), "role_name": "IncomingVoter"}, "down_seconds": float64(2)},
		{"peer": map[string]interface{}{"id": float64(4), "store_id": float64(40), "role": float64(3), "role_name": "DemotingVoter"}, "down_seconds": float64(3)},
	}

	data, err := json.Marshal(fromPeerStatsSlice(peers))
	c.Assert(err, IsNil)
	var ret []map[string]interface{}
	c.Assert(json.Unmarshal(data, &ret), IsNil)
	c.Assert(ret, DeepEquals, expected)
}

var _ = Suite(&testRegionSuite{})

type testRegionSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testRegionSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testRegionSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func newTestRegionInfo(regionID, storeID uint64, start, end []byte, opts ...core.RegionCreateOption) *core.RegionInfo {
	leader := &metapb.Peer{
		Id:      regionID,
		StoreId: storeID,
	}
	metaRegion := &metapb.Region{
		Id:          regionID,
		StartKey:    start,
		EndKey:      end,
		Peers:       []*metapb.Peer{leader},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	newOpts := []core.RegionCreateOption{
		core.SetApproximateKeys(10),
		core.SetApproximateSize(10),
		core.SetWrittenBytes(100 * 1024 * 1024),
		core.SetWrittenKeys(1 * 1024 * 1024),
		core.SetReadBytes(200 * 1024 * 1024),
		core.SetReadKeys(2 * 1024 * 1024),
	}
	newOpts = append(newOpts, opts...)
	region := core.NewRegionInfo(metaRegion, leader, newOpts...)
	return region
}

func (s *testRegionSuite) TestRegion(c *C) {
	r := newTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	mustRegionHeartbeat(c, s.svr, r)
	url := fmt.Sprintf("%s/region/id/%d", s.urlPrefix, r.GetID())
	r1 := &RegionInfo{}
	r1m := make(map[string]interface{})
	c.Assert(readJSON(testDialClient, url, r1), IsNil)
	r1.Adjust()
	c.Assert(r1, DeepEquals, NewRegionInfo(r))
	c.Assert(readJSON(testDialClient, url, &r1m), IsNil)
	c.Assert(r1m["written_bytes"].(float64), Equals, float64(r.GetBytesWritten()))
	c.Assert(r1m["written_keys"].(float64), Equals, float64(r.GetKeysWritten()))
	c.Assert(r1m["read_bytes"].(float64), Equals, float64(r.GetBytesRead()))
	c.Assert(r1m["read_keys"].(float64), Equals, float64(r.GetKeysRead()))

	url = fmt.Sprintf("%s/region/key/%s", s.urlPrefix, "a")
	r2 := &RegionInfo{}
	c.Assert(readJSON(testDialClient, url, r2), IsNil)
	r2.Adjust()
	c.Assert(r2, DeepEquals, NewRegionInfo(r))
}

func (s *testRegionSuite) TestRegionCheck(c *C) {
	r := newTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	downPeer := &metapb.Peer{Id: 13, StoreId: 2}
	r = r.Clone(core.WithAddPeer(downPeer), core.WithDownPeers([]*pdpb.PeerStats{{Peer: downPeer, DownSeconds: 3600}}), core.WithPendingPeers([]*metapb.Peer{downPeer}))
	mustRegionHeartbeat(c, s.svr, r)
	url := fmt.Sprintf("%s/region/id/%d", s.urlPrefix, r.GetID())
	r1 := &RegionInfo{}
	c.Assert(readJSON(testDialClient, url, r1), IsNil)
	r1.Adjust()
	c.Assert(r1, DeepEquals, NewRegionInfo(r))

	url = fmt.Sprintf("%s/regions/check/%s", s.urlPrefix, "down-peer")
	r2 := &RegionsInfo{}
	c.Assert(readJSON(testDialClient, url, r2), IsNil)
	r2.Adjust()
	c.Assert(r2, DeepEquals, &RegionsInfo{Count: 1, Regions: []RegionInfo{*NewRegionInfo(r)}})

	url = fmt.Sprintf("%s/regions/check/%s", s.urlPrefix, "pending-peer")
	r3 := &RegionsInfo{}
	c.Assert(readJSON(testDialClient, url, r3), IsNil)
	r3.Adjust()
	c.Assert(r3, DeepEquals, &RegionsInfo{Count: 1, Regions: []RegionInfo{*NewRegionInfo(r)}})

	url = fmt.Sprintf("%s/regions/check/%s", s.urlPrefix, "offline-peer")
	r4 := &RegionsInfo{}
	c.Assert(readJSON(testDialClient, url, r4), IsNil)
	r4.Adjust()
	c.Assert(r4, DeepEquals, &RegionsInfo{Count: 0, Regions: []RegionInfo{}})

	r = r.Clone(core.SetApproximateSize(1))
	mustRegionHeartbeat(c, s.svr, r)
	url = fmt.Sprintf("%s/regions/check/%s", s.urlPrefix, "empty-region")
	r5 := &RegionsInfo{}
	c.Assert(readJSON(testDialClient, url, r5), IsNil)
	r5.Adjust()
	c.Assert(r5, DeepEquals, &RegionsInfo{Count: 1, Regions: []RegionInfo{*NewRegionInfo(r)}})

	r = r.Clone(core.SetApproximateSize(1))
	mustRegionHeartbeat(c, s.svr, r)
	url = fmt.Sprintf("%s/regions/check/%s", s.urlPrefix, "hist-size")
	r6 := make([]*histItem, 1)
	c.Assert(readJSON(testDialClient, url, &r6), IsNil)
	histSizes := []*histItem{{Start: 1, End: 1, Count: 1}}
	c.Assert(r6, DeepEquals, histSizes)

	r = r.Clone(core.SetApproximateKeys(1000))
	mustRegionHeartbeat(c, s.svr, r)
	url = fmt.Sprintf("%s/regions/check/%s", s.urlPrefix, "hist-keys")
	r7 := make([]*histItem, 1)
	c.Assert(readJSON(testDialClient, url, &r7), IsNil)
	histKeys := []*histItem{{Start: 1000, End: 1999, Count: 1}}
	c.Assert(r7, DeepEquals, histKeys)
}

func (s *testRegionSuite) TestRegions(c *C) {
	rs := []*core.RegionInfo{
		newTestRegionInfo(2, 1, []byte("a"), []byte("b")),
		newTestRegionInfo(3, 1, []byte("b"), []byte("c")),
		newTestRegionInfo(4, 2, []byte("c"), []byte("d")),
	}
	regions := make([]RegionInfo, 0, len(rs))
	for _, r := range rs {
		regions = append(regions, *NewRegionInfo(r))
		mustRegionHeartbeat(c, s.svr, r)
	}
	url := fmt.Sprintf("%s/regions", s.urlPrefix)
	RegionsInfo := &RegionsInfo{}
	err := readJSON(testDialClient, url, RegionsInfo)
	c.Assert(err, IsNil)
	c.Assert(RegionsInfo.Count, Equals, len(regions))
	sort.Slice(RegionsInfo.Regions, func(i, j int) bool {
		return RegionsInfo.Regions[i].ID < RegionsInfo.Regions[j].ID
	})
	for i, r := range RegionsInfo.Regions {
		c.Assert(r.ID, Equals, regions[i].ID)
		c.Assert(r.ApproximateSize, Equals, regions[i].ApproximateSize)
		c.Assert(r.ApproximateKeys, Equals, regions[i].ApproximateKeys)
	}
}

func (s *testRegionSuite) TestStoreRegions(c *C) {
	r1 := newTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	r2 := newTestRegionInfo(3, 1, []byte("b"), []byte("c"))
	r3 := newTestRegionInfo(4, 2, []byte("c"), []byte("d"))
	mustRegionHeartbeat(c, s.svr, r1)
	mustRegionHeartbeat(c, s.svr, r2)
	mustRegionHeartbeat(c, s.svr, r3)

	regionIDs := []uint64{2, 3}
	url := fmt.Sprintf("%s/regions/store/%d", s.urlPrefix, 1)
	r4 := &RegionsInfo{}
	err := readJSON(testDialClient, url, r4)
	c.Assert(err, IsNil)
	c.Assert(r4.Count, Equals, len(regionIDs))
	sort.Slice(r4.Regions, func(i, j int) bool { return r4.Regions[i].ID < r4.Regions[j].ID })
	for i, r := range r4.Regions {
		c.Assert(r.ID, Equals, regionIDs[i])
	}

	regionIDs = []uint64{4}
	url = fmt.Sprintf("%s/regions/store/%d", s.urlPrefix, 2)
	r5 := &RegionsInfo{}
	err = readJSON(testDialClient, url, r5)
	c.Assert(err, IsNil)
	c.Assert(r5.Count, Equals, len(regionIDs))
	for i, r := range r5.Regions {
		c.Assert(r.ID, Equals, regionIDs[i])
	}

	regionIDs = []uint64{}
	url = fmt.Sprintf("%s/regions/store/%d", s.urlPrefix, 3)
	r6 := &RegionsInfo{}
	err = readJSON(testDialClient, url, r6)
	c.Assert(err, IsNil)
	c.Assert(r6.Count, Equals, len(regionIDs))
}

func (s *testRegionSuite) TestTopFlow(c *C) {
	r1 := newTestRegionInfo(1, 1, []byte("a"), []byte("b"), core.SetWrittenBytes(1000), core.SetReadBytes(1000), core.SetRegionConfVer(1), core.SetRegionVersion(1))
	mustRegionHeartbeat(c, s.svr, r1)
	r2 := newTestRegionInfo(2, 1, []byte("b"), []byte("c"), core.SetWrittenBytes(2000), core.SetReadBytes(0), core.SetRegionConfVer(2), core.SetRegionVersion(3))
	mustRegionHeartbeat(c, s.svr, r2)
	r3 := newTestRegionInfo(3, 1, []byte("c"), []byte("d"), core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	mustRegionHeartbeat(c, s.svr, r3)
	s.checkTopRegions(c, fmt.Sprintf("%s/regions/writeflow", s.urlPrefix), []uint64{2, 1, 3})
	s.checkTopRegions(c, fmt.Sprintf("%s/regions/readflow", s.urlPrefix), []uint64{1, 3, 2})
	s.checkTopRegions(c, fmt.Sprintf("%s/regions/writeflow?limit=2", s.urlPrefix), []uint64{2, 1})
	s.checkTopRegions(c, fmt.Sprintf("%s/regions/confver", s.urlPrefix), []uint64{3, 2, 1})
	s.checkTopRegions(c, fmt.Sprintf("%s/regions/confver?limit=2", s.urlPrefix), []uint64{3, 2})
	s.checkTopRegions(c, fmt.Sprintf("%s/regions/version", s.urlPrefix), []uint64{2, 3, 1})
	s.checkTopRegions(c, fmt.Sprintf("%s/regions/version?limit=2", s.urlPrefix), []uint64{2, 3})
}

func (s *testRegionSuite) TestTopSize(c *C) {
	baseOpt := []core.RegionCreateOption{core.SetRegionConfVer(3), core.SetRegionVersion(3)}
	opt := core.SetApproximateSize(1000)
	r1 := newTestRegionInfo(7, 1, []byte("a"), []byte("b"), append(baseOpt, opt)...)
	mustRegionHeartbeat(c, s.svr, r1)
	opt = core.SetApproximateSize(900)
	r2 := newTestRegionInfo(8, 1, []byte("b"), []byte("c"), append(baseOpt, opt)...)
	mustRegionHeartbeat(c, s.svr, r2)
	opt = core.SetApproximateSize(800)
	r3 := newTestRegionInfo(9, 1, []byte("c"), []byte("d"), append(baseOpt, opt)...)
	mustRegionHeartbeat(c, s.svr, r3)
	// query with limit
	s.checkTopRegions(c, fmt.Sprintf("%s/regions/size?limit=%d", s.urlPrefix, 2), []uint64{7, 8})
}

func (s *testRegionSuite) TestAccelerateRegionsScheduleInRange(c *C) {
	r1 := newTestRegionInfo(557, 13, []byte("a1"), []byte("a2"))
	r2 := newTestRegionInfo(558, 14, []byte("a2"), []byte("a3"))
	r3 := newTestRegionInfo(559, 15, []byte("a3"), []byte("a4"))
	mustRegionHeartbeat(c, s.svr, r1)
	mustRegionHeartbeat(c, s.svr, r2)
	mustRegionHeartbeat(c, s.svr, r3)
	body := fmt.Sprintf(`{"start_key":"%s", "end_key": "%s"}`, hex.EncodeToString([]byte("a1")), hex.EncodeToString([]byte("a3")))

	err := postJSON(testDialClient, fmt.Sprintf("%s/regions/accelerate-schedule", s.urlPrefix), []byte(body))
	c.Assert(err, IsNil)
	idList := s.svr.GetRaftCluster().GetSuspectRegions()
	c.Assert(len(idList), Equals, 2)
}

func (s *testRegionSuite) TestScatterRegions(c *C) {
	r1 := newTestRegionInfo(601, 13, []byte("b1"), []byte("b2"))
	r1.GetMeta().Peers = append(r1.GetMeta().Peers, &metapb.Peer{Id: 5, StoreId: 13}, &metapb.Peer{Id: 6, StoreId: 13})
	r2 := newTestRegionInfo(602, 13, []byte("b2"), []byte("b3"))
	r2.GetMeta().Peers = append(r2.GetMeta().Peers, &metapb.Peer{Id: 7, StoreId: 13}, &metapb.Peer{Id: 8, StoreId: 13})
	r3 := newTestRegionInfo(603, 13, []byte("b4"), []byte("b4"))
	r3.GetMeta().Peers = append(r3.GetMeta().Peers, &metapb.Peer{Id: 9, StoreId: 13}, &metapb.Peer{Id: 10, StoreId: 13})
	mustRegionHeartbeat(c, s.svr, r1)
	mustRegionHeartbeat(c, s.svr, r2)
	mustRegionHeartbeat(c, s.svr, r3)
	mustPutStore(c, s.svr, 13, metapb.StoreState_Up, []*metapb.StoreLabel{})
	mustPutStore(c, s.svr, 14, metapb.StoreState_Up, []*metapb.StoreLabel{})
	mustPutStore(c, s.svr, 15, metapb.StoreState_Up, []*metapb.StoreLabel{})
	body := fmt.Sprintf(`{"start_key":"%s", "end_key": "%s"}`, hex.EncodeToString([]byte("b1")), hex.EncodeToString([]byte("b3")))

	err := postJSON(testDialClient, fmt.Sprintf("%s/regions/scatter", s.urlPrefix), []byte(body))
	c.Assert(err, IsNil)
	op1 := s.svr.GetRaftCluster().GetOperatorController().GetOperator(601)
	op2 := s.svr.GetRaftCluster().GetOperatorController().GetOperator(602)
	op3 := s.svr.GetRaftCluster().GetOperatorController().GetOperator(603)
	// At least one operator used to scatter region
	c.Assert(op1 != nil || op2 != nil || op3 != nil, IsTrue)
}

func (s *testRegionSuite) TestSplitRegions(c *C) {
	r1 := newTestRegionInfo(601, 13, []byte("aaa"), []byte("ggg"))
	r1.GetMeta().Peers = append(r1.GetMeta().Peers, &metapb.Peer{Id: 5, StoreId: 13}, &metapb.Peer{Id: 6, StoreId: 13})
	mustRegionHeartbeat(c, s.svr, r1)
	mustPutStore(c, s.svr, 13, metapb.StoreState_Up, []*metapb.StoreLabel{})
	newRegionID := uint64(11)
	body := fmt.Sprintf(`{"retry_limit":%v, "split_keys": ["%s","%s","%s"]}`, 0,
		hex.EncodeToString([]byte("bbb")),
		hex.EncodeToString([]byte("ccc")),
		hex.EncodeToString([]byte("ddd")))
	checkOpt := func(res []byte, code int) {
		s := &struct {
			ProcessedPercentage int      `json:"processed-percentage"`
			NewRegionsID        []uint64 `json:"regions-id"`
		}{}
		err := json.Unmarshal(res, s)
		c.Assert(err, IsNil)
		c.Assert(s.ProcessedPercentage, Equals, 100)
		c.Assert(s.NewRegionsID, DeepEquals, []uint64{newRegionID})
	}
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/api/splitResponses", fmt.Sprintf("return(%v)", newRegionID)), IsNil)
	err := postJSON(testDialClient, fmt.Sprintf("%s/regions/split", s.urlPrefix), []byte(body), checkOpt)
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/api/splitResponses"), IsNil)
	c.Assert(err, IsNil)
}

func (s *testRegionSuite) checkTopRegions(c *C, url string, regionIDs []uint64) {
	regions := &RegionsInfo{}
	err := readJSON(testDialClient, url, regions)
	c.Assert(err, IsNil)
	c.Assert(regions.Count, Equals, len(regionIDs))
	for i, r := range regions.Regions {
		c.Assert(r.ID, Equals, regionIDs[i])
	}
}

func (s *testRegionSuite) TestTopN(c *C) {
	writtenBytes := []uint64{10, 10, 9, 5, 3, 2, 2, 1, 0, 0}
	for n := 0; n <= len(writtenBytes)+1; n++ {
		regions := make([]*core.RegionInfo, 0, len(writtenBytes))
		for _, i := range rand.Perm(len(writtenBytes)) {
			id := uint64(i + 1)
			region := newTestRegionInfo(id, id, nil, nil, core.SetWrittenBytes(writtenBytes[i]))
			regions = append(regions, region)
		}
		topN := TopNRegions(regions, func(a, b *core.RegionInfo) bool { return a.GetBytesWritten() < b.GetBytesWritten() }, n)
		if n > len(writtenBytes) {
			c.Assert(len(topN), Equals, len(writtenBytes))
		} else {
			c.Assert(len(topN), Equals, n)
		}
		for i := range topN {
			c.Assert(topN[i].GetBytesWritten(), Equals, writtenBytes[i])
		}
	}
}

var _ = Suite(&testGetRegionSuite{})

type testGetRegionSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testGetRegionSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testGetRegionSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testGetRegionSuite) TestRegionKey(c *C) {
	r := newTestRegionInfo(99, 1, []byte{0xFF, 0xFF, 0xAA}, []byte{0xFF, 0xFF, 0xCC}, core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	mustRegionHeartbeat(c, s.svr, r)
	url := fmt.Sprintf("%s/region/key/%s", s.urlPrefix, url.QueryEscape(string([]byte{0xFF, 0xFF, 0xBB})))
	RegionInfo := &RegionInfo{}
	err := readJSON(testDialClient, url, RegionInfo)
	c.Assert(err, IsNil)
	c.Assert(r.GetID(), Equals, RegionInfo.ID)
}

func (s *testGetRegionSuite) TestScanRegionByKey(c *C) {
	r1 := newTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	r2 := newTestRegionInfo(3, 1, []byte("b"), []byte("c"))
	r3 := newTestRegionInfo(4, 2, []byte("c"), []byte("e"))
	r4 := newTestRegionInfo(5, 2, []byte("x"), []byte("z"))
	r := newTestRegionInfo(99, 1, []byte{0xFF, 0xFF, 0xAA}, []byte{0xFF, 0xFF, 0xCC}, core.SetWrittenBytes(500), core.SetReadBytes(800), core.SetRegionConfVer(3), core.SetRegionVersion(2))
	mustRegionHeartbeat(c, s.svr, r1)
	mustRegionHeartbeat(c, s.svr, r2)
	mustRegionHeartbeat(c, s.svr, r3)
	mustRegionHeartbeat(c, s.svr, r4)
	mustRegionHeartbeat(c, s.svr, r)

	url := fmt.Sprintf("%s/regions/key?key=%s", s.urlPrefix, "b")
	regionIds := []uint64{3, 4, 5, 99}
	regions := &RegionsInfo{}
	err := readJSON(testDialClient, url, regions)
	c.Assert(err, IsNil)
	c.Assert(len(regionIds), Equals, regions.Count)
	for i, v := range regionIds {
		c.Assert(v, Equals, regions.Regions[i].ID)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s", s.urlPrefix, "d")
	regionIds = []uint64{4, 5, 99}
	regions = &RegionsInfo{}
	err = readJSON(testDialClient, url, regions)
	c.Assert(err, IsNil)
	c.Assert(len(regionIds), Equals, regions.Count)
	for i, v := range regionIds {
		c.Assert(v, Equals, regions.Regions[i].ID)
	}
	url = fmt.Sprintf("%s/regions/key?key=%s", s.urlPrefix, "g")
	regionIds = []uint64{5, 99}
	regions = &RegionsInfo{}
	err = readJSON(testDialClient, url, regions)
	c.Assert(err, IsNil)
	c.Assert(len(regionIds), Equals, regions.Count)
	for i, v := range regionIds {
		c.Assert(v, Equals, regions.Regions[i].ID)
	}
}

// Create n regions (0..n) of n stores (0..n).
// Each region contains np peers, the first peer is the leader.
// (copied from server/cluster_test.go)
func newTestRegions() []*core.RegionInfo {
	n := uint64(10000)
	np := uint64(3)

	regions := make([]*core.RegionInfo, 0, n)
	for i := uint64(0); i < n; i++ {
		peers := make([]*metapb.Peer, 0, np)
		for j := uint64(0); j < np; j++ {
			peer := &metapb.Peer{
				Id: i*np + j,
			}
			peer.StoreId = (i + j) % n
			peers = append(peers, peer)
		}
		region := &metapb.Region{
			Id:          i,
			Peers:       peers,
			StartKey:    []byte(fmt.Sprintf("%d", i)),
			EndKey:      []byte(fmt.Sprintf("%d", i+1)),
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
		}
		regions = append(regions, core.NewRegionInfo(region, peers[0]))
	}
	return regions
}

func BenchmarkRenderJSON(b *testing.B) {
	regionInfos := newTestRegions()
	rd := createStreamingRender()
	regions := convertToAPIRegions(regionInfos)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buffer bytes.Buffer
		rd.JSON(&buffer, 200, regions)
	}
}

func BenchmarkConvertToAPIRegions(b *testing.B) {
	regionInfos := newTestRegions()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		regions := convertToAPIRegions(regionInfos)
		_ = regions.Count
	}
}

func BenchmarkHexRegionKey(b *testing.B) {
	key := []byte("region_number_infinity")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = core.HexRegionKey(key)
	}
}

func BenchmarkHexRegionKeyStr(b *testing.B) {
	key := []byte("region_number_infinity")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = core.HexRegionKeyStr(key)
	}
}
