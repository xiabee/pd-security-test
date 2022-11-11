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

package operator

import (
	"context"
	"encoding/hex"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/mock/mockcluster"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule/opt"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/versioninfo"
)

var _ = Suite(&testCreateOperatorSuite{})

type testCreateOperatorSuite struct {
	cluster *mockcluster.Cluster
	ctx     context.Context
	cancel  context.CancelFunc
}

func (s *testCreateOperatorSuite) SetUpTest(c *C) {
	opts := config.NewTestOptions()
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.cluster = mockcluster.NewCluster(s.ctx, opts)
	s.cluster.SetLabelPropertyConfig(config.LabelPropertyConfig{
		opt.RejectLeader: {{Key: "noleader", Value: "true"}},
	})
	s.cluster.SetLocationLabels([]string{"zone", "host"})
	s.cluster.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsStore(3, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsStore(4, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsStore(5, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsStore(6, 0, map[string]string{"zone": "z1", "host": "h2"})
	s.cluster.AddLabelsStore(7, 0, map[string]string{"zone": "z1", "host": "h2"})
	s.cluster.AddLabelsStore(8, 0, map[string]string{"zone": "z2", "host": "h1"})
	s.cluster.AddLabelsStore(9, 0, map[string]string{"zone": "z2", "host": "h2"})
	s.cluster.AddLabelsStore(10, 0, map[string]string{"zone": "z3", "host": "h1", "noleader": "true"})
}

func (s *testCreateOperatorSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testCreateOperatorSuite) TestCreateSplitRegionOperator(c *C) {
	type testCase struct {
		startKey      []byte
		endKey        []byte
		originPeers   []*metapb.Peer // first is leader
		policy        pdpb.CheckPolicy
		keys          [][]byte
		expectedError bool
	}
	cases := []testCase{
		{
			startKey: []byte("a"),
			endKey:   []byte("b"),
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			policy:        pdpb.CheckPolicy_APPROXIMATE,
			expectedError: false,
		},
		{
			startKey: []byte("c"),
			endKey:   []byte("d"),
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			policy:        pdpb.CheckPolicy_SCAN,
			expectedError: false,
		},
		{
			startKey: []byte("e"),
			endKey:   []byte("h"),
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			policy:        pdpb.CheckPolicy_USEKEY,
			keys:          [][]byte{[]byte("f"), []byte("g")},
			expectedError: false,
		},
		{
			startKey: []byte("i"),
			endKey:   []byte("j"),
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_IncomingVoter},
			},
			policy:        pdpb.CheckPolicy_APPROXIMATE,
			expectedError: true,
		},
		{
			startKey: []byte("k"),
			endKey:   []byte("l"),
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
			},
			policy:        pdpb.CheckPolicy_APPROXIMATE,
			expectedError: true,
		},
	}

	for _, tc := range cases {
		region := core.NewRegionInfo(&metapb.Region{
			Id:       1,
			StartKey: tc.startKey,
			EndKey:   tc.endKey,
			Peers:    tc.originPeers,
		}, tc.originPeers[0])
		op, err := CreateSplitRegionOperator("test", region, 0, tc.policy, tc.keys)
		if tc.expectedError {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(op.Kind(), Equals, OpSplit)
		c.Assert(op.steps, HasLen, 1)
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case SplitRegion:
				c.Assert(step.StartKey, DeepEquals, tc.startKey)
				c.Assert(step.EndKey, DeepEquals, tc.endKey)
				c.Assert(step.Policy, Equals, tc.policy)
				c.Assert(step.SplitKeys, DeepEquals, tc.keys)
			default:
				c.Errorf("unexpected type: %s", step.String())
			}
		}
	}
}

func (s *testCreateOperatorSuite) TestCreateMergeRegionOperator(c *C) {
	type testCase struct {
		sourcePeers   []*metapb.Peer // first is leader
		targetPeers   []*metapb.Peer // first is leader
		kind          OpKind
		expectedError bool
		prepareSteps  []OpStep
	}
	cases := []testCase{
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			[]*metapb.Peer{
				{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			OpMerge,
			false,
			[]OpStep{},
		},
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			[]*metapb.Peer{
				{Id: 4, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			OpMerge | OpLeader | OpRegion,
			false,
			[]OpStep{
				AddLearner{ToStore: 3},
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 3}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 3}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
				RemovePeer{FromStore: 1},
			},
		},
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_DemotingVoter},
			},
			[]*metapb.Peer{
				{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			0,
			true,
			nil,
		},
		{
			[]*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
			},
			[]*metapb.Peer{
				{Id: 3, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 2, Role: metapb.PeerRole_IncomingVoter},
			},
			0,
			true,
			nil,
		},
	}

	for _, tc := range cases {
		source := core.NewRegionInfo(&metapb.Region{Id: 68, Peers: tc.sourcePeers}, tc.sourcePeers[0])
		target := core.NewRegionInfo(&metapb.Region{Id: 86, Peers: tc.targetPeers}, tc.targetPeers[0])
		ops, err := CreateMergeRegionOperator("test", s.cluster, source, target, 0)
		if tc.expectedError {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(ops, HasLen, 2)
		c.Assert(ops[0].kind, Equals, tc.kind)
		c.Assert(ops[0].Len(), Equals, len(tc.prepareSteps)+1)
		c.Assert(ops[1].kind, Equals, tc.kind)
		c.Assert(ops[1].Len(), Equals, 1)
		c.Assert(ops[1].Step(0).(MergeRegion), DeepEquals, MergeRegion{source.GetMeta(), target.GetMeta(), true})

		expectedSteps := append(tc.prepareSteps, MergeRegion{source.GetMeta(), target.GetMeta(), false})
		for i := 0; i < ops[0].Len(); i++ {
			switch step := ops[0].Step(i).(type) {
			case TransferLeader:
				c.Assert(step.FromStore, Equals, expectedSteps[i].(TransferLeader).FromStore)
				c.Assert(step.ToStore, Equals, expectedSteps[i].(TransferLeader).ToStore)
			case AddLearner:
				c.Assert(step.ToStore, Equals, expectedSteps[i].(AddLearner).ToStore)
			case RemovePeer:
				c.Assert(step.FromStore, Equals, expectedSteps[i].(RemovePeer).FromStore)
			case ChangePeerV2Enter:
				c.Assert(len(step.PromoteLearners), Equals, len(expectedSteps[i].(ChangePeerV2Enter).PromoteLearners))
				c.Assert(len(step.DemoteVoters), Equals, len(expectedSteps[i].(ChangePeerV2Enter).DemoteVoters))
				for j, p := range expectedSteps[i].(ChangePeerV2Enter).PromoteLearners {
					c.Assert(step.PromoteLearners[j].ToStore, Equals, p.ToStore)
				}
				for j, d := range expectedSteps[i].(ChangePeerV2Enter).DemoteVoters {
					c.Assert(step.DemoteVoters[j].ToStore, Equals, d.ToStore)
				}
			case ChangePeerV2Leave:
				c.Assert(len(step.PromoteLearners), Equals, len(expectedSteps[i].(ChangePeerV2Leave).PromoteLearners))
				c.Assert(len(step.DemoteVoters), Equals, len(expectedSteps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range expectedSteps[i].(ChangePeerV2Leave).PromoteLearners {
					c.Assert(step.PromoteLearners[j].ToStore, Equals, p.ToStore)
				}
				for j, d := range expectedSteps[i].(ChangePeerV2Leave).DemoteVoters {
					c.Assert(step.DemoteVoters[j].ToStore, Equals, d.ToStore)
				}
			case MergeRegion:
				c.Assert(step, DeepEquals, expectedSteps[i].(MergeRegion))
			}
		}
	}
}

func (s *testCreateOperatorSuite) TestCreateTransferLeaderOperator(c *C) {
	type testCase struct {
		originPeers         []*metapb.Peer // first is leader
		targetLeaderStoreID uint64
		isErr               bool
	}
	cases := []testCase{
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetLeaderStoreID: 3,
			isErr:               false,
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetLeaderStoreID: 1,
			isErr:               true,
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetLeaderStoreID: 4,
			isErr:               true,
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Learner},
			},
			targetLeaderStoreID: 3,
			isErr:               true,
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			targetLeaderStoreID: 3,
			isErr:               true,
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			targetLeaderStoreID: 4,
			isErr:               false,
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			targetLeaderStoreID: 3,
			isErr:               false,
		},
	}
	for _, tc := range cases {
		region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: tc.originPeers}, tc.originPeers[0])
		op, err := CreateTransferLeaderOperator("test", s.cluster, region, tc.originPeers[0].StoreId, tc.targetLeaderStoreID, 0)

		if tc.isErr {
			c.Assert(err, NotNil)
			continue
		}

		c.Assert(err, IsNil)
		c.Assert(op.Kind(), Equals, OpLeader)
		c.Assert(op.steps, HasLen, 1)
		switch step := op.Step(0).(type) {
		case TransferLeader:
			c.Assert(step.FromStore, Equals, tc.originPeers[0].StoreId)
			c.Assert(step.ToStore, Equals, tc.targetLeaderStoreID)
		default:
			c.Errorf("unexpected type: %s", step.String())
		}
	}
}

func (s *testCreateOperatorSuite) TestCreateLeaveJointStateOperator(c *C) {
	type testCase struct {
		originPeers   []*metapb.Peer // first is leader
		offlineStores []uint64
		kind          OpKind
		steps         []OpStep // empty means error
	}
	cases := []testCase{
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			kind: 0,
			steps: []OpStep{
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 3}},
				},
			},
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			kind: OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
			},
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			offlineStores: []uint64{2},
			kind:          OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 3},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
			},
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			offlineStores: []uint64{2, 3},
			kind:          OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
			},
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			offlineStores: []uint64{1, 2, 3, 4},
			kind:          OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
			},
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_IncomingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			kind: 0,
			steps: []OpStep{
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 1}, {ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 3}},
				},
			},
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			kind: OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}, {ToStore: 3}},
				},
			},
		},
		{
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_DemotingVoter},
				{Id: 10, StoreId: 10, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			kind: OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}, {ToStore: 3}},
				},
			},
		},
	}

	for _, tc := range cases {
		for _, storeID := range tc.offlineStores {
			s.cluster.SetStoreOffline(storeID)
		}

		revertOffline := func() {
			for _, storeID := range tc.offlineStores {
				s.cluster.SetStoreUp(storeID)
			}
		}

		region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: tc.originPeers}, tc.originPeers[0])
		op, err := CreateLeaveJointStateOperator("test", s.cluster, region)
		if len(tc.steps) == 0 {
			c.Assert(err, NotNil)
			revertOffline()
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(op.Kind(), Equals, tc.kind)
		c.Assert(len(op.steps), Equals, len(tc.steps))
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				c.Assert(step.FromStore, Equals, tc.steps[i].(TransferLeader).FromStore)
				c.Assert(step.ToStore, Equals, tc.steps[i].(TransferLeader).ToStore)
			case ChangePeerV2Leave:
				c.Assert(len(step.PromoteLearners), Equals, len(tc.steps[i].(ChangePeerV2Leave).PromoteLearners))
				c.Assert(len(step.DemoteVoters), Equals, len(tc.steps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Leave).PromoteLearners {
					c.Assert(step.PromoteLearners[j].ToStore, Equals, p.ToStore)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Leave).DemoteVoters {
					c.Assert(step.DemoteVoters[j].ToStore, Equals, d.ToStore)
				}
			default:
				c.Errorf("unexpected type: %s", step.String())
			}
		}

		revertOffline()
	}
}

func (s *testCreateOperatorSuite) TestCreateMoveRegionOperator(c *C) {
	type testCase struct {
		name            string
		originPeers     []*metapb.Peer // first is leader
		targetPeerRoles map[uint64]placement.PeerRoleType
		steps           []OpStep
		expectedError   error
	}
	tt := []testCase{
		{
			name: "move region partially with incoming voter, demote existed voter",
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Leader,
				3: placement.Learner,
				4: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4, PeerID: 4},
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 3, PeerID: 3},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 3, PeerID: 3},
					},
				},
				RemovePeer{FromStore: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move region partially with incoming leader",
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Leader,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4, PeerID: 4},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1, PeerID: 1}},
				},
				TransferLeader{FromStore: 1, ToStore: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1, PeerID: 1}},
				},
				RemovePeer{FromStore: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move region partially with incoming voter",
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4, PeerID: 4},
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1, PeerID: 1}},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1, PeerID: 1}},
				},
				RemovePeer{FromStore: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move region partially with incoming learner, demote leader",
			originPeers: []*metapb.Peer{
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Learner,
				3: placement.Voter,
				4: placement.Learner,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4, PeerID: 4},
				TransferLeader{FromStore: 2, ToStore: 3},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 2, PeerID: 2},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 2, PeerID: 2},
					},
				},
				RemovePeer{FromStore: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move entirely with incoming voter",
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				4: placement.Leader,
				5: placement.Voter,
				6: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4, PeerID: 4},
				AddLearner{ToStore: 5, PeerID: 5},
				AddLearner{ToStore: 6, PeerID: 6},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{
						{ToStore: 4, PeerID: 4},
						{ToStore: 5, PeerID: 5},
						{ToStore: 6, PeerID: 6},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 2, PeerID: 2},
						{ToStore: 3, PeerID: 3},
					},
				},
				TransferLeader{FromStore: 1, ToStore: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{
						{ToStore: 4, PeerID: 4},
						{ToStore: 5, PeerID: 5},
						{ToStore: 6, PeerID: 6},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 2, PeerID: 2},
						{ToStore: 3, PeerID: 3},
					},
				},
				RemovePeer{FromStore: 1, PeerID: 1},
				RemovePeer{FromStore: 2, PeerID: 2},
				RemovePeer{FromStore: 3, PeerID: 3},
			},
			expectedError: nil,
		},
		{
			name: "move region partially with incoming and old voter, leader step down",
			originPeers: []*metapb.Peer{
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToStore: 2, PeerID: 8},
				TransferLeader{FromStore: 4, ToStore: 3},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{
						{ToStore: 2, PeerID: 8},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 5, PeerID: 5},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{
						{ToStore: 2, PeerID: 8},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 5, PeerID: 5},
					},
				},
				RemovePeer{FromStore: 5, PeerID: 5},
			},
			expectedError: nil,
		},
		{
			name: "move region partially with incoming voter and follower, leader step down",
			originPeers: []*metapb.Peer{
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				1: placement.Follower,
				2: placement.Voter,
				4: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToStore: 1, PeerID: 9},
				AddLearner{ToStore: 2, PeerID: 10},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{
						{ToStore: 1, PeerID: 9},
						{ToStore: 2, PeerID: 10},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 3, PeerID: 3},
						{ToStore: 5, PeerID: 5},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{
						{ToStore: 1, PeerID: 9},
						{ToStore: 2, PeerID: 10},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 3, PeerID: 3},
						{ToStore: 5, PeerID: 5},
					},
				},
				TransferLeader{FromStore: 4, ToStore: 2},
				RemovePeer{FromStore: 3, PeerID: 3},
				RemovePeer{FromStore: 5, PeerID: 5},
			},
			expectedError: nil,
		},
		{
			name: "move region partially with all incoming follower, leader step down",
			originPeers: []*metapb.Peer{
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				1: placement.Follower,
				2: placement.Follower,
				4: placement.Follower,
			},
			steps:         []OpStep{},
			expectedError: errors.New("region need at least 1 voter or leader"),
		},
		{
			name: "only leader transfer",
			originPeers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
			},
			steps: []OpStep{
				TransferLeader{FromStore: 3, ToStore: 4},
			},
		},
		{
			name: "add peer and transfer leader",
			originPeers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
				6: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToStore: 6},
				PromoteLearner{ToStore: 6},
				TransferLeader{FromStore: 3, ToStore: 4},
			},
		},
	}
	for _, tc := range tt {
		c.Log(tc.name)
		region := core.NewRegionInfo(&metapb.Region{Id: 10, Peers: tc.originPeers}, tc.originPeers[0])
		op, err := CreateMoveRegionOperator("test", s.cluster, region, OpAdmin, tc.targetPeerRoles)

		if tc.expectedError == nil {
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, NotNil)
			c.Assert(strings.Contains(err.Error(), tc.expectedError.Error()), IsTrue)
			continue
		}
		c.Assert(op, NotNil)
		c.Assert(op.Len(), Equals, len(tc.steps))
		// Since the peer id may be generated by allocator in runtime, we only check store id.
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				c.Assert(step.FromStore, Equals, tc.steps[i].(TransferLeader).FromStore)
				c.Assert(step.ToStore, Equals, tc.steps[i].(TransferLeader).ToStore)
			case ChangePeerV2Leave:
				c.Assert(len(step.PromoteLearners), Equals, len(tc.steps[i].(ChangePeerV2Leave).PromoteLearners))
				c.Assert(len(step.DemoteVoters), Equals, len(tc.steps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Leave).PromoteLearners {
					c.Assert(step.PromoteLearners[j].ToStore, Equals, p.ToStore)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Leave).DemoteVoters {
					c.Assert(step.DemoteVoters[j].ToStore, Equals, d.ToStore)
				}
			case ChangePeerV2Enter:
				c.Assert(len(step.PromoteLearners), Equals, len(tc.steps[i].(ChangePeerV2Enter).PromoteLearners))
				c.Assert(len(step.DemoteVoters), Equals, len(tc.steps[i].(ChangePeerV2Enter).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Enter).PromoteLearners {
					c.Assert(step.PromoteLearners[j].ToStore, Equals, p.ToStore)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Enter).DemoteVoters {
					c.Assert(step.DemoteVoters[j].ToStore, Equals, d.ToStore)
				}
			case AddLearner:
				c.Assert(step.ToStore, Equals, tc.steps[i].(AddLearner).ToStore)
			case PromoteLearner:
				c.Assert(step.ToStore, Equals, tc.steps[i].(PromoteLearner).ToStore)
			case RemovePeer:
				c.Assert(step.FromStore, Equals, tc.steps[i].(RemovePeer).FromStore)
			default:
				c.Errorf("unexpected type: %s", step.String())
			}
		}
	}
}

func (s *testCreateOperatorSuite) TestMoveRegionWithoutJointConsensus(c *C) {
	type testCase struct {
		name            string
		originPeers     []*metapb.Peer // first is leader
		targetPeerRoles map[uint64]placement.PeerRoleType
		steps           []OpStep
		expectedError   error
	}
	tt := []testCase{
		{
			name: "move region partially with incoming voter, demote existed voter",
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Leader,
				3: placement.Learner,
				4: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4},
				PromoteLearner{ToStore: 4},
				TransferLeader{FromStore: 1, ToStore: 2},
				RemovePeer{FromStore: 1},
				RemovePeer{FromStore: 3},
				AddLearner{ToStore: 3},
			},
		},
		{
			name: "move region partially with incoming leader",
			originPeers: []*metapb.Peer{
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Leader,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4},
				PromoteLearner{ToStore: 4},
				TransferLeader{FromStore: 1, ToStore: 2},
				RemovePeer{FromStore: 1},
				TransferLeader{FromStore: 2, ToStore: 4},
			},
		},
		{
			name: "move region partially with incoming learner, demote leader",
			originPeers: []*metapb.Peer{
				{Id: 2, StoreId: 2, Role: metapb.PeerRole_Voter},
				{Id: 1, StoreId: 1, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Learner,
				3: placement.Voter,
				4: placement.Learner,
			},
			steps: []OpStep{
				RemovePeer{FromStore: 1},
				TransferLeader{FromStore: 2, ToStore: 3},
				RemovePeer{FromStore: 2},
				AddLearner{ToStore: 2},
				AddLearner{ToStore: 4},
			},
		},
		{
			name: "move region partially with all incoming follower, leader step down",
			originPeers: []*metapb.Peer{
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				1: placement.Follower,
				2: placement.Follower,
				4: placement.Follower,
			},
			steps:         []OpStep{},
			expectedError: errors.New("region need at least 1 voter or leader"),
		},
		{
			name: "only leader transfer",
			originPeers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
			},
			steps: []OpStep{
				TransferLeader{FromStore: 3, ToStore: 4},
			},
		},
		{
			name: "add peer and transfer leader",
			originPeers: []*metapb.Peer{
				{Id: 3, StoreId: 3, Role: metapb.PeerRole_Voter},
				{Id: 4, StoreId: 4, Role: metapb.PeerRole_Voter},
				{Id: 5, StoreId: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
				6: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToStore: 6},
				PromoteLearner{ToStore: 6},
				TransferLeader{FromStore: 3, ToStore: 4},
			},
		},
	}

	s.cluster.DisableFeature(versioninfo.JointConsensus)
	for _, tc := range tt {
		c.Log(tc.name)
		region := core.NewRegionInfo(&metapb.Region{Id: 10, Peers: tc.originPeers}, tc.originPeers[0])
		op, err := CreateMoveRegionOperator("test", s.cluster, region, OpAdmin, tc.targetPeerRoles)

		if tc.expectedError == nil {
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, NotNil)
			c.Assert(strings.Contains(err.Error(), tc.expectedError.Error()), IsTrue)
			continue
		}
		c.Log(op)
		c.Assert(op, NotNil)
		c.Assert(op.Len(), Equals, len(tc.steps))
		// Since the peer id may be generated by allocator in runtime, we only check store id.
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				c.Assert(step.FromStore, Equals, tc.steps[i].(TransferLeader).FromStore)
				c.Assert(step.ToStore, Equals, tc.steps[i].(TransferLeader).ToStore)
			case AddLearner:
				c.Assert(step.ToStore, Equals, tc.steps[i].(AddLearner).ToStore)
			case PromoteLearner:
				c.Assert(step.ToStore, Equals, tc.steps[i].(PromoteLearner).ToStore)
			case RemovePeer:
				c.Assert(step.FromStore, Equals, tc.steps[i].(RemovePeer).FromStore)
			default:
				c.Errorf("unexpected type: %s", step.String())
			}
		}
	}
}

var _ = Suite(&testCreateOperatorWithRulesSuite{})

type testCreateOperatorWithRulesSuite struct{}

// Ref https://github.com/tikv/pd/issues/5401
func (s *testCreateOperatorWithRulesSuite) TestCreateLeaveJointStateOperatorWithoutFitRules(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := config.NewTestOptions()
	cluster := mockcluster.NewCluster(ctx, opts)
	err := cluster.SetRules([]*placement.Rule{
		{
			GroupID:     "pd",
			ID:          "default",
			StartKeyHex: hex.EncodeToString([]byte("")),
			EndKeyHex:   hex.EncodeToString([]byte("")),
			Role:        placement.Voter,
			Count:       1,
		},
		{
			GroupID:     "t1",
			ID:          "t1",
			StartKeyHex: hex.EncodeToString([]byte("a")),
			EndKeyHex:   hex.EncodeToString([]byte("b")),
			Role:        placement.Voter,
			Count:       1,
		},
		{
			GroupID:     "t2",
			ID:          "t2",
			StartKeyHex: hex.EncodeToString([]byte("b")),
			EndKeyHex:   hex.EncodeToString([]byte("c")),
			Role:        placement.Voter,
			Count:       1,
		},
	})
	c.Assert(err, IsNil)
	cluster.AddRegionStore(1, 1)
	cluster.AddRegionStore(2, 1)
	cluster.AddRegionStore(3, 1)
	cluster.AddRegionStore(4, 1)
	originPeers := []*metapb.Peer{
		{Id: 3, StoreId: 3, Role: metapb.PeerRole_DemotingVoter},
		{Id: 4, StoreId: 4, Role: metapb.PeerRole_IncomingVoter},
	}

	region := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: originPeers, StartKey: []byte("a"), EndKey: []byte("c")}, originPeers[0])
	op, err := CreateLeaveJointStateOperator("test", cluster, region)
	c.Assert(err, IsNil)
	c.Assert(op.kind, Equals, OpLeader)
	c.Assert(op.steps, HasLen, 2)
	step0 := op.steps[0].(TransferLeader)
	c.Assert(step0.FromStore, Equals, uint64(3))
	c.Assert(step0.ToStore, Equals, uint64(4))
	step1 := op.steps[1].(ChangePeerV2Leave)
	c.Assert(step1.PromoteLearners, HasLen, 1)
	c.Assert(step1.DemoteVoters, HasLen, 1)
	c.Assert(step1.PromoteLearners[0].ToStore, Equals, uint64(4))
	c.Assert(step1.DemoteVoters[0].ToStore, Equals, uint64(3))
}
