// Copyright 2022 TiKV Project Authors.
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

package keyspace

import (
	"container/heap"
	"encoding/binary"
	"encoding/hex"
	"regexp"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/tikv/pd/pkg/codec"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/pkg/storage/endpoint"
)

const (
	spaceIDMax = ^uint32(0) >> 8 // 16777215 (Uint24Max) is the maximum value of spaceID.
	// namePattern is a regex that specifies acceptable characters of the keyspace name.
	// Name must be non-empty and contains only alphanumerical, `_` and `-`.
	namePattern = "^[-A-Za-z0-9_]+$"
)

var (
	// ErrKeyspaceNotFound is used to indicate target keyspace does not exist.
	ErrKeyspaceNotFound = errors.New("keyspace does not exist")
	// ErrKeyspaceExists indicates target keyspace already exists.
	// It's used when creating a new keyspace.
	ErrKeyspaceExists = errors.New("keyspace already exists")
	// ErrKeyspaceGroupExists indicates target keyspace group already exists.
	ErrKeyspaceGroupExists = errors.New("keyspace group already exists")
	// ErrKeyspaceGroupNotExists is used to indicate target keyspace group does not exist.
	ErrKeyspaceGroupNotExists = errors.New("keyspace group does not exist")
	// ErrKeyspaceGroupInSplit is used to indicate target keyspace group is in split state.
	ErrKeyspaceGroupInSplit = errors.New("keyspace group is in split state")
	// ErrKeyspaceGroupNotInSplit is used to indicate target keyspace group is not in split state.
	ErrKeyspaceGroupNotInSplit = errors.New("keyspace group is not in split state")
	// ErrKeyspaceNotInKeyspaceGroup is used to indicate target keyspace is not in this keyspace group.
	ErrKeyspaceNotInKeyspaceGroup = errors.New("keyspace is not in this keyspace group")
	// ErrNoAvailableNode is used to indicate no available node in the keyspace group.
	ErrNoAvailableNode  = errors.New("no available node")
	errModifyDefault    = errors.New("cannot modify default keyspace's state")
	errIllegalOperation = errors.New("unknown operation")

	// stateTransitionTable lists all allowed next state for the given current state.
	// Note that transit from any state to itself is allowed for idempotence.
	stateTransitionTable = map[keyspacepb.KeyspaceState][]keyspacepb.KeyspaceState{
		keyspacepb.KeyspaceState_ENABLED:   {keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_DISABLED},
		keyspacepb.KeyspaceState_DISABLED:  {keyspacepb.KeyspaceState_DISABLED, keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_ARCHIVED},
		keyspacepb.KeyspaceState_ARCHIVED:  {keyspacepb.KeyspaceState_ARCHIVED, keyspacepb.KeyspaceState_TOMBSTONE},
		keyspacepb.KeyspaceState_TOMBSTONE: {keyspacepb.KeyspaceState_TOMBSTONE},
	}
	// Only keyspaces in the state specified by allowChangeConfig are allowed to change their config.
	allowChangeConfig = []keyspacepb.KeyspaceState{keyspacepb.KeyspaceState_ENABLED, keyspacepb.KeyspaceState_DISABLED}
)

// validateID check if keyspace falls within the acceptable range.
// It throws errIllegalID when input id is our of range,
// or if it collides with reserved id.
func validateID(id uint32) error {
	if id > spaceIDMax {
		return errors.Errorf("illegal keyspace id %d, larger than spaceID Max %d", id, spaceIDMax)
	}
	if id == DefaultKeyspaceID {
		return errors.Errorf("illegal keyspace id %d, collides with default keyspace id", id)
	}
	return nil
}

// validateName check if user provided name is legal.
// It throws errIllegalName when name contains illegal character,
// or if it collides with reserved name.
func validateName(name string) error {
	isValid, err := regexp.MatchString(namePattern, name)
	if err != nil {
		return err
	}
	if !isValid {
		return errors.Errorf("illegal keyspace name %s, should contain only alphanumerical and underline", name)
	}
	if name == DefaultKeyspaceName {
		return errors.Errorf("illegal keyspace name %s, collides with default keyspace name", name)
	}
	return nil
}

// keyspaceIDHash is used to hash the spaceID inside the lockGroup.
// A simple mask is applied to spaceID to use its last byte as map key,
// limiting the maximum map length to 256.
// Since keyspaceID is sequentially allocated, this can also reduce the chance
// of collision when comparing with random hashes.
func keyspaceIDHash(id uint32) uint32 {
	return id & 0xFF
}

// makeKeyRanges encodes keyspace ID to correct LabelRule data.
// For a keyspace with id ['a', 'b', 'c'], it has four boundaries:
//
//	Lower bound for raw mode: ['r', 'a', 'b', 'c']
//	Upper bound for raw mode: ['r', 'a', 'b', 'c + 1']
//	Lower bound for txn mode: ['x', 'a', 'b', 'c']
//	Upper bound for txn mode: ['x', 'a', 'b', 'c + 1']
//
// From which it shares the lower bound with keyspace with id ['a', 'b', 'c-1'].
// And shares upper bound with keyspace with id ['a', 'b', 'c + 1'].
// These repeated bound will not cause any problem, as repetitive bound will be ignored during rangeListBuild,
// but provides guard against hole in keyspace allocations should it occur.
func makeKeyRanges(id uint32) []interface{} {
	keyspaceIDBytes := make([]byte, 4)
	nextKeyspaceIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyspaceIDBytes, id)
	binary.BigEndian.PutUint32(nextKeyspaceIDBytes, id+1)
	rawLeftBound := hex.EncodeToString(codec.EncodeBytes(append([]byte{'r'}, keyspaceIDBytes[1:]...)))
	rawRightBound := hex.EncodeToString(codec.EncodeBytes(append([]byte{'r'}, nextKeyspaceIDBytes[1:]...)))
	txnLeftBound := hex.EncodeToString(codec.EncodeBytes(append([]byte{'x'}, keyspaceIDBytes[1:]...)))
	txnRightBound := hex.EncodeToString(codec.EncodeBytes(append([]byte{'x'}, nextKeyspaceIDBytes[1:]...)))
	return []interface{}{
		map[string]interface{}{
			"start_key": rawLeftBound,
			"end_key":   rawRightBound,
		},
		map[string]interface{}{
			"start_key": txnLeftBound,
			"end_key":   txnRightBound,
		},
	}
}

// getRegionLabelID returns the region label id of the target keyspace.
func getRegionLabelID(id uint32) string {
	return regionLabelIDPrefix + strconv.FormatUint(uint64(id), endpoint.SpaceIDBase)
}

// makeLabelRule makes the label rule for the given keyspace id.
func makeLabelRule(id uint32) *labeler.LabelRule {
	return &labeler.LabelRule{
		ID:    getRegionLabelID(id),
		Index: 0,
		Labels: []labeler.RegionLabel{
			{
				Key:   regionLabelKey,
				Value: strconv.FormatUint(uint64(id), endpoint.SpaceIDBase),
			},
		},
		RuleType: labeler.KeyRange,
		Data:     makeKeyRanges(id),
	}
}

// indexedHeap is a heap with index.
type indexedHeap struct {
	items []*endpoint.KeyspaceGroup
	// keyspace group id -> position in items
	index map[uint32]int
}

func newIndexedHeap(hint int) *indexedHeap {
	return &indexedHeap{
		items: make([]*endpoint.KeyspaceGroup, 0, hint),
		index: map[uint32]int{},
	}
}

// Implementing heap.Interface.
func (hp *indexedHeap) Len() int {
	return len(hp.items)
}

// Implementing heap.Interface.
func (hp *indexedHeap) Less(i, j int) bool {
	// Gives the keyspace group with the least number of keyspaces first
	return len(hp.items[j].Keyspaces) > len(hp.items[i].Keyspaces)
}

// Implementing heap.Interface.
func (hp *indexedHeap) Swap(i, j int) {
	lid := hp.items[i].ID
	rid := hp.items[j].ID
	hp.items[i], hp.items[j] = hp.items[j], hp.items[i]
	hp.index[lid] = j
	hp.index[rid] = i
}

// Implementing heap.Interface.
func (hp *indexedHeap) Push(x interface{}) {
	item := x.(*endpoint.KeyspaceGroup)
	hp.index[item.ID] = hp.Len()
	hp.items = append(hp.items, item)
}

// Implementing heap.Interface.
func (hp *indexedHeap) Pop() interface{} {
	l := hp.Len()
	item := hp.items[l-1]
	hp.items = hp.items[:l-1]
	delete(hp.index, item.ID)
	return item
}

// Top returns the top item.
func (hp *indexedHeap) Top() *endpoint.KeyspaceGroup {
	if hp.Len() <= 0 {
		return nil
	}
	return hp.items[0]
}

// Get returns item with the given ID.
func (hp *indexedHeap) Get(id uint32) *endpoint.KeyspaceGroup {
	idx, ok := hp.index[id]
	if !ok {
		return nil
	}
	item := hp.items[idx]
	return item
}

// GetAll returns all the items.
func (hp *indexedHeap) GetAll() []*endpoint.KeyspaceGroup {
	all := make([]*endpoint.KeyspaceGroup, len(hp.items))
	copy(all, hp.items)
	return all
}

// Put inserts item or updates the old item if it exists.
func (hp *indexedHeap) Put(item *endpoint.KeyspaceGroup) (isUpdate bool) {
	if idx, ok := hp.index[item.ID]; ok {
		hp.items[idx] = item
		heap.Fix(hp, idx)
		return true
	}
	heap.Push(hp, item)
	return false
}

// Remove deletes item by ID and returns it.
func (hp *indexedHeap) Remove(id uint32) *endpoint.KeyspaceGroup {
	if idx, ok := hp.index[id]; ok {
		item := heap.Remove(hp, idx)
		return item.(*endpoint.KeyspaceGroup)
	}
	return nil
}
