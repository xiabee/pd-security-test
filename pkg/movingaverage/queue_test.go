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

package movingaverage

import (
	. "github.com/pingcap/check"
)

func (t *testMovingAvg) TestQueue(c *C) {
	sq := NewSafeQueue()
	sq.PushBack(1)
	sq.PushBack(2)
	v1 := sq.PopFront()
	v2 := sq.PopFront()
	c.Assert(1, Equals, v1.(int))
	c.Assert(2, Equals, v2.(int))
}

func (t *testMovingAvg) TestClone(c *C) {
	s1 := NewSafeQueue()
	s1.PushBack(1)
	s1.PushBack(2)
	s2 := s1.Clone()
	s2.PopFront()
	s2.PopFront()
	c.Assert(s1.que.Len(), Equals, 2)
	c.Assert(s2.que.Len(), Equals, 0)
}
