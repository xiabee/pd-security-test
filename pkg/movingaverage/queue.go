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
	"github.com/phf/go-queue/queue"
	"github.com/tikv/pd/pkg/syncutil"
)

// SafeQueue is a concurrency safe queue
type SafeQueue struct {
	mu  syncutil.Mutex
	que *queue.Queue
}

// NewSafeQueue return a SafeQueue
func NewSafeQueue() *SafeQueue {
	sq := &SafeQueue{}
	sq.que = queue.New()
	return sq
}

// Init implement init
func (sq *SafeQueue) Init() {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	sq.que.Init()
}

// PushBack implement PushBack
func (sq *SafeQueue) PushBack(v interface{}) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	sq.que.PushBack(v)
}

// PopFront implement PopFront
func (sq *SafeQueue) PopFront() interface{} {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	return sq.que.PopFront()
}

// Clone returns a copy of SafeQueue
func (sq *SafeQueue) Clone() *SafeQueue {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	q := queue.New().Init()
	for i := 0; i < sq.que.Len(); i++ {
		v := sq.que.PopFront()
		sq.que.PushBack(v)
		q.PushBack(v)
	}
	return &SafeQueue{
		que: q,
	}
}
