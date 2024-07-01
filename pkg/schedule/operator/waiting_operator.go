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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"math/rand"
)

// priorityWeight is used to represent the weight of different priorities of operators.
var priorityWeight = []float64{1.0, 4.0, 9.0, 16.0}

// WaitingOperator is an interface of waiting operators.
type WaitingOperator interface {
	PutOperator(op *Operator)
	GetOperator() []*Operator
	ListOperator() []*Operator
}

// bucket is used to maintain the operators created by a specific scheduler.
type bucket struct {
	weight float64
	ops    []*Operator
}

// randBuckets is an implementation of waiting operators
type randBuckets struct {
	totalWeight float64
	buckets     []*bucket
}

// newRandBuckets creates a random buckets.
func newRandBuckets() *randBuckets {
	var buckets []*bucket
	for i := 0; i < len(priorityWeight); i++ {
		buckets = append(buckets, &bucket{
			weight: priorityWeight[i],
		})
	}
	return &randBuckets{buckets: buckets}
}

// PutOperator puts an operator into the random buckets.
func (b *randBuckets) PutOperator(op *Operator) {
	priority := op.GetPriorityLevel()
	bucket := b.buckets[priority]
	if len(bucket.ops) == 0 {
		b.totalWeight += bucket.weight
	}
	bucket.ops = append(bucket.ops, op)
}

// ListOperator lists all operator in the random buckets.
func (b *randBuckets) ListOperator() []*Operator {
	var ops []*Operator
	for i := range b.buckets {
		bucket := b.buckets[i]
		ops = append(ops, bucket.ops...)
	}
	return ops
}

// GetOperator gets an operator from the random buckets.
func (b *randBuckets) GetOperator() []*Operator {
	if b.totalWeight == 0 {
		return nil
	}
	r := rand.Float64()
	var sum float64
	for i := range b.buckets {
		bucket := b.buckets[i]
		if len(bucket.ops) == 0 {
			continue
		}
		proportion := bucket.weight / b.totalWeight
		if r >= sum && r < sum+proportion {
			var res []*Operator
			res = append(res, bucket.ops[0])
			// Merge operation has two operators, and thus it should be handled specifically.
			if bucket.ops[0].Kind()&OpMerge != 0 {
				res = append(res, bucket.ops[1])
				bucket.ops = bucket.ops[2:]
			} else {
				bucket.ops = bucket.ops[1:]
			}
			if len(bucket.ops) == 0 {
				b.totalWeight -= bucket.weight
			}
			return res
		}
		sum += proportion
	}
	return nil
}

// waitingOperatorStatus is used to limit the count of each kind of operators.
type waitingOperatorStatus struct {
	ops map[string]uint64
}

// newWaitingOperatorStatus creates a new waitingOperatorStatus.
func newWaitingOperatorStatus() *waitingOperatorStatus {
	return &waitingOperatorStatus{
		make(map[string]uint64),
	}
}
