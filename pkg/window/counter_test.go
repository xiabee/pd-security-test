// The MIT License (MIT)
// Copyright (c) 2022 go-kratos Project Authors.
//
// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,g
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package window

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRollingCounterAdd(t *testing.T) {
	re := require.New(t)
	size := 3
	bucketDuration := time.Second
	opts := RollingCounterOpts{
		Size:           size,
		BucketDuration: bucketDuration,
	}
	r := NewRollingCounter(opts)
	listBuckets := func() [][]float64 {
		buckets := make([][]float64, 0)
		r.Reduce(func(i Iterator) float64 {
			for i.Next() {
				bucket := i.Bucket()
				buckets = append(buckets, bucket.Points)
			}
			return 0.0
		})
		return buckets
	}
	re.Equal([][]float64{{}, {}, {}}, listBuckets())
	r.Add(1)
	re.Equal([][]float64{{}, {}, {1}}, listBuckets())
	time.Sleep(time.Second)
	r.Add(2)
	r.Add(3)
	re.Equal([][]float64{{}, {1}, {5}}, listBuckets())
	time.Sleep(time.Second)
	r.Add(4)
	r.Add(5)
	r.Add(6)
	re.Equal([][]float64{{1}, {5}, {15}}, listBuckets())
	time.Sleep(time.Second)
	r.Add(7)
	re.Equal([][]float64{{5}, {15}, {7}}, listBuckets())

	// test the given reduce methods.
	re.Less(math.Abs((r.Sum() - 27.)), 1e-7)
	re.Less(math.Abs((r.Max() - 15.)), 1e-7)
	re.Less(math.Abs((r.Min() - 5.)), 1e-7)
	re.Less(math.Abs((r.Avg() - 9.)), 1e-7)
}

func TestRollingCounterReduce(t *testing.T) {
	re := require.New(t)
	size := 3
	bucketDuration := time.Second
	opts := RollingCounterOpts{
		Size:           size,
		BucketDuration: bucketDuration,
	}
	r := NewRollingCounter(opts)
	for x := 0; x < size; x++ {
		for i := 0; i <= x; i++ {
			r.Add(1)
		}
		if x < size-1 {
			time.Sleep(bucketDuration)
		}
	}
	var result = r.Reduce(func(iterator Iterator) float64 {
		var result float64
		for iterator.Next() {
			bucket := iterator.Bucket()
			result += bucket.Points[0]
		}
		return result
	})
	re.Less(math.Abs(result-6.), 1e-7)
	re.Less(math.Abs((r.Sum() - 6.)), 1e-7)
	re.Less(math.Abs(float64(r.Value())-6), 1e-7)
}

func TestRollingCounterDataRace(t *testing.T) {
	size := 3
	bucketDuration := time.Millisecond * 10
	opts := RollingCounterOpts{
		Size:           size,
		BucketDuration: bucketDuration,
	}
	r := NewRollingCounter(opts)
	var stop = make(chan bool)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				r.Add(1)
				time.Sleep(time.Millisecond * 5)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = r.Reduce(func(i Iterator) float64 {
					for i.Next() {
						bucket := i.Bucket()
						for range bucket.Points {
							continue
						}
					}
					return 0
				})
			}
		}
	}()
	time.Sleep(time.Second * 3)
	close(stop)
}

func BenchmarkRollingCounterIncr(b *testing.B) {
	size := 3
	bucketDuration := time.Millisecond * 100
	opts := RollingCounterOpts{
		Size:           size,
		BucketDuration: bucketDuration,
	}
	r := NewRollingCounter(opts)
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		r.Add(1)
	}
}

func BenchmarkRollingCounterReduce(b *testing.B) {
	size := 3
	bucketDuration := time.Second
	opts := RollingCounterOpts{
		Size:           size,
		BucketDuration: bucketDuration,
	}
	r := NewRollingCounter(opts)
	for i := 0; i <= 10; i++ {
		r.Add(1)
		time.Sleep(time.Millisecond * 500)
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		var _ = r.Reduce(func(i Iterator) float64 {
			var result float64
			for i.Next() {
				bucket := i.Bucket()
				if len(bucket.Points) != 0 {
					result += bucket.Points[0]
				}
			}
			return result
		})
	}
}
