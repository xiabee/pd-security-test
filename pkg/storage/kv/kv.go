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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

// Base is an abstract interface for load/save pd cluster data.
type Base interface {
	Load(key string) (string, error)
	LoadRange(key, endKey string, limit int) (keys []string, values []string, err error)
	Save(key, value string) error
	Remove(key string) error
}
