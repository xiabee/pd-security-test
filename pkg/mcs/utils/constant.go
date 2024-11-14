// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import "time"

const (
	// TCPNetworkStr is the string of tcp network
	TCPNetworkStr = "tcp"

	// DefaultEnableGRPCGateway is the default value of EnableGRPCGateway
	DefaultEnableGRPCGateway = true
	// DefaultGRPCGracefulStopTimeout is the default timeout to wait for grpc server to gracefully stop
	DefaultGRPCGracefulStopTimeout = 5 * time.Second
	// DefaultHTTPGracefulShutdownTimeout is the default timeout to wait for http server to gracefully shutdown
	DefaultHTTPGracefulShutdownTimeout = 5 * time.Second
	// DefaultLogFormat is the default log format
	DefaultLogFormat = "text"
	// DefaultDisableErrorVerbose is the default value of DisableErrorVerbose
	DefaultDisableErrorVerbose = true
	// DefaultLeaderLease is the default value of LeaderLease
	DefaultLeaderLease = int64(3)
	// LeaderTickInterval is the interval to check leader
	LeaderTickInterval = 50 * time.Millisecond

	// DefaultKeyspaceID is the default key space id.
	// Valid keyspace id range is [0, 0xFFFFFF](uint24max, or 16777215)
	// ​0 is reserved for default keyspace with the name "DEFAULT", It's initialized when PD bootstrap
	// and reserved for users who haven't been assigned keyspace.
	DefaultKeyspaceID = uint32(0)

	// DefaultKeyspaceGroupID is the default key space group id.
	// We also reserved 0 for the keyspace group for the same purpose.
	DefaultKeyspaceGroupID = uint32(0)

	// APIServiceName is the name of api server.
	APIServiceName = "api"
	// TSOServiceName is the name of tso server.
	TSOServiceName = "tso"
	// ResourceManagerServiceName is the name of resource manager server.
	ResourceManagerServiceName = "resource_manager"

	// MaxKeyspaceGroupCount is the max count of keyspace groups. keyspace group in tso
	// is the sharding unit, i.e., by the definition here, the max count of the shards
	// that we support is MaxKeyspaceGroupCount. The keyspace group id is in the range
	// [0, 99999], which explains we use five-digits number (%05d) to render the keyspace
	// group id in the storage endpoint path.
	MaxKeyspaceGroupCount = uint32(100000)
	// MaxKeyspaceGroupCountInUse is the max count of keyspace groups in use, which should
	// never exceed MaxKeyspaceGroupCount defined above. Compared to MaxKeyspaceGroupCount,
	// MaxKeyspaceGroupCountInUse is a much more reasonable value of the max count in the
	// foreseen future, and the former is just for extensibility in theory.
	MaxKeyspaceGroupCountInUse = uint32(4096)
)
