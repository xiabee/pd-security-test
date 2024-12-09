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

package simulator

import (
	"fmt"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/docker/go-units"
	"github.com/tikv/pd/pkg/tempurl"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/schedule/placement"
	"github.com/tikv/pd/server/versioninfo"
)

const (
	// tick
	defaultSimTickInterval = 100 * time.Millisecond
	// store
	defaultStoreIOMBPerSecond = 40
	defaultStoreHeartbeat     = 10 * time.Second
	defaultRegionHeartbeat    = 1 * time.Minute
	defaultRegionSplitKeys    = 960000
	defaultRegionSplitSize    = 96 * units.MiB
	defaultCapacity           = 1 * units.TiB
	defaultExtraUsedSpace     = 0
	// server
	defaultLeaderLease                 = 3
	defaultTSOSaveInterval             = 200 * time.Millisecond
	defaultTickInterval                = 100 * time.Millisecond
	defaultElectionInterval            = 3 * time.Second
	defaultLeaderPriorityCheckInterval = 100 * time.Millisecond
)

// SimConfig is the simulator configuration.
type SimConfig struct {
	// tick
	CaseName        string            `toml:"case-name"`
	SimTickInterval typeutil.Duration `toml:"sim-tick-interval"`
	// store
	StoreIOMBPerSecond int64       `toml:"store-io-per-second"`
	StoreVersion       string      `toml:"store-version"`
	RaftStore          RaftStore   `toml:"raftstore"`
	Coprocessor        Coprocessor `toml:"coprocessor"`
	// server
	ServerConfig *config.Config `toml:"server"`
}

// RaftStore the configuration for raft store.
type RaftStore struct {
	Capacity                typeutil.ByteSize `toml:"capacity" json:"capacity"`
	ExtraUsedSpace          typeutil.ByteSize `toml:"extra-used-space" json:"extra-used-space"`
	RegionHeartBeatInterval typeutil.Duration `toml:"pd-heartbeat-tick-interval" json:"pd-heartbeat-tick-interval"`
	StoreHeartBeatInterval  typeutil.Duration `toml:"pd-store-heartbeat-tick-interval" json:"pd-store-heartbeat-tick-interval"`
}

// Coprocessor the configuration for coprocessor.
type Coprocessor struct {
	RegionSplitSize typeutil.ByteSize `toml:"region-split-size" json:"region-split-size"`
	RegionSplitKey  uint64            `toml:"region-split-keys" json:"region-split-keys"`
}

// NewSimConfig create a new configuration of the simulator.
func NewSimConfig(serverLogLevel string) *SimConfig {
	config.DefaultStoreLimit = config.StoreLimit{AddPeer: 2000, RemovePeer: 2000}
	cfg := &config.Config{
		Name:       "pd",
		ClientUrls: tempurl.Alloc(),
		PeerUrls:   tempurl.Alloc(),
	}

	cfg.AdvertiseClientUrls = cfg.ClientUrls
	cfg.AdvertisePeerUrls = cfg.PeerUrls
	cfg.DataDir, _ = os.MkdirTemp("/tmp", "test_pd")
	cfg.InitialCluster = fmt.Sprintf("pd=%s", cfg.PeerUrls)
	cfg.Log.Level = serverLogLevel
	return &SimConfig{ServerConfig: cfg}
}

func adjustDuration(v *typeutil.Duration, defValue time.Duration) {
	if v.Duration == 0 {
		v.Duration = defValue
	}
}

func adjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

func adjustUint64(v *uint64, defValue uint64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustInt64(v *int64, defValue int64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustByteSize(v *typeutil.ByteSize, defValue typeutil.ByteSize) {
	if *v == 0 {
		*v = defValue
	}
}

// Adjust is used to adjust configurations
func (sc *SimConfig) Adjust(meta *toml.MetaData) error {
	adjustDuration(&sc.SimTickInterval, defaultSimTickInterval)
	adjustInt64(&sc.StoreIOMBPerSecond, defaultStoreIOMBPerSecond)
	adjustString(&sc.StoreVersion, versioninfo.PDReleaseVersion)
	adjustDuration(&sc.RaftStore.RegionHeartBeatInterval, defaultRegionHeartbeat)
	adjustDuration(&sc.RaftStore.StoreHeartBeatInterval, defaultStoreHeartbeat)
	adjustByteSize(&sc.RaftStore.Capacity, defaultCapacity)
	adjustByteSize(&sc.RaftStore.ExtraUsedSpace, defaultExtraUsedSpace)
	adjustUint64(&sc.Coprocessor.RegionSplitKey, defaultRegionSplitKeys)
	adjustByteSize(&sc.Coprocessor.RegionSplitSize, defaultRegionSplitSize)

	adjustInt64(&sc.ServerConfig.LeaderLease, defaultLeaderLease)
	adjustDuration(&sc.ServerConfig.TSOSaveInterval, defaultTSOSaveInterval)
	adjustDuration(&sc.ServerConfig.TickInterval, defaultTickInterval)
	adjustDuration(&sc.ServerConfig.ElectionInterval, defaultElectionInterval)
	adjustDuration(&sc.ServerConfig.LeaderPriorityCheckInterval, defaultLeaderPriorityCheckInterval)

	return sc.ServerConfig.Adjust(meta, false)
}

// PDConfig saves some config which may be changed in PD.
type PDConfig struct {
	PlacementRules []*placement.Rule
	LocationLabels typeutil.StringSlice
}
