package simulator

import (
	"fmt"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/tikv/pd/pkg/tempurl"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/config"
)

const (
	// tick
	defaultSimTickInterval = 100 * time.Millisecond
	// store
	defaultStoreCapacityGB    = 1024
	defaultStoreAvailableGB   = 1024
	defaultStoreIOMBPerSecond = 40
	defaultStoreVersion       = "2.1.0"
	// server
	defaultLeaderLease                 = 1
	defaultTSOSaveInterval             = 200 * time.Millisecond
	defaultTickInterval                = 100 * time.Millisecond
	defaultElectionInterval            = 3 * time.Second
	defaultLeaderPriorityCheckInterval = 100 * time.Millisecond
)

// SimConfig is the simulator configuration.
type SimConfig struct {
	// tick
	SimTickInterval typeutil.Duration `toml:"sim-tick-interval"`
	// store
	StoreCapacityGB    uint64 `toml:"store-capacity"`
	StoreAvailableGB   uint64 `toml:"store-available"`
	StoreIOMBPerSecond int64  `toml:"store-io-per-second"`
	StoreVersion       string `toml:"store-version"`
	// server
	ServerConfig *config.Config `toml:"server"`
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

// Adjust is used to adjust configurations
func (sc *SimConfig) Adjust(meta *toml.MetaData) error {
	adjustDuration(&sc.SimTickInterval, defaultSimTickInterval)
	adjustUint64(&sc.StoreCapacityGB, defaultStoreCapacityGB)
	adjustUint64(&sc.StoreAvailableGB, defaultStoreAvailableGB)
	adjustInt64(&sc.StoreIOMBPerSecond, defaultStoreIOMBPerSecond)
	adjustString(&sc.StoreVersion, defaultStoreVersion)
	adjustInt64(&sc.ServerConfig.LeaderLease, defaultLeaderLease)
	adjustDuration(&sc.ServerConfig.TSOSaveInterval, defaultTSOSaveInterval)
	adjustDuration(&sc.ServerConfig.TickInterval, defaultTickInterval)
	adjustDuration(&sc.ServerConfig.ElectionInterval, defaultElectionInterval)
	adjustDuration(&sc.ServerConfig.LeaderPriorityCheckInterval, defaultLeaderPriorityCheckInterval)

	return sc.ServerConfig.Adjust(meta, false)
}
