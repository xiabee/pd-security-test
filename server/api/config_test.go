// Copyright 2016 TiKV Project Authors.
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

package api

import (
	"encoding/json"
	"fmt"
	"time"

	. "github.com/pingcap/check"
	tu "github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/versioninfo"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testConfigSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c, func(cfg *config.Config) {
		cfg.Replication.EnablePlacementRules = false
	})
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
}

func (s *testConfigSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testConfigSuite) TestConfigAll(c *C) {
	addr := fmt.Sprintf("%s/config", s.urlPrefix)
	cfg := &config.Config{}
	err := tu.ReadGetJSON(c, testDialClient, addr, cfg)
	c.Assert(err, IsNil)

	// the original way
	r := map[string]int{"max-replicas": 5}
	postData, err := json.Marshal(r)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c))
	c.Assert(err, IsNil)
	l := map[string]interface{}{
		"location-labels":       "zone,rack",
		"region-schedule-limit": 10,
	}
	postData, err = json.Marshal(l)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c))
	c.Assert(err, IsNil)

	l = map[string]interface{}{
		"metric-storage": "http://127.0.0.1:9090",
	}
	postData, err = json.Marshal(l)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c))
	c.Assert(err, IsNil)

	newCfg := &config.Config{}
	err = tu.ReadGetJSON(c, testDialClient, addr, newCfg)
	c.Assert(err, IsNil)
	cfg.Replication.MaxReplicas = 5
	cfg.Replication.LocationLabels = []string{"zone", "rack"}
	cfg.Schedule.RegionScheduleLimit = 10
	cfg.PDServerCfg.MetricStorage = "http://127.0.0.1:9090"
	c.Assert(cfg, DeepEquals, newCfg)

	// the new way
	l = map[string]interface{}{
		"schedule.tolerant-size-ratio":            2.5,
		"replication.location-labels":             "idc,host",
		"pd-server.metric-storage":                "http://127.0.0.1:1234",
		"log.level":                               "warn",
		"cluster-version":                         "v4.0.0-beta",
		"replication-mode.replication-mode":       "dr-auto-sync",
		"replication-mode.dr-auto-sync.label-key": "foobar",
	}
	postData, err = json.Marshal(l)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c))
	c.Assert(err, IsNil)
	newCfg1 := &config.Config{}
	err = tu.ReadGetJSON(c, testDialClient, addr, newCfg1)
	c.Assert(err, IsNil)
	cfg.Schedule.TolerantSizeRatio = 2.5
	cfg.Replication.LocationLabels = []string{"idc", "host"}
	cfg.PDServerCfg.MetricStorage = "http://127.0.0.1:1234"
	cfg.Log.Level = "warn"
	cfg.ReplicationMode.DRAutoSync.LabelKey = "foobar"
	cfg.ReplicationMode.ReplicationMode = "dr-auto-sync"
	v, err := versioninfo.ParseVersion("v4.0.0-beta")
	c.Assert(err, IsNil)
	cfg.ClusterVersion = *v
	c.Assert(newCfg1, DeepEquals, cfg)

	postData, err = json.Marshal(l)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c))
	c.Assert(err, IsNil)

	// illegal prefix
	l = map[string]interface{}{
		"replicate.max-replicas": 1,
	}
	postData, err = json.Marshal(l)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData,
		tu.StatusNotOK(c),
		tu.StringContain(c, "not found"))
	c.Assert(err, IsNil)

	// update prefix directly
	l = map[string]interface{}{
		"replication-mode": nil,
	}
	postData, err = json.Marshal(l)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData,
		tu.StatusNotOK(c),
		tu.StringContain(c, "cannot update config prefix"))
	c.Assert(err, IsNil)

	// config item not found
	l = map[string]interface{}{
		"schedule.region-limit": 10,
	}
	postData, err = json.Marshal(l)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusNotOK(c), tu.StringContain(c, "not found"))
	c.Assert(err, IsNil)
}

func (s *testConfigSuite) TestConfigSchedule(c *C) {
	addr := fmt.Sprintf("%s/config/schedule", s.urlPrefix)
	sc := &config.ScheduleConfig{}
	c.Assert(tu.ReadGetJSON(c, testDialClient, addr, sc), IsNil)

	sc.MaxStoreDownTime.Duration = time.Second
	postData, err := json.Marshal(sc)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c))
	c.Assert(err, IsNil)

	sc1 := &config.ScheduleConfig{}
	c.Assert(tu.ReadGetJSON(c, testDialClient, addr, sc1), IsNil)
	c.Assert(*sc, DeepEquals, *sc1)
}

func (s *testConfigSuite) TestConfigReplication(c *C) {
	addr := fmt.Sprintf("%s/config/replicate", s.urlPrefix)
	rc := &config.ReplicationConfig{}
	err := tu.ReadGetJSON(c, testDialClient, addr, rc)
	c.Assert(err, IsNil)

	rc.MaxReplicas = 5
	rc1 := map[string]int{"max-replicas": 5}
	postData, err := json.Marshal(rc1)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c))
	c.Assert(err, IsNil)

	rc.LocationLabels = []string{"zone", "rack"}
	rc2 := map[string]string{"location-labels": "zone,rack"}
	postData, err = json.Marshal(rc2)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c))
	c.Assert(err, IsNil)

	rc.IsolationLevel = "zone"
	rc3 := map[string]string{"isolation-level": "zone"}
	postData, err = json.Marshal(rc3)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c))
	c.Assert(err, IsNil)

	rc4 := &config.ReplicationConfig{}
	err = tu.ReadGetJSON(c, testDialClient, addr, rc4)
	c.Assert(err, IsNil)

	c.Assert(*rc, DeepEquals, *rc4)
}

func (s *testConfigSuite) TestConfigLabelProperty(c *C) {
	addr := s.svr.GetAddr() + apiPrefix + "/api/v1/config/label-property"

	loadProperties := func() config.LabelPropertyConfig {
		var cfg config.LabelPropertyConfig
		err := tu.ReadGetJSON(c, testDialClient, addr, &cfg)
		c.Assert(err, IsNil)
		return cfg
	}

	cfg := loadProperties()
	c.Assert(cfg, HasLen, 0)

	cmds := []string{
		`{"type": "foo", "action": "set", "label-key": "zone", "label-value": "cn1"}`,
		`{"type": "foo", "action": "set", "label-key": "zone", "label-value": "cn2"}`,
		`{"type": "bar", "action": "set", "label-key": "host", "label-value": "h1"}`,
	}
	for _, cmd := range cmds {
		err := tu.CheckPostJSON(testDialClient, addr, []byte(cmd), tu.StatusOK(c))
		c.Assert(err, IsNil)
	}

	cfg = loadProperties()
	c.Assert(cfg, HasLen, 2)
	c.Assert(cfg["foo"], DeepEquals, []config.StoreLabel{
		{Key: "zone", Value: "cn1"},
		{Key: "zone", Value: "cn2"},
	})
	c.Assert(cfg["bar"], DeepEquals, []config.StoreLabel{{Key: "host", Value: "h1"}})

	cmds = []string{
		`{"type": "foo", "action": "delete", "label-key": "zone", "label-value": "cn1"}`,
		`{"type": "bar", "action": "delete", "label-key": "host", "label-value": "h1"}`,
	}
	for _, cmd := range cmds {
		err := tu.CheckPostJSON(testDialClient, addr, []byte(cmd), tu.StatusOK(c))
		c.Assert(err, IsNil)
	}

	cfg = loadProperties()
	c.Assert(cfg, HasLen, 1)
	c.Assert(cfg["foo"], DeepEquals, []config.StoreLabel{{Key: "zone", Value: "cn2"}})
}

func (s *testConfigSuite) TestConfigDefault(c *C) {
	addr := fmt.Sprintf("%s/config", s.urlPrefix)

	r := map[string]int{"max-replicas": 5}
	postData, err := json.Marshal(r)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c))
	c.Assert(err, IsNil)
	l := map[string]interface{}{
		"location-labels":       "zone,rack",
		"region-schedule-limit": 10,
	}
	postData, err = json.Marshal(l)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c))
	c.Assert(err, IsNil)

	l = map[string]interface{}{
		"metric-storage": "http://127.0.0.1:9090",
	}
	postData, err = json.Marshal(l)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c))
	c.Assert(err, IsNil)

	addr = fmt.Sprintf("%s/config/default", s.urlPrefix)
	defaultCfg := &config.Config{}
	err = tu.ReadGetJSON(c, testDialClient, addr, defaultCfg)
	c.Assert(err, IsNil)

	c.Assert(defaultCfg.Replication.MaxReplicas, Equals, uint64(3))
	c.Assert(defaultCfg.Replication.LocationLabels, DeepEquals, typeutil.StringSlice([]string{}))
	c.Assert(defaultCfg.Schedule.RegionScheduleLimit, Equals, uint64(2048))
	c.Assert(defaultCfg.PDServerCfg.MetricStorage, Equals, "")
}

func (s *testConfigSuite) TestConfigPDServer(c *C) {
	addrPost := fmt.Sprintf("%s/config", s.urlPrefix)

	ms := map[string]interface{}{
		"metric-storage": "",
	}
	postData, err := json.Marshal(ms)
	c.Assert(err, IsNil)
	c.Assert(tu.CheckPostJSON(testDialClient, addrPost, postData, tu.StatusOK(c)), IsNil)

	addrGet := fmt.Sprintf("%s/config/pd-server", s.urlPrefix)
	sc := &config.PDServerConfig{}
	c.Assert(tu.ReadGetJSON(c, testDialClient, addrGet, sc), IsNil)

	c.Assert(sc.UseRegionStorage, Equals, bool(true))
	c.Assert(sc.KeyType, Equals, "table")
	c.Assert(sc.RuntimeServices, DeepEquals, typeutil.StringSlice([]string{}))
	c.Assert(sc.MetricStorage, Equals, "")
	c.Assert(sc.DashboardAddress, Equals, "auto")
	c.Assert(sc.FlowRoundByDigit, Equals, int(3))
	c.Assert(sc.MinResolvedTSPersistenceInterval, Equals, typeutil.NewDuration(0))
	c.Assert(sc.MaxResetTSGap.Duration, Equals, 24*time.Hour)
}

var ttlConfig = map[string]interface{}{
	"schedule.max-snapshot-count":             999,
	"schedule.enable-location-replacement":    false,
	"schedule.max-merge-region-size":          999,
	"schedule.max-merge-region-keys":          999,
	"schedule.scheduler-max-waiting-operator": 999,
	"schedule.leader-schedule-limit":          999,
	"schedule.region-schedule-limit":          999,
	"schedule.hot-region-schedule-limit":      999,
	"schedule.replica-schedule-limit":         999,
	"schedule.merge-schedule-limit":           999,
}

var invalidTTLConfig = map[string]interface{}{
	"schedule.invalid-ttl-config": 0,
}

func assertTTLConfig(c *C, options *config.PersistOptions, checker Checker) {
	c.Assert(options.GetMaxSnapshotCount(), checker, uint64(999))
	c.Assert(options.IsLocationReplacementEnabled(), checker, false)
	c.Assert(options.GetMaxMergeRegionSize(), checker, uint64(999))
	c.Assert(options.GetMaxMergeRegionKeys(), checker, uint64(999))
	c.Assert(options.GetSchedulerMaxWaitingOperator(), checker, uint64(999))
	c.Assert(options.GetLeaderScheduleLimit(), checker, uint64(999))
	c.Assert(options.GetRegionScheduleLimit(), checker, uint64(999))
	c.Assert(options.GetHotRegionScheduleLimit(), checker, uint64(999))
	c.Assert(options.GetReplicaScheduleLimit(), checker, uint64(999))
	c.Assert(options.GetMergeScheduleLimit(), checker, uint64(999))
}

func createTTLUrl(url string, ttl int) string {
	return fmt.Sprintf("%s/config?ttlSecond=%d", url, ttl)
}

func (s *testConfigSuite) TestConfigTTL(c *C) {
	postData, err := json.Marshal(ttlConfig)
	c.Assert(err, IsNil)

	// test no config and cleaning up
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(s.urlPrefix, 0), postData, tu.StatusOK(c))
	c.Assert(err, IsNil)
	assertTTLConfig(c, s.svr.GetPersistOptions(), Not(Equals))

	// test time goes by
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(s.urlPrefix, 1), postData, tu.StatusOK(c))
	c.Assert(err, IsNil)
	assertTTLConfig(c, s.svr.GetPersistOptions(), Equals)
	time.Sleep(2 * time.Second)
	assertTTLConfig(c, s.svr.GetPersistOptions(), Not(Equals))

	// test cleaning up
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(s.urlPrefix, 1), postData, tu.StatusOK(c))
	c.Assert(err, IsNil)
	assertTTLConfig(c, s.svr.GetPersistOptions(), Equals)
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(s.urlPrefix, 0), postData, tu.StatusOK(c))
	c.Assert(err, IsNil)
	assertTTLConfig(c, s.svr.GetPersistOptions(), Not(Equals))

	postData, err = json.Marshal(invalidTTLConfig)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(s.urlPrefix, 1), postData,
		tu.StatusNotOK(c), tu.StringEqual(c, "\"unsupported ttl config schedule.invalid-ttl-config\"\n"))
	c.Assert(err, IsNil)
}

func (s *testConfigSuite) TestTTLConflict(c *C) {
	addr := createTTLUrl(s.urlPrefix, 1)
	postData, err := json.Marshal(ttlConfig)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c))
	c.Assert(err, IsNil)
	assertTTLConfig(c, s.svr.GetPersistOptions(), Equals)

	cfg := map[string]interface{}{"max-snapshot-count": 30}
	postData, err = json.Marshal(cfg)
	c.Assert(err, IsNil)
	addr = fmt.Sprintf("%s/config", s.urlPrefix)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusNotOK(c), tu.StringEqual(c, "\"need to clean up TTL first for schedule.max-snapshot-count\"\n"))
	c.Assert(err, IsNil)
	addr = fmt.Sprintf("%s/config/schedule", s.urlPrefix)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusNotOK(c), tu.StringEqual(c, "\"need to clean up TTL first for schedule.max-snapshot-count\"\n"))
	c.Assert(err, IsNil)
	cfg = map[string]interface{}{"schedule.max-snapshot-count": 30}
	postData, err = json.Marshal(cfg)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, createTTLUrl(s.urlPrefix, 0), postData, tu.StatusOK(c))
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, addr, postData, tu.StatusOK(c))
	c.Assert(err, IsNil)
}
