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

package tso

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/mcs/discovery"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	tsoapi "github.com/tikv/pd/pkg/mcs/tso/server/apis/v1"
	"github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type tsoServerTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	cluster          *tests.TestCluster
	pdLeader         *tests.TestServer
	backendEndpoints string
}

func TestTSOServerTestSuite(t *testing.T) {
	suite.Run(t, new(tsoServerTestSuite))
}

func (suite *tsoServerTestSuite) SetupSuite() {
	var err error
	re := suite.Require()

	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 1)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
	re.NoError(suite.pdLeader.BootstrapCluster())
}

func (suite *tsoServerTestSuite) TearDownSuite() {
	suite.cluster.Destroy()
	suite.cancel()
}

func (suite *tsoServerTestSuite) TestTSOServerStartAndStopNormally() {
	defer func() {
		if r := recover(); r != nil {
			suite.T().Log("Recovered from an unexpected panic", r)
			suite.T().Errorf("Expected no panic, but something bad occurred with")
		}
	}()

	re := suite.Require()
	s, cleanup := tests.StartSingleTSOTestServer(suite.ctx, re, suite.backendEndpoints, tempurl.Alloc())

	defer cleanup()
	testutil.Eventually(re, func() bool {
		return s.IsServing()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	// Test registered GRPC Service
	cc, err := grpc.DialContext(suite.ctx, s.GetAddr(), grpc.WithInsecure())
	re.NoError(err)
	cc.Close()

	url := s.GetAddr() + tsoapi.APIPathPrefix + "/admin/reset-ts"
	// Test reset ts
	input := []byte(`{"tso":"121312", "force-use-larger":true}`)
	err = testutil.CheckPostJSON(dialClient, url, input,
		testutil.StatusOK(re), testutil.StringContain(re, "Reset ts successfully"))
	re.NoError(err)

	// Test reset ts with invalid tso
	input = []byte(`{}`)
	err = testutil.CheckPostJSON(dialClient, suite.backendEndpoints+"/pd/api/v1/admin/reset-ts", input,
		testutil.StatusNotOK(re), testutil.StringContain(re, "invalid tso value"))
	re.NoError(err)
}

func (suite *tsoServerTestSuite) TestParticipantStartWithAdvertiseListenAddr() {
	re := suite.Require()

	cfg := tso.NewConfig()
	cfg.BackendEndpoints = suite.backendEndpoints
	cfg.ListenAddr = tempurl.Alloc()
	cfg.AdvertiseListenAddr = tempurl.Alloc()
	cfg, err := tso.GenerateConfig(cfg)
	re.NoError(err)

	// Setup the logger.
	err = tests.InitLogger(cfg.Log, cfg.Logger, cfg.LogProps, cfg.Security.RedactInfoLog)
	re.NoError(err)

	s, cleanup, err := tests.NewTSOTestServer(suite.ctx, cfg)
	re.NoError(err)
	defer cleanup()
	testutil.Eventually(re, func() bool {
		return s.IsServing()
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	member, err := s.GetMember(utils.DefaultKeyspaceID, utils.DefaultKeyspaceGroupID)
	re.NoError(err)
	re.Equal(fmt.Sprintf("%s-%05d", cfg.AdvertiseListenAddr, utils.DefaultKeyspaceGroupID), member.Name())
}

func TestTSOPath(t *testing.T) {
	re := require.New(t)
	checkTSOPath(re, true /*isAPIServiceMode*/)
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/tso/server/skipWaitAPIServiceReady", "return(true)"))
	checkTSOPath(re, false /*isAPIServiceMode*/)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/tso/server/skipWaitAPIServiceReady"))
}

func checkTSOPath(re *require.Assertions, isAPIServiceMode bool) {
	var (
		cluster *tests.TestCluster
		err     error
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if isAPIServiceMode {
		cluster, err = tests.NewTestAPICluster(ctx, 1)
	} else {
		cluster, err = tests.NewTestCluster(ctx, 1)
	}
	re.NoError(err)
	defer cluster.Destroy()
	err = cluster.RunInitialServers()
	re.NoError(err)
	leaderName := cluster.WaitLeader()
	pdLeader := cluster.GetServer(leaderName)
	re.NoError(pdLeader.BootstrapCluster())
	backendEndpoints := pdLeader.GetAddr()
	client := pdLeader.GetEtcdClient()
	if isAPIServiceMode {
		re.Equal(0, getEtcdTimestampKeyNum(re, client))
	} else {
		re.Equal(1, getEtcdTimestampKeyNum(re, client))
	}

	_, cleanup := tests.StartSingleTSOTestServer(ctx, re, backendEndpoints, tempurl.Alloc())
	defer cleanup()

	cli := mcs.SetupClientWithAPIContext(ctx, re, pd.NewAPIContextV2(""), []string{backendEndpoints})
	physical, logical, err := cli.GetTS(ctx)
	re.NoError(err)
	ts := tsoutil.ComposeTS(physical, logical)
	re.NotEmpty(ts)
	// After we request the tso server, etcd still has only one key related to the timestamp.
	re.Equal(1, getEtcdTimestampKeyNum(re, client))
}

func getEtcdTimestampKeyNum(re *require.Assertions, client *clientv3.Client) int {
	resp, err := etcdutil.EtcdKVGet(client, "/", clientv3.WithPrefix())
	re.NoError(err)
	var count int
	for _, kv := range resp.Kvs {
		key := strings.TrimSpace(string(kv.Key))
		if !strings.HasSuffix(key, endpoint.TimestampKey) {
			continue
		}
		count++
	}
	return count
}

func TestWaitAPIServiceReady(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startCluster := func(isAPIServiceMode bool) (cluster *tests.TestCluster, backendEndpoints string) {
		var err error
		if isAPIServiceMode {
			cluster, err = tests.NewTestAPICluster(ctx, 1)
		} else {
			cluster, err = tests.NewTestCluster(ctx, 1)
		}
		re.NoError(err)
		err = cluster.RunInitialServers()
		re.NoError(err)
		leaderName := cluster.WaitLeader()
		pdLeader := cluster.GetServer(leaderName)
		return cluster, pdLeader.GetAddr()
	}

	// tso server cannot be started because the pd server is not ready as api service.
	cluster, backendEndpoints := startCluster(false /*isAPIServiceMode*/)
	sctx, scancel := context.WithTimeout(ctx, time.Second*10)
	defer scancel()
	s, _, err := tests.StartSingleTSOTestServerWithoutCheck(sctx, re, backendEndpoints, tempurl.Alloc())
	re.Error(err)
	re.Nil(s)
	cluster.Destroy()

	// tso server can be started because the pd server is ready as api service.
	cluster, backendEndpoints = startCluster(true /*isAPIServiceMode*/)
	sctx, scancel = context.WithTimeout(ctx, time.Second*10)
	defer scancel()
	s, cleanup, err := tests.StartSingleTSOTestServerWithoutCheck(sctx, re, backendEndpoints, tempurl.Alloc())
	re.NoError(err)
	defer cluster.Destroy()
	defer cleanup()

	for i := 0; i < 12; i++ {
		select {
		case <-time.After(time.Second):
		case <-sctx.Done():
			return
		}
		if s != nil && s.IsServing() {
			break
		}
	}
}

type APIServerForwardTestSuite struct {
	suite.Suite
	ctx              context.Context
	cancel           context.CancelFunc
	cluster          *tests.TestCluster
	pdLeader         *tests.TestServer
	backendEndpoints string
	pdClient         pd.Client
}

func TestAPIServerForwardTestSuite(t *testing.T) {
	suite.Run(t, new(APIServerForwardTestSuite))
}

func (suite *APIServerForwardTestSuite) SetupTest() {
	var err error
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 3)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
	re.NoError(suite.pdLeader.BootstrapCluster())
	suite.addRegions()

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/usePDServiceMode", "return(true)"))
	suite.pdClient, err = pd.NewClientWithContext(context.Background(),
		[]string{suite.backendEndpoints}, pd.SecurityOption{}, pd.WithMaxErrorRetry(1))
	re.NoError(err)
}

func (suite *APIServerForwardTestSuite) TearDownTest() {
	re := suite.Require()
	suite.pdClient.Close()

	etcdClient := suite.pdLeader.GetEtcdClient()
	clusterID := strconv.FormatUint(suite.pdLeader.GetClusterID(), 10)
	endpoints, err := discovery.Discover(etcdClient, clusterID, utils.TSOServiceName)
	re.NoError(err)
	if len(endpoints) != 0 {
		endpoints, err = discovery.Discover(etcdClient, clusterID, utils.TSOServiceName)
		re.NoError(err)
		re.Empty(endpoints)
	}
	suite.cluster.Destroy()
	suite.cancel()
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/usePDServiceMode"))
}

func (suite *APIServerForwardTestSuite) TestForwardTSORelated() {
	re := suite.Require()
	// Unable to use the tso-related interface without tso server
	suite.checkUnavailableTSO(re)
	tc, err := tests.NewTestTSOCluster(suite.ctx, 1, suite.backendEndpoints)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForDefaultPrimaryServing(re)
	suite.checkAvailableTSO(re)
}

func (suite *APIServerForwardTestSuite) TestForwardTSOWhenPrimaryChanged() {
	re := suite.Require()

	tc, err := tests.NewTestTSOCluster(suite.ctx, 2, suite.backendEndpoints)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForDefaultPrimaryServing(re)

	// can use the tso-related interface with old primary
	oldPrimary, exist := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, utils.TSOServiceName)
	re.True(exist)
	suite.checkAvailableTSO(re)

	// can use the tso-related interface with new primary
	tc.DestroyServer(oldPrimary)
	time.Sleep(time.Duration(utils.DefaultLeaderLease) * time.Second) // wait for leader lease timeout
	tc.WaitForDefaultPrimaryServing(re)
	primary, exist := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, utils.TSOServiceName)
	re.True(exist)
	re.NotEqual(oldPrimary, primary)
	suite.checkAvailableTSO(re)

	// can use the tso-related interface with old primary again
	tc.AddServer(oldPrimary)
	suite.checkAvailableTSO(re)
	for addr := range tc.GetServers() {
		if addr != oldPrimary {
			tc.DestroyServer(addr)
		}
	}
	tc.WaitForDefaultPrimaryServing(re)
	time.Sleep(time.Duration(utils.DefaultLeaderLease) * time.Second) // wait for leader lease timeout
	primary, exist = suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, utils.TSOServiceName)
	re.True(exist)
	re.Equal(oldPrimary, primary)
	suite.checkAvailableTSO(re)
}

func (suite *APIServerForwardTestSuite) TestResignTSOPrimaryForward() {
	// TODO: test random kill primary with 3 nodes
	re := suite.Require()

	tc, err := tests.NewTestTSOCluster(suite.ctx, 2, suite.backendEndpoints)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForDefaultPrimaryServing(re)

	for j := 0; j < 10; j++ {
		tc.ResignPrimary(utils.DefaultKeyspaceID, utils.DefaultKeyspaceGroupID)
		tc.WaitForDefaultPrimaryServing(re)
		var err error
		for i := 0; i < 3; i++ { // try 3 times
			_, _, err = suite.pdClient.GetTS(suite.ctx)
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		re.NoError(err)
		suite.checkAvailableTSO(re)
	}
}

func (suite *APIServerForwardTestSuite) TestResignAPIPrimaryForward() {
	re := suite.Require()

	tc, err := tests.NewTestTSOCluster(suite.ctx, 2, suite.backendEndpoints)
	re.NoError(err)
	defer tc.Destroy()
	tc.WaitForDefaultPrimaryServing(re)

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/member/skipCampaignLeaderCheck"))
	}()

	for j := 0; j < 10; j++ {
		suite.pdLeader.ResignLeader()
		suite.pdLeader = suite.cluster.GetServer(suite.cluster.WaitLeader())
		suite.backendEndpoints = suite.pdLeader.GetAddr()
		_, _, err = suite.pdClient.GetTS(suite.ctx)
		re.NoError(err)
	}
}

func (suite *APIServerForwardTestSuite) TestForwardTSOUnexpectedToFollower1() {
	re := suite.Require()
	suite.checkForwardTSOUnexpectedToFollower(func() {
		// unary call will retry internally
		// try to update gc safe point
		min, err := suite.pdClient.UpdateServiceGCSafePoint(context.Background(), "a", 1000, 1)
		re.NoError(err)
		re.Equal(uint64(0), min)
	})
}

func (suite *APIServerForwardTestSuite) TestForwardTSOUnexpectedToFollower2() {
	re := suite.Require()
	suite.checkForwardTSOUnexpectedToFollower(func() {
		// unary call will retry internally
		// try to set external ts
		ts, err := suite.pdClient.GetExternalTimestamp(suite.ctx)
		re.NoError(err)
		err = suite.pdClient.SetExternalTimestamp(suite.ctx, ts+1)
		re.NoError(err)
	})
}

func (suite *APIServerForwardTestSuite) TestForwardTSOUnexpectedToFollower3() {
	re := suite.Require()
	suite.checkForwardTSOUnexpectedToFollower(func() {
		_, _, err := suite.pdClient.GetTS(suite.ctx)
		re.Error(err)
	})
}

func (suite *APIServerForwardTestSuite) checkForwardTSOUnexpectedToFollower(checkTSO func()) {
	re := suite.Require()
	tc, err := tests.NewTestTSOCluster(suite.ctx, 2, suite.backendEndpoints)
	re.NoError(err)
	tc.WaitForDefaultPrimaryServing(re)

	// get follower's address
	servers := tc.GetServers()
	oldPrimary := tc.GetPrimaryServer(utils.DefaultKeyspaceID, utils.DefaultKeyspaceGroupID).GetAddr()
	var follower string
	for addr := range servers {
		if addr != oldPrimary {
			follower = addr
			break
		}
	}
	re.NotEmpty(follower)

	// write follower's address to cache to simulate cache is not updated.
	suite.pdLeader.GetServer().SetServicePrimaryAddr(utils.TSOServiceName, follower)
	errorAddr, ok := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, utils.TSOServiceName)
	re.True(ok)
	re.Equal(follower, errorAddr)

	// test tso request
	checkTSO()

	// test tso request will success after cache is updated
	suite.checkAvailableTSO(re)
	newPrimary, exist2 := suite.pdLeader.GetServer().GetServicePrimaryAddr(suite.ctx, utils.TSOServiceName)
	re.True(exist2)
	re.NotEqual(errorAddr, newPrimary)
	re.Equal(oldPrimary, newPrimary)
	tc.Destroy()
}

func (suite *APIServerForwardTestSuite) addRegions() {
	leader := suite.cluster.GetServer(suite.cluster.WaitLeader())
	rc := leader.GetServer().GetRaftCluster()
	for i := 0; i < 3; i++ {
		region := &metapb.Region{
			Id:       uint64(i*4 + 1),
			Peers:    []*metapb.Peer{{Id: uint64(i*4 + 2), StoreId: uint64(i*4 + 3)}},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
		}
		rc.HandleRegionHeartbeat(core.NewRegionInfo(region, region.Peers[0]))
	}
}

func (suite *APIServerForwardTestSuite) checkUnavailableTSO(re *require.Assertions) {
	_, _, err := suite.pdClient.GetTS(suite.ctx)
	re.Error(err)
	// try to update gc safe point
	_, err = suite.pdClient.UpdateServiceGCSafePoint(suite.ctx, "a", 1000, 1)
	re.Error(err)
	// try to set external ts
	err = suite.pdClient.SetExternalTimestamp(suite.ctx, 1000)
	re.Error(err)
}

func (suite *APIServerForwardTestSuite) checkAvailableTSO(re *require.Assertions) {
	mcs.WaitForTSOServiceAvailable(suite.ctx, re, suite.pdClient)
	// try to get ts
	_, _, err := suite.pdClient.GetTS(suite.ctx)
	re.NoError(err)
	// try to update gc safe point
	min, err := suite.pdClient.UpdateServiceGCSafePoint(context.Background(), "a", 1000, 1)
	re.NoError(err)
	re.Equal(uint64(0), min)
	// try to set external ts
	ts, err := suite.pdClient.GetExternalTimestamp(suite.ctx)
	re.NoError(err)
	err = suite.pdClient.SetExternalTimestamp(suite.ctx, ts+1)
	re.NoError(err)
}

type CommonTestSuite struct {
	suite.Suite
	ctx        context.Context
	cancel     context.CancelFunc
	cluster    *tests.TestCluster
	tsoCluster *tests.TestTSOCluster
	pdLeader   *tests.TestServer
	// tsoDefaultPrimaryServer is the primary server of the default keyspace group
	tsoDefaultPrimaryServer *tso.Server
	backendEndpoints        string
}

func TestCommonTestSuite(t *testing.T) {
	suite.Run(t, new(CommonTestSuite))
}

func (suite *CommonTestSuite) SetupSuite() {
	var err error
	re := suite.Require()
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.cluster, err = tests.NewTestAPICluster(suite.ctx, 1)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	leaderName := suite.cluster.WaitLeader()
	suite.pdLeader = suite.cluster.GetServer(leaderName)
	suite.backendEndpoints = suite.pdLeader.GetAddr()
	re.NoError(suite.pdLeader.BootstrapCluster())

	suite.tsoCluster, err = tests.NewTestTSOCluster(suite.ctx, 1, suite.backendEndpoints)
	re.NoError(err)
	suite.tsoCluster.WaitForDefaultPrimaryServing(re)
	suite.tsoDefaultPrimaryServer = suite.tsoCluster.GetPrimaryServer(utils.DefaultKeyspaceID, utils.DefaultKeyspaceGroupID)
}

func (suite *CommonTestSuite) TearDownSuite() {
	re := suite.Require()
	suite.tsoCluster.Destroy()
	etcdClient := suite.pdLeader.GetEtcdClient()
	clusterID := strconv.FormatUint(suite.pdLeader.GetClusterID(), 10)
	endpoints, err := discovery.Discover(etcdClient, clusterID, utils.TSOServiceName)
	re.NoError(err)
	if len(endpoints) != 0 {
		endpoints, err = discovery.Discover(etcdClient, clusterID, utils.TSOServiceName)
		re.NoError(err)
		re.Empty(endpoints)
	}
	suite.cluster.Destroy()
	suite.cancel()
}

func (suite *CommonTestSuite) TestAdvertiseAddr() {
	re := suite.Require()

	conf := suite.tsoDefaultPrimaryServer.GetConfig()
	re.Equal(conf.GetListenAddr(), conf.GetAdvertiseListenAddr())
}

func (suite *CommonTestSuite) TestBootstrapDefaultKeyspaceGroup() {
	re := suite.Require()

	// check the default keyspace group
	check := func() {
		resp, err := http.Get(suite.pdLeader.GetServer().GetConfig().AdvertiseClientUrls + "/pd/api/v2/tso/keyspace-groups")
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		respString, err := io.ReadAll(resp.Body)
		re.NoError(err)
		var kgs []*endpoint.KeyspaceGroup
		re.NoError(json.Unmarshal(respString, &kgs))
		re.Len(kgs, 1)
		re.Equal(utils.DefaultKeyspaceGroupID, kgs[0].ID)
		re.Equal(endpoint.Basic.String(), kgs[0].UserKind)
		re.Empty(kgs[0].SplitState)
		re.Empty(kgs[0].Members)
		re.Empty(kgs[0].KeyspaceLookupTable)
	}
	check()

	s, err := suite.cluster.JoinAPIServer(suite.ctx)
	re.NoError(err)
	re.NoError(s.Run())

	// transfer leader to the new server
	suite.pdLeader.ResignLeader()
	suite.pdLeader = suite.cluster.GetServer(suite.cluster.WaitLeader())
	check()
	suite.pdLeader.ResignLeader()
	suite.pdLeader = suite.cluster.GetServer(suite.cluster.WaitLeader())
}
