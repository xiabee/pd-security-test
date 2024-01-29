// Copyright 2023 TiKV Project Authors.
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

package client_test

import (
	"path"
	"strconv"
	"testing"
	"time"

	pd "github.com/tikv/pd/client"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/utils/assertutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const globalConfigPath = "/global/config/"

type testReceiver struct {
	re *require.Assertions
	grpc.ServerStream
}

func (s testReceiver) Send(m *pdpb.WatchGlobalConfigResponse) error {
	log.Info("received", zap.Any("received", m.GetChanges()))
	for _, change := range m.GetChanges() {
		s.re.Contains(change.Name, globalConfigPath+string(change.Payload))
	}
	return nil
}

type globalConfigTestSuite struct {
	suite.Suite
	server  *server.GrpcServer
	client  pd.Client
	cleanup testutil.CleanupFunc
	mu      syncutil.Mutex
}

func TestGlobalConfigTestSuite(t *testing.T) {
	suite.Run(t, new(globalConfigTestSuite))
}

func (suite *globalConfigTestSuite) SetupSuite() {
	re := suite.Require()
	var (
		err error
		gsi *server.Server
	)
	checker := assertutil.NewChecker()
	checker.FailNow = func() {}
	gsi, suite.cleanup, err = server.NewTestServer(re, checker)
	suite.server = &server.GrpcServer{Server: gsi}
	re.NoError(err)
	addr := suite.server.GetAddr()
	suite.client, err = pd.NewClientWithContext(suite.server.Context(), []string{addr}, pd.SecurityOption{})
	re.NoError(err)
}

func (suite *globalConfigTestSuite) TearDownSuite() {
	suite.client.Close()
	suite.cleanup()
	suite.client.Close()
}

func (suite *globalConfigTestSuite) GetEtcdPath(configPath string) string {
	return globalConfigPath + configPath
}

func (suite *globalConfigTestSuite) TestLoadWithoutNames() {
	re := suite.Require()
	defer func() {
		// clean up
		_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath("test"))
		re.NoError(err)
	}()
	r, err := suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath("test"), "test")
	re.NoError(err)
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		ConfigPath: globalConfigPath,
	})
	re.NoError(err)
	re.Len(res.Items, 1)
	suite.LessOrEqual(r.Header.GetRevision(), res.Revision)
	re.Equal("test", string(res.Items[0].Payload))
}

func (suite *globalConfigTestSuite) TestLoadWithoutConfigPath() {
	re := suite.Require()
	defer func() {
		// clean up
		_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath("source_id"))
		re.NoError(err)
	}()
	_, err := suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath("source_id"), "1")
	re.NoError(err)
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		Names: []string{"source_id"},
	})
	re.NoError(err)
	re.Len(res.Items, 1)
	re.Equal([]byte("1"), res.Items[0].Payload)
}

func (suite *globalConfigTestSuite) TestLoadOtherConfigPath() {
	re := suite.Require()
	defer func() {
		for i := 0; i < 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
			re.NoError(err)
		}
	}()
	for i := 0; i < 3; i++ {
		_, err := suite.server.GetClient().Put(suite.server.Context(), path.Join("OtherConfigPath", strconv.Itoa(i)), strconv.Itoa(i))
		re.NoError(err)
	}
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		Names:      []string{"0", "1"},
		ConfigPath: "OtherConfigPath",
	})
	re.NoError(err)
	re.Len(res.Items, 2)
	for i, item := range res.Items {
		re.Equal(&pdpb.GlobalConfigItem{Kind: pdpb.EventType_PUT, Name: strconv.Itoa(i), Payload: []byte(strconv.Itoa(i))}, item)
	}
}

func (suite *globalConfigTestSuite) TestLoadAndStore() {
	re := suite.Require()
	defer func() {
		for i := 0; i < 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath("test"))
			re.NoError(err)
		}
	}()
	changes := []*pdpb.GlobalConfigItem{{Kind: pdpb.EventType_PUT, Name: "0", Payload: []byte("0")}, {Kind: pdpb.EventType_PUT, Name: "1", Payload: []byte("1")}, {Kind: pdpb.EventType_PUT, Name: "2", Payload: []byte("2")}}
	_, err := suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{
		ConfigPath: globalConfigPath,
		Changes:    changes,
	})
	re.NoError(err)
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		ConfigPath: globalConfigPath,
	})
	re.Len(res.Items, 3)
	re.NoError(err)
	for i, item := range res.Items {
		re.Equal(&pdpb.GlobalConfigItem{Kind: pdpb.EventType_PUT, Name: suite.GetEtcdPath(strconv.Itoa(i)), Payload: []byte(strconv.Itoa(i))}, item)
	}
}

func (suite *globalConfigTestSuite) TestStore() {
	re := suite.Require()
	defer func() {
		for i := 0; i < 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath("test"))
			re.NoError(err)
		}
	}()
	changes := []*pdpb.GlobalConfigItem{{Kind: pdpb.EventType_PUT, Name: "0", Payload: []byte("0")}, {Kind: pdpb.EventType_PUT, Name: "1", Payload: []byte("1")}, {Kind: pdpb.EventType_PUT, Name: "2", Payload: []byte("2")}}
	_, err := suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{
		ConfigPath: globalConfigPath,
		Changes:    changes,
	})
	re.NoError(err)
	for i := 0; i < 3; i++ {
		res, err := suite.server.GetClient().Get(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
		re.NoError(err)
		re.Equal(suite.GetEtcdPath(string(res.Kvs[0].Value)), string(res.Kvs[0].Key))
	}
}

func (suite *globalConfigTestSuite) TestWatch() {
	re := suite.Require()
	defer func() {
		for i := 0; i < 3; i++ {
			// clean up
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
			re.NoError(err)
		}
	}()
	server := testReceiver{re: suite.Require()}
	go suite.server.WatchGlobalConfig(&pdpb.WatchGlobalConfigRequest{
		ConfigPath: globalConfigPath,
		Revision:   0,
	}, server)
	for i := 0; i < 6; i++ {
		_, err := suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)), strconv.Itoa(i))
		re.NoError(err)
	}
	for i := 3; i < 6; i++ {
		_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
		re.NoError(err)
	}
	res, err := suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		ConfigPath: globalConfigPath,
	})
	re.Len(res.Items, 3)
	re.NoError(err)
}

func (suite *globalConfigTestSuite) TestClientLoadWithoutNames() {
	re := suite.Require()
	defer func() {
		for i := 0; i < 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
			re.NoError(err)
		}
	}()
	for i := 0; i < 3; i++ {
		_, err := suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)), strconv.Itoa(i))
		re.NoError(err)
	}
	res, _, err := suite.client.LoadGlobalConfig(suite.server.Context(), nil, globalConfigPath)
	re.NoError(err)
	re.Len(res, 3)
	for i, item := range res {
		re.Equal(pd.GlobalConfigItem{EventType: pdpb.EventType_PUT, Name: suite.GetEtcdPath(strconv.Itoa(i)), PayLoad: []byte(strconv.Itoa(i)), Value: strconv.Itoa(i)}, item)
	}
}

func (suite *globalConfigTestSuite) TestClientLoadWithoutConfigPath() {
	re := suite.Require()
	defer func() {
		_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath("source_id"))
		re.NoError(err)
	}()
	_, err := suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath("source_id"), "1")
	re.NoError(err)
	res, _, err := suite.client.LoadGlobalConfig(suite.server.Context(), []string{"source_id"}, "")
	re.NoError(err)
	re.Len(res, 1)
	re.Equal(pd.GlobalConfigItem{EventType: pdpb.EventType_PUT, Name: "source_id", PayLoad: []byte("1"), Value: "1"}, res[0])
}

func (suite *globalConfigTestSuite) TestClientLoadOtherConfigPath() {
	re := suite.Require()
	defer func() {
		for i := 0; i < 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
			re.NoError(err)
		}
	}()
	for i := 0; i < 3; i++ {
		_, err := suite.server.GetClient().Put(suite.server.Context(), path.Join("OtherConfigPath", strconv.Itoa(i)), strconv.Itoa(i))
		re.NoError(err)
	}
	res, _, err := suite.client.LoadGlobalConfig(suite.server.Context(), []string{"0", "1"}, "OtherConfigPath")
	re.NoError(err)
	re.Len(res, 2)
	for i, item := range res {
		re.Equal(pd.GlobalConfigItem{EventType: pdpb.EventType_PUT, Name: strconv.Itoa(i), PayLoad: []byte(strconv.Itoa(i)), Value: strconv.Itoa(i)}, item)
	}
}

func (suite *globalConfigTestSuite) TestClientStore() {
	re := suite.Require()
	defer func() {
		for i := 0; i < 3; i++ {
			_, err := suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
			re.NoError(err)
		}
	}()
	err := suite.client.StoreGlobalConfig(suite.server.Context(), globalConfigPath,
		[]pd.GlobalConfigItem{{Name: "0", Value: "0"}, {Name: "1", Value: "1"}, {Name: "2", Value: "2"}})
	re.NoError(err)
	for i := 0; i < 3; i++ {
		res, err := suite.server.GetClient().Get(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
		re.NoError(err)
		re.Equal(suite.GetEtcdPath(string(res.Kvs[0].Value)), string(res.Kvs[0].Key))
	}
}

func (suite *globalConfigTestSuite) TestClientWatchWithRevision() {
	re := suite.Require()
	ctx := suite.server.Context()
	defer func() {
		_, err := suite.server.GetClient().Delete(ctx, suite.GetEtcdPath("test"))
		re.NoError(err)

		for i := 3; i < 9; i++ {
			_, err := suite.server.GetClient().Delete(ctx, suite.GetEtcdPath(strconv.Itoa(i)))
			re.NoError(err)
		}
	}()
	// Mock get revision by loading
	r, err := suite.server.GetClient().Put(ctx, suite.GetEtcdPath("test"), "test")
	re.NoError(err)
	res, revision, err := suite.client.LoadGlobalConfig(ctx, nil, globalConfigPath)
	re.NoError(err)
	re.Len(res, 1)
	suite.LessOrEqual(r.Header.GetRevision(), revision)
	re.Equal(pd.GlobalConfigItem{EventType: pdpb.EventType_PUT, Name: suite.GetEtcdPath("test"), PayLoad: []byte("test"), Value: "test"}, res[0])
	// Mock when start watcher there are existed some keys, will load firstly
	for i := 0; i < 6; i++ {
		_, err = suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)), strconv.Itoa(i))
		re.NoError(err)
	}
	// Start watcher at next revision
	configChan, err := suite.client.WatchGlobalConfig(suite.server.Context(), globalConfigPath, revision)
	re.NoError(err)
	// Mock delete
	for i := 0; i < 3; i++ {
		_, err = suite.server.GetClient().Delete(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)))
		re.NoError(err)
	}
	// Mock put
	for i := 6; i < 9; i++ {
		_, err = suite.server.GetClient().Put(suite.server.Context(), suite.GetEtcdPath(strconv.Itoa(i)), strconv.Itoa(i))
		re.NoError(err)
	}
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	runTest := false
	for {
		select {
		case <-timer.C:
			re.True(runTest)
			return
		case res := <-configChan:
			for _, r := range res {
				re.Equal(suite.GetEtcdPath(r.Value), r.Name)
			}
			runTest = true
		}
	}
}

func (suite *globalConfigTestSuite) TestEtcdNotStart() {
	re := suite.Require()
	cli := suite.server.GetClient()
	defer func() {
		suite.mu.Lock()
		suite.server.SetClient(cli)
		suite.mu.Unlock()
	}()
	suite.mu.Lock()
	suite.server.SetClient(nil)
	suite.mu.Unlock()
	err := suite.server.WatchGlobalConfig(&pdpb.WatchGlobalConfigRequest{
		ConfigPath: globalConfigPath,
		Revision:   0,
	}, nil)
	re.Error(err)

	_, err = suite.server.StoreGlobalConfig(suite.server.Context(), &pdpb.StoreGlobalConfigRequest{
		ConfigPath: globalConfigPath,
		Changes:    []*pdpb.GlobalConfigItem{{Kind: pdpb.EventType_PUT, Name: "0", Payload: []byte("0")}},
	})
	re.Error(err)

	_, err = suite.server.LoadGlobalConfig(suite.server.Context(), &pdpb.LoadGlobalConfigRequest{
		Names: []string{"test_etcd"},
	})
	re.Error(err)
}
