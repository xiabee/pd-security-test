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

package mcs

import (
	"context"
	"time"

	"github.com/stretchr/testify/require"
	tso "github.com/tikv/pd/pkg/mcs/tso/server"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/testutil"
)

// TestCluster is a test cluster for TSO.
type TestCluster struct {
	ctx context.Context

	backendEndpoints string
	servers          map[string]*tso.Server
	cleanupFuncs     map[string]testutil.CleanupFunc
}

// NewTestTSOCluster creates a new TSO test cluster.
func NewTestTSOCluster(ctx context.Context, initialServerCount int, backendEndpoints string) (tc *TestCluster, err error) {
	tc = &TestCluster{
		ctx:              ctx,
		backendEndpoints: backendEndpoints,
		servers:          make(map[string]*tso.Server, initialServerCount),
		cleanupFuncs:     make(map[string]testutil.CleanupFunc, initialServerCount),
	}
	for i := 0; i < initialServerCount; i++ {
		err = tc.AddServer(tempurl.Alloc())
		if err != nil {
			return nil, err
		}
	}
	return tc, nil
}

// AddServer adds a new TSO server to the test cluster.
func (tc *TestCluster) AddServer(addr string) error {
	cfg := tso.NewConfig()
	cfg.BackendEndpoints = tc.backendEndpoints
	cfg.ListenAddr = addr
	cfg.Name = cfg.ListenAddr
	generatedCfg, err := tso.GenerateConfig(cfg)
	if err != nil {
		return err
	}
	err = initLogger(generatedCfg)
	if err != nil {
		return err
	}
	server, cleanup, err := newTSOTestServer(tc.ctx, generatedCfg)
	if err != nil {
		return err
	}
	tc.servers[generatedCfg.GetListenAddr()] = server
	tc.cleanupFuncs[generatedCfg.GetListenAddr()] = cleanup
	return nil
}

// Destroy stops and destroy the test cluster.
func (tc *TestCluster) Destroy() {
	for _, cleanup := range tc.cleanupFuncs {
		cleanup()
	}
	tc.cleanupFuncs = nil
	tc.servers = nil
}

// DestroyServer stops and destroy the test server by the given address.
func (tc *TestCluster) DestroyServer(addr string) {
	tc.cleanupFuncs[addr]()
	delete(tc.cleanupFuncs, addr)
	delete(tc.servers, addr)
}

// GetPrimary returns the primary TSO server.
func (tc *TestCluster) GetPrimary(keyspaceID, keyspaceGroupID uint32) *tso.Server {
	for _, server := range tc.servers {
		if server.IsKeyspaceServing(keyspaceID, keyspaceGroupID) {
			return server
		}
	}
	return nil
}

// WaitForPrimaryServing waits for one of servers being elected to be the primary/leader of the given keyspace.
func (tc *TestCluster) WaitForPrimaryServing(re *require.Assertions, keyspaceID, keyspaceGroupID uint32) string {
	var primary string
	testutil.Eventually(re, func() bool {
		for name, s := range tc.servers {
			if s.IsKeyspaceServing(keyspaceID, keyspaceGroupID) {
				primary = name
				return true
			}
		}
		return false
	}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

	return primary
}

// WaitForDefaultPrimaryServing waits for one of servers being elected to be the primary/leader of the default keyspace.
func (tc *TestCluster) WaitForDefaultPrimaryServing(re *require.Assertions) string {
	return tc.WaitForPrimaryServing(re, mcsutils.DefaultKeyspaceID, mcsutils.DefaultKeyspaceGroupID)
}

// GetServer returns the TSO server by the given address.
func (tc *TestCluster) GetServer(addr string) *tso.Server {
	for srvAddr, server := range tc.servers {
		if srvAddr == addr {
			return server
		}
	}
	return nil
}

// GetServers returns all TSO servers.
func (tc *TestCluster) GetServers() map[string]*tso.Server {
	return tc.servers
}
