// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tso_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/pkg/grpcutil"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/pkg/tsoutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"github.com/tikv/pd/server/tso"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"
)

func Test(t *testing.T) {
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

// There are three kinds of ways to generate a TSO:
// 1. Normal Global TSO, the normal way to get a global TSO from the PD leader,
//    a.k.a the single Global TSO Allocator.
// 2. Normal Local TSO, the new way to get a local TSO may from any of PD servers,
//    a.k.a the Local TSO Allocator leader.
// 3. Synchronized global TSO, the new way to get a global TSO from the PD leader,
//    which will coordinate and synchronize a TSO with other Local TSO Allocator
//    leaders.

const (
	tsoRequestConcurrencyNumber = 5
	tsoRequestRound             = 30
	tsoCount                    = 10
)

var _ = Suite(&testNormalGlobalTSOSuite{})

type testNormalGlobalTSOSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testNormalGlobalTSOSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *testNormalGlobalTSOSuite) TearDownSuite(c *C) {
	s.cancel()
}

// TestNormalGlobalTSO is used to test the normal way of global TSO generation.
func (s *testNormalGlobalTSOSuite) TestNormalGlobalTSO(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	leaderServer := cluster.GetServer(cluster.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(clusterID),
		Count:      uint32(tsoCount),
		DcLocation: tso.GlobalDCLocation,
	}
	s.requestGlobalTSOConcurrently(c, grpcPDClient, req)
	// Test Global TSO after the leader change
	leaderServer.GetServer().GetMember().ResetLeader()
	cluster.WaitLeader()
	s.requestGlobalTSOConcurrently(c, grpcPDClient, req)
}

func (s *testNormalGlobalTSOSuite) requestGlobalTSOConcurrently(c *C, grpcPDClient pdpb.PDClient, req *pdpb.TsoRequest) {
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			last := &pdpb.Timestamp{
				Physical: 0,
				Logical:  0,
			}
			for j := 0; j < tsoRequestRound; j++ {
				ts := s.testGetNormalGlobalTimestamp(c, grpcPDClient, req)
				// Check whether the TSO fallbacks
				c.Assert(tsoutil.CompareTimestamp(ts, last), Equals, 1)
				last = ts
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
}

func (s *testNormalGlobalTSOSuite) testGetNormalGlobalTimestamp(c *C, pdCli pdpb.PDClient, req *pdpb.TsoRequest) *pdpb.Timestamp {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tsoClient, err := pdCli.Tso(ctx)
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	resp, err := tsoClient.Recv()
	c.Assert(err, IsNil)
	c.Assert(resp.GetCount(), Equals, req.GetCount())
	res := resp.GetTimestamp()
	c.Assert(res.GetPhysical(), Greater, int64(0))
	c.Assert(uint32(res.GetLogical())>>res.GetSuffixBits(), GreaterEqual, req.GetCount())
	return res
}

func (s *testNormalGlobalTSOSuite) TestConcurrentlyReset(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitLeader()
	leader := cluster.GetServer(cluster.GetLeader())
	c.Assert(leader, NotNil)

	var wg sync.WaitGroup
	wg.Add(2)
	now := time.Now()
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i <= 100; i++ {
				physical := now.Add(time.Duration(2*i)*time.Minute).UnixNano() / int64(time.Millisecond)
				ts := uint64(physical << 18)
				leader.GetServer().GetHandler().ResetTS(ts)
			}
		}()
	}
	wg.Wait()
}

func (s *testNormalGlobalTSOSuite) TestZeroTSOCount(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 1)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	leaderServer := cluster.GetServer(cluster.GetLeader())
	grpcPDClient := testutil.MustNewGrpcClient(c, leaderServer.GetAddr())
	clusterID := leaderServer.GetClusterID()

	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(clusterID),
		DcLocation: tso.GlobalDCLocation,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tsoClient, err := grpcPDClient.Tso(ctx)
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	_, err = tsoClient.Recv()
	c.Assert(err, NotNil)
}

func (s *testNormalGlobalTSOSuite) TestRequestFollower(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 2)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	var followerServer *tests.TestServer
	for _, s := range cluster.GetServers() {
		if s.GetConfig().Name != cluster.GetLeader() {
			followerServer = s
		}
	}
	c.Assert(followerServer, NotNil)

	grpcPDClient := testutil.MustNewGrpcClient(c, followerServer.GetAddr())
	clusterID := followerServer.GetClusterID()
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(clusterID),
		Count:      1,
		DcLocation: tso.GlobalDCLocation,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = grpcutil.BuildForwardContext(ctx, followerServer.GetAddr())
	tsoClient, err := grpcPDClient.Tso(ctx)
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()

	start := time.Now()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	_, err = tsoClient.Recv()
	c.Assert(err, NotNil)

	// Requesting follower should fail fast, or the unavailable time will be
	// too long.
	c.Assert(time.Since(start), Less, time.Second)
}

// In some cases, when a TSO request arrives, the SyncTimestamp may not finish yet.
// This test is used to simulate this situation and verify that the retry mechanism.
func (s *testNormalGlobalTSOSuite) TestDelaySyncTimestamp(c *C) {
	cluster, err := tests.NewTestCluster(s.ctx, 2)
	c.Assert(err, IsNil)
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	var leaderServer, nextLeaderServer *tests.TestServer
	leaderServer = cluster.GetServer(cluster.GetLeader())
	c.Assert(leaderServer, NotNil)
	for _, s := range cluster.GetServers() {
		if s.GetConfig().Name != cluster.GetLeader() {
			nextLeaderServer = s
		}
	}
	c.Assert(nextLeaderServer, NotNil)

	grpcPDClient := testutil.MustNewGrpcClient(c, nextLeaderServer.GetAddr())
	clusterID := nextLeaderServer.GetClusterID()
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(clusterID),
		Count:      1,
		DcLocation: tso.GlobalDCLocation,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.Assert(failpoint.Enable("github.com/tikv/pd/server/tso/delaySyncTimestamp", `return(true)`), IsNil)

	// Make the old leader resign and wait for the new leader to get a lease
	leaderServer.ResignLeader()
	c.Assert(nextLeaderServer.WaitLeader(), IsTrue)

	ctx = grpcutil.BuildForwardContext(ctx, nextLeaderServer.GetAddr())
	tsoClient, err := grpcPDClient.Tso(ctx)
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	resp, err := tsoClient.Recv()
	c.Assert(err, IsNil)
	c.Assert(resp.GetCount(), Equals, uint32(1))
	res := resp.GetTimestamp()
	c.Assert(res.GetPhysical(), Greater, int64(0))
	c.Assert(uint32(res.GetLogical())>>res.GetSuffixBits(), GreaterEqual, req.GetCount())
	failpoint.Disable("github.com/tikv/pd/server/tso/delaySyncTimestamp")
}

var _ = Suite(&testTimeFallBackSuite{})

type testTimeFallBackSuite struct {
	ctx          context.Context
	cancel       context.CancelFunc
	cluster      *tests.TestCluster
	grpcPDClient pdpb.PDClient
	server       *tests.TestServer
}

func (s *testTimeFallBackSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/tso/fallBackSync", `return(true)`), IsNil)
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/tso/fallBackUpdate", `return(true)`), IsNil)
	var err error
	s.cluster, err = tests.NewTestCluster(s.ctx, 1)
	c.Assert(err, IsNil)

	err = s.cluster.RunInitialServers()
	c.Assert(err, IsNil)
	s.cluster.WaitLeader()

	s.server = s.cluster.GetServer(s.cluster.GetLeader())
	s.grpcPDClient = testutil.MustNewGrpcClient(c, s.server.GetAddr())
	svr := s.server.GetServer()
	svr.Close()
	failpoint.Disable("github.com/tikv/pd/server/tso/fallBackSync")
	failpoint.Disable("github.com/tikv/pd/server/tso/fallBackUpdate")
	err = svr.Run()
	c.Assert(err, IsNil)
	s.cluster.WaitLeader()
}

func (s *testTimeFallBackSuite) TearDownSuite(c *C) {
	s.cancel()
	s.cluster.Destroy()
}

func (s *testTimeFallBackSuite) testGetTimestamp(c *C, n uint32) *pdpb.Timestamp {
	clusterID := s.server.GetClusterID()
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(clusterID),
		Count:      n,
		DcLocation: tso.GlobalDCLocation,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tsoClient, err := s.grpcPDClient.Tso(ctx)
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	resp, err := tsoClient.Recv()
	c.Assert(err, IsNil)
	c.Assert(resp.GetCount(), Equals, uint32(n))
	res := resp.GetTimestamp()
	c.Assert(tsoutil.CompareTimestamp(res, tsoutil.GenerateTimestamp(time.Now(), 0)), Equals, 1)
	c.Assert(uint32(res.GetLogical())>>res.GetSuffixBits(), GreaterEqual, req.GetCount())
	return res
}

func (s *testTimeFallBackSuite) TestTimeFallBack(c *C) {
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			last := &pdpb.Timestamp{
				Physical: 0,
				Logical:  0,
			}
			for j := 0; j < tsoRequestRound; j++ {
				ts := s.testGetTimestamp(c, 10)
				c.Assert(tsoutil.CompareTimestamp(ts, last), Equals, 1)
				last = ts
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
}

var _ = Suite(&testFollowerTsoSuite{})

type testFollowerTsoSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testFollowerTsoSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	server.EnableZap = true
}

func (s *testFollowerTsoSuite) TearDownSuite(c *C) {
	s.cancel()
}

func (s *testFollowerTsoSuite) TestRequest(c *C) {
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/tso/skipRetryGetTS", `return(true)`), IsNil)
	cluster, err := tests.NewTestCluster(s.ctx, 2)
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)
	cluster.WaitLeader()

	servers := cluster.GetServers()
	var followerServer *tests.TestServer
	for _, s := range servers {
		if !s.IsLeader() {
			followerServer = s
		}
	}
	c.Assert(followerServer, NotNil)
	grpcPDClient := testutil.MustNewGrpcClient(c, followerServer.GetAddr())
	clusterID := followerServer.GetClusterID()

	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(clusterID),
		Count:      1,
		DcLocation: tso.GlobalDCLocation,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = grpcutil.BuildForwardContext(ctx, followerServer.GetAddr())
	tsoClient, err := grpcPDClient.Tso(ctx)
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	_, err = tsoClient.Recv()
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "generate timestamp failed"), IsTrue)
	failpoint.Disable("github.com/tikv/pd/server/tso/skipRetryGetTS")
}

var _ = Suite(&testSynchronizedGlobalTSO{})

type testSynchronizedGlobalTSO struct {
	ctx          context.Context
	cancel       context.CancelFunc
	leaderServer *tests.TestServer
	dcClientMap  map[string]pdpb.PDClient
}

func (s *testSynchronizedGlobalTSO) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.dcClientMap = make(map[string]pdpb.PDClient)
	server.EnableZap = true
}

func (s *testSynchronizedGlobalTSO) TearDownSuite(c *C) {
	s.cancel()
}

// TestSynchronizedGlobalTSO is used to test the synchronized way of global TSO generation.
func (s *testSynchronizedGlobalTSO) TestSynchronizedGlobalTSO(c *C) {
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitAllLeaders(c, dcLocationConfig)

	s.leaderServer = cluster.GetServer(cluster.GetLeader())
	c.Assert(s.leaderServer, NotNil)
	s.dcClientMap[tso.GlobalDCLocation] = testutil.MustNewGrpcClient(c, s.leaderServer.GetAddr())
	for _, dcLocation := range dcLocationConfig {
		pdName := s.leaderServer.GetAllocatorLeader(dcLocation).GetName()
		s.dcClientMap[dcLocation] = testutil.MustNewGrpcClient(c, cluster.GetServer(pdName).GetAddr())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	maxGlobalTSO := &pdpb.Timestamp{}
	for i := 0; i < tsoRequestRound; i++ {
		// Get some local TSOs first
		oldLocalTSOs := make([]*pdpb.Timestamp, 0, dcLocationNum)
		for _, dcLocation := range dcLocationConfig {
			localTSO := s.testGetTimestamp(ctx, c, cluster, tsoCount, dcLocation)
			oldLocalTSOs = append(oldLocalTSOs, localTSO)
			c.Assert(tsoutil.CompareTimestamp(maxGlobalTSO, localTSO), Equals, -1)
		}
		// Get a global TSO then
		globalTSO := s.testGetTimestamp(ctx, c, cluster, tsoCount, tso.GlobalDCLocation)
		for _, oldLocalTSO := range oldLocalTSOs {
			c.Assert(tsoutil.CompareTimestamp(globalTSO, oldLocalTSO), Equals, 1)
		}
		if tsoutil.CompareTimestamp(maxGlobalTSO, globalTSO) < 0 {
			maxGlobalTSO = globalTSO
		}
		// Get some local TSOs again
		newLocalTSOs := make([]*pdpb.Timestamp, 0, dcLocationNum)
		for _, dcLocation := range dcLocationConfig {
			newLocalTSOs = append(newLocalTSOs, s.testGetTimestamp(ctx, c, cluster, tsoCount, dcLocation))
		}
		for _, newLocalTSO := range newLocalTSOs {
			c.Assert(tsoutil.CompareTimestamp(maxGlobalTSO, newLocalTSO), Equals, -1)
		}
	}
}

func (s *testSynchronizedGlobalTSO) testGetTimestamp(ctx context.Context, c *C, cluster *tests.TestCluster, n uint32, dcLocation string) *pdpb.Timestamp {
	req := &pdpb.TsoRequest{
		Header:     testutil.NewRequestHeader(s.leaderServer.GetClusterID()),
		Count:      n,
		DcLocation: dcLocation,
	}
	pdClient, ok := s.dcClientMap[dcLocation]
	c.Assert(ok, IsTrue)
	forwardedHost := cluster.GetServer(s.leaderServer.GetAllocatorLeader(dcLocation).GetName()).GetAddr()
	ctx = grpcutil.BuildForwardContext(ctx, forwardedHost)
	tsoClient, err := pdClient.Tso(ctx)
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	resp, err := tsoClient.Recv()
	c.Assert(err, IsNil)
	c.Assert(resp.GetCount(), Equals, uint32(n))
	res := resp.GetTimestamp()
	c.Assert(res.GetPhysical(), Greater, int64(0))
	c.Assert(uint32(res.GetLogical())>>res.GetSuffixBits(), GreaterEqual, req.GetCount())
	return res
}

func (s *testSynchronizedGlobalTSO) TestSynchronizedGlobalTSOOverflow(c *C) {
	dcLocationConfig := map[string]string{
		"pd1": "dc-1",
		"pd2": "dc-2",
		"pd3": "dc-3",
	}
	dcLocationNum := len(dcLocationConfig)
	cluster, err := tests.NewTestCluster(s.ctx, dcLocationNum, func(conf *config.Config, serverName string) {
		conf.EnableLocalTSO = true
		conf.Labels[config.ZoneLabel] = dcLocationConfig[serverName]
	})
	defer cluster.Destroy()
	c.Assert(err, IsNil)

	err = cluster.RunInitialServers()
	c.Assert(err, IsNil)

	cluster.WaitAllLeaders(c, dcLocationConfig)

	s.leaderServer = cluster.GetServer(cluster.GetLeader())
	c.Assert(s.leaderServer, NotNil)
	s.dcClientMap[tso.GlobalDCLocation] = testutil.MustNewGrpcClient(c, s.leaderServer.GetAddr())
	for _, dcLocation := range dcLocationConfig {
		pdName := s.leaderServer.GetAllocatorLeader(dcLocation).GetName()
		s.dcClientMap[dcLocation] = testutil.MustNewGrpcClient(c, cluster.GetServer(pdName).GetAddr())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Assert(failpoint.Enable("github.com/tikv/pd/server/tso/globalTSOOverflow", `return(true)`), IsNil)
	s.testGetTimestamp(ctx, c, cluster, tsoCount, tso.GlobalDCLocation)
	failpoint.Disable("github.com/tikv/pd/server/tso/globalTSOOverflow")
}
