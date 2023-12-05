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
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"context"
	"net/http"
	"sync"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
	"go.uber.org/goleak"
)

var (
	// testDialClient used to dial http request. only used for test.
	testDialClient = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	store = &metapb.Store{
		Id:      1,
		Address: "localhost",
	}
	peers = []*metapb.Peer{
		{
			Id:      2,
			StoreId: store.GetId(),
		},
	}
	region = &metapb.Region{
		Id: 8,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers,
	}
)

func TestAPIServer(t *testing.T) {
	server.EnableZap = true
	TestingT(t)
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type cleanUpFunc func()

func mustNewServer(c *C, opts ...func(cfg *config.Config)) (*server.Server, cleanUpFunc) {
	_, svrs, cleanup := mustNewCluster(c, 1, opts...)
	return svrs[0], cleanup
}

var zapLogOnce sync.Once

func mustNewCluster(c *C, num int, opts ...func(cfg *config.Config)) ([]*config.Config, []*server.Server, cleanUpFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	svrs := make([]*server.Server, 0, num)
	cfgs := server.NewTestMultiConfig(c, num)

	ch := make(chan *server.Server, num)
	for _, cfg := range cfgs {
		go func(cfg *config.Config) {
			err := cfg.SetupLogger()
			c.Assert(err, IsNil)
			zapLogOnce.Do(func() {
				log.ReplaceGlobals(cfg.GetZapLogger(), cfg.GetZapLogProperties())
			})
			for _, opt := range opts {
				opt(cfg)
			}
			s, err := server.CreateServer(ctx, cfg, NewHandler)
			c.Assert(err, IsNil)
			err = s.Run()
			c.Assert(err, IsNil)
			ch <- s
		}(cfg)
	}

	for i := 0; i < num; i++ {
		svr := <-ch
		svrs = append(svrs, svr)
	}
	close(ch)
	// wait etcd and http servers
	mustWaitLeader(c, svrs)

	// clean up
	clean := func() {
		cancel()
		for _, s := range svrs {
			s.Close()
		}
		for _, cfg := range cfgs {
			testutil.CleanServer(cfg.DataDir)
		}
	}

	return cfgs, svrs, clean
}

func mustWaitLeader(c *C, svrs []*server.Server) *server.Server {
	var leaderServer *server.Server
	testutil.WaitUntil(c, func(c *C) bool {
		var leader *pdpb.Member
		for _, svr := range svrs {
			l := svr.GetLeader()
			// All servers' GetLeader should return the same leader.
			if l == nil || (leader != nil && l.GetMemberId() != leader.GetMemberId()) {
				return false
			}
			if leader == nil {
				leader = l
			}
			if leader.GetMemberId() == svr.GetMember().ID() {
				leaderServer = svr
			}
		}
		return true
	})
	return leaderServer
}

func mustBootstrapCluster(c *C, s *server.Server) {
	grpcPDClient := testutil.MustNewGrpcClient(c, s.GetAddr())
	req := &pdpb.BootstrapRequest{
		Header: testutil.NewRequestHeader(s.ClusterID()),
		Store:  store,
		Region: region,
	}
	resp, err := grpcPDClient.Bootstrap(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_OK)
}
