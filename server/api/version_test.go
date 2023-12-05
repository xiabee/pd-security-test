// Copyright 2019 TiKV Project Authors.
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
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/config"
)

var _ = Suite(&testVersionSuite{})

type testVersionSuite struct{}

func (s *testVersionSuite) TestGetVersion(c *C) {
	// TODO: enable it.
	c.Skip("Temporary disable. See issue: https://github.com/tikv/pd/issues/1893")

	fname := filepath.Join(os.TempDir(), "stdout")
	old := os.Stdout
	temp, _ := os.Create(fname)
	os.Stdout = temp

	cfg := server.NewTestSingleConfig(c)
	reqCh := make(chan struct{})
	go func() {
		<-reqCh
		time.Sleep(200 * time.Millisecond)
		addr := cfg.ClientUrls + apiPrefix + "/api/v1/version"
		resp, err := testDialClient.Get(addr)
		c.Assert(err, IsNil)
		defer resp.Body.Close()
		_, err = io.ReadAll(resp.Body)
		c.Assert(err, IsNil)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan *server.Server)
	go func(cfg *config.Config) {
		s, err := server.CreateServer(ctx, cfg, NewHandler)
		c.Assert(err, IsNil)
		c.Assert(failpoint.Enable("github.com/tikv/pd/server/memberNil", `return(true)`), IsNil)
		reqCh <- struct{}{}
		err = s.Run()
		c.Assert(err, IsNil)
		ch <- s
	}(cfg)

	svr := <-ch
	close(ch)
	out, _ := os.ReadFile(fname)
	c.Assert(strings.Contains(string(out), "PANIC"), IsFalse)

	// clean up
	func() {
		temp.Close()
		os.Stdout = old
		os.Remove(fname)
		svr.Close()
		cancel()
		testutil.CleanServer(cfg.DataDir)
	}()
	c.Assert(failpoint.Disable("github.com/tikv/pd/server/memberNil"), IsNil)
}
