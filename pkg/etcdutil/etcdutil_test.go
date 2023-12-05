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

package etcdutil

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/types"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testEtcdutilSuite{})

type testEtcdutilSuite struct{}

func (s *testEtcdutilSuite) TestMemberHelpers(c *C) {
	cfg1 := NewTestSingleConfig()
	etcd1, err := embed.StartEtcd(cfg1)
	defer func() {
		etcd1.Close()
		CleanConfig(cfg1)
	}()
	c.Assert(err, IsNil)

	ep1 := cfg1.LCUrls[0].String()
	client1, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep1},
	})
	c.Assert(err, IsNil)

	<-etcd1.Server.ReadyNotify()

	// Test ListEtcdMembers
	listResp1, err := ListEtcdMembers(client1)
	c.Assert(err, IsNil)
	c.Assert(len(listResp1.Members), Equals, 1)
	// types.ID is an alias of uint64.
	c.Assert(listResp1.Members[0].ID, Equals, uint64(etcd1.Server.ID()))

	// Test AddEtcdMember
	// Make a new etcd config.
	cfg2 := NewTestSingleConfig()
	cfg2.Name = "etcd2"
	cfg2.InitialCluster = cfg1.InitialCluster + fmt.Sprintf(",%s=%s", cfg2.Name, &cfg2.LPUrls[0])
	cfg2.ClusterState = embed.ClusterStateFlagExisting

	// Add it to the cluster above.
	peerURL := cfg2.LPUrls[0].String()
	addResp, err := AddEtcdMember(client1, []string{peerURL})
	c.Assert(err, IsNil)

	etcd2, err := embed.StartEtcd(cfg2)
	defer func() {
		etcd2.Close()
		CleanConfig(cfg2)
	}()
	c.Assert(err, IsNil)
	c.Assert(addResp.Member.ID, Equals, uint64(etcd2.Server.ID()))

	ep2 := cfg2.LCUrls[0].String()
	client2, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep2},
	})
	c.Assert(err, IsNil)

	<-etcd2.Server.ReadyNotify()
	c.Assert(err, IsNil)

	listResp2, err := ListEtcdMembers(client2)
	c.Assert(err, IsNil)
	c.Assert(len(listResp2.Members), Equals, 2)
	for _, m := range listResp2.Members {
		switch m.ID {
		case uint64(etcd1.Server.ID()):
		case uint64(etcd2.Server.ID()):
		default:
			c.Fatalf("unknown member: %v", m)
		}
	}

	// Test CheckClusterID
	urlsMap, err := types.NewURLsMap(cfg2.InitialCluster)
	c.Assert(err, IsNil)
	err = CheckClusterID(etcd1.Server.Cluster().ID(), urlsMap, &tls.Config{})
	c.Assert(err, IsNil)

	// Test RemoveEtcdMember
	_, err = RemoveEtcdMember(client1, uint64(etcd2.Server.ID()))
	c.Assert(err, IsNil)

	listResp3, err := ListEtcdMembers(client1)
	c.Assert(err, IsNil)
	c.Assert(len(listResp3.Members), Equals, 1)
	c.Assert(listResp3.Members[0].ID, Equals, uint64(etcd1.Server.ID()))
}

func (s *testEtcdutilSuite) TestEtcdKVGet(c *C) {
	cfg := NewTestSingleConfig()
	etcd, err := embed.StartEtcd(cfg)
	defer func() {
		etcd.Close()
		CleanConfig(cfg)
	}()
	c.Assert(err, IsNil)

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	c.Assert(err, IsNil)

	<-etcd.Server.ReadyNotify()

	keys := []string{"test/key1", "test/key2", "test/key3", "test/key4", "test/key5"}
	vals := []string{"val1", "val2", "val3", "val4", "val5"}

	kv := clientv3.NewKV(client)
	for i := range keys {
		_, err = kv.Put(context.TODO(), keys[i], vals[i])
		c.Assert(err, IsNil)
	}

	// Test simple point get
	resp, err := EtcdKVGet(client, "test/key1")
	c.Assert(err, IsNil)
	c.Assert(string(resp.Kvs[0].Value), Equals, "val1")

	// Test range get
	withRange := clientv3.WithRange("test/zzzz")
	withLimit := clientv3.WithLimit(3)
	resp, err = EtcdKVGet(client, "test/", withRange, withLimit, clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	c.Assert(err, IsNil)
	c.Assert(len(resp.Kvs), Equals, 3)

	for i := range resp.Kvs {
		c.Assert(string(resp.Kvs[i].Key), Equals, keys[i])
		c.Assert(string(resp.Kvs[i].Value), Equals, vals[i])
	}

	lastKey := string(resp.Kvs[len(resp.Kvs)-1].Key)
	next := clientv3.GetPrefixRangeEnd(lastKey)
	resp, err = EtcdKVGet(client, next, withRange, withLimit, clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	c.Assert(err, IsNil)
	c.Assert(len(resp.Kvs), Equals, 2)
}

func (s *testEtcdutilSuite) TestEtcdKVPutWithTTL(c *C) {
	cfg := NewTestSingleConfig()
	etcd, err := embed.StartEtcd(cfg)
	defer func() {
		etcd.Close()
		CleanConfig(cfg)
	}()
	c.Assert(err, IsNil)

	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	c.Assert(err, IsNil)

	<-etcd.Server.ReadyNotify()

	_, err = EtcdKVPutWithTTL(context.TODO(), client, "test/ttl1", "val1", 2)
	c.Assert(err, IsNil)
	_, err = EtcdKVPutWithTTL(context.TODO(), client, "test/ttl2", "val2", 4)
	c.Assert(err, IsNil)

	time.Sleep(3 * time.Second)
	// test/ttl1 is outdated
	resp, err := EtcdKVGet(client, "test/ttl1")
	c.Assert(err, IsNil)
	c.Assert(resp.Count, Equals, int64(0))
	// but test/ttl2 is not
	resp, err = EtcdKVGet(client, "test/ttl2")
	c.Assert(err, IsNil)
	c.Assert(string(resp.Kvs[0].Value), Equals, "val2")

	time.Sleep(2 * time.Second)

	// test/ttl2 is also outdated
	resp, err = EtcdKVGet(client, "test/ttl2")
	c.Assert(err, IsNil)
	c.Assert(resp.Count, Equals, int64(0))
}
