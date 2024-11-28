// Copyright 2024 TiKV Project Authors.
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

package utils

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/discovery"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/keypath"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// expectedPrimaryFlag is the flag to indicate the expected primary.
// 1. When the primary was campaigned successfully, it will set the `expected_primary` flag.
// 2. Using `{service}/primary/transfer` API will revoke the previous lease and set a new `expected_primary` flag.
// This flag used to help new primary to campaign successfully while other secondaries can skip the campaign.
const expectedPrimaryFlag = "expected_primary"

// expectedPrimaryPath formats the primary path with the expected primary flag.
func expectedPrimaryPath(primaryPath string) string {
	return fmt.Sprintf("%s/%s", primaryPath, expectedPrimaryFlag)
}

// GetExpectedPrimaryFlag gets the expected primary flag.
func GetExpectedPrimaryFlag(client *clientv3.Client, primaryPath string) string {
	path := expectedPrimaryPath(primaryPath)
	primary, err := etcdutil.GetValue(client, path)
	if err != nil {
		log.Error("get expected primary flag error", errs.ZapError(err), zap.String("primary-path", path))
		return ""
	}

	return string(primary)
}

// markExpectedPrimaryFlag marks the expected primary flag when the primary is specified.
func markExpectedPrimaryFlag(client *clientv3.Client, primaryPath string, leaderRaw string, leaseID clientv3.LeaseID) (int64, error) {
	path := expectedPrimaryPath(primaryPath)
	log.Info("set expected primary flag", zap.String("primary-path", path), zap.String("leader-raw", leaderRaw))
	// write a flag to indicate the expected primary.
	resp, err := kv.NewSlowLogTxn(client).
		Then(clientv3.OpPut(expectedPrimaryPath(primaryPath), leaderRaw, clientv3.WithLease(leaseID))).
		Commit()
	if err != nil || !resp.Succeeded {
		log.Error("mark expected primary error", errs.ZapError(err), zap.String("primary-path", path))
		return 0, err
	}
	return resp.Header.Revision, nil
}

// KeepExpectedPrimaryAlive keeps the expected primary alive.
// We use lease to keep `expected primary` healthy.
// ONLY reset by the following conditions:
// - changed by `{service}/primary/transfer` API.
// - leader lease expired.
// ONLY primary called this function.
func KeepExpectedPrimaryAlive(ctx context.Context, cli *clientv3.Client, exitPrimary chan<- struct{},
	leaseTimeout int64, leaderPath, memberValue, service string) (*election.Lease, error) {
	log.Info("primary start to watch the expected primary", zap.String("service", service), zap.String("primary-value", memberValue))
	service = fmt.Sprintf("%s expected primary", service)
	lease := election.NewLease(cli, service)
	if err := lease.Grant(leaseTimeout); err != nil {
		return nil, err
	}

	revision, err := markExpectedPrimaryFlag(cli, leaderPath, memberValue, lease.ID.Load().(clientv3.LeaseID))
	if err != nil {
		log.Error("mark expected primary error", errs.ZapError(err))
		return nil, err
	}
	// Keep alive the current expected primary leadership to indicate that the server is still alive.
	// Watch the expected primary path to check whether the expected primary has changed by `{service}/primary/transfer` API.
	expectedPrimary := election.NewLeadership(cli, expectedPrimaryPath(leaderPath), service)
	expectedPrimary.SetLease(lease)
	expectedPrimary.Keep(ctx)

	go watchExpectedPrimary(ctx, expectedPrimary, revision+1, exitPrimary)
	return lease, nil
}

// watchExpectedPrimary watches `{service}/primary/transfer` API whether changed the expected primary.
func watchExpectedPrimary(ctx context.Context,
	expectedPrimary *election.Leadership, revision int64, exitPrimary chan<- struct{}) {
	expectedPrimary.SetPrimaryWatch(true)
	// ONLY exited watch by the following conditions:
	// - changed by `{service}/primary/transfer` API.
	// - leader lease expired.
	expectedPrimary.Watch(ctx, revision)
	expectedPrimary.Reset()
	defer log.Info("primary exit the primary watch loop")
	select {
	case <-ctx.Done():
		return
	case exitPrimary <- struct{}{}:
		return
	}
}

// TransferPrimary transfers the primary of the specified service.
// keyspaceGroupID is optional, only used for TSO service.
func TransferPrimary(client *clientv3.Client, lease *election.Lease, serviceName,
	oldPrimary, newPrimary string, keyspaceGroupID uint32, tsoMembersMap map[string]bool) error {
	if lease == nil {
		return errors.New("current lease is nil, please check leadership")
	}
	log.Info("try to transfer primary", zap.String("service", serviceName), zap.String("from", oldPrimary), zap.String("to", newPrimary))
	entries, err := discovery.GetMSMembers(serviceName, client)
	if err != nil {
		return err
	}

	// Do nothing when I am the only member of cluster.
	if len(entries) == 1 {
		return errors.Errorf("no valid secondary to transfer primary, the only member is %s", entries[0].Name)
	}

	var primaryIDs []string
	for _, member := range entries {
		// only members of specific group are valid primary candidates for TSO service.
		if tsoMembersMap != nil && !tsoMembersMap[member.ServiceAddr] {
			continue
		}
		if (newPrimary == "" && member.Name != oldPrimary) || (newPrimary != "" && member.Name == newPrimary) {
			primaryIDs = append(primaryIDs, member.ServiceAddr)
		}
	}
	if len(primaryIDs) == 0 {
		return errors.Errorf("no valid secondary to transfer primary, from %s to %s", oldPrimary, newPrimary)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	nextPrimaryID := r.Intn(len(primaryIDs))

	// update expected primary flag
	grantResp, err := client.Grant(client.Ctx(), constant.DefaultLeaderLease)
	if err != nil {
		return errors.Errorf("failed to grant lease for expected primary, err: %v", err)
	}

	// revoke current primary's lease to ensure keepalive goroutine of primary exits.
	if err := lease.Close(); err != nil {
		return errors.Errorf("failed to revoke current primary's lease: %v", err)
	}

	var primaryPath string
	switch serviceName {
	case constant.SchedulingServiceName:
		primaryPath = keypath.SchedulingPrimaryPath()
	case constant.TSOServiceName:
		tsoRootPath := keypath.TSOSvcRootPath()
		primaryPath = keypath.KeyspaceGroupPrimaryPath(tsoRootPath, keyspaceGroupID)
	}
	_, err = markExpectedPrimaryFlag(client, primaryPath, primaryIDs[nextPrimaryID], grantResp.ID)
	if err != nil {
		return errors.Errorf("failed to mark expected primary flag for %s, err: %v", serviceName, err)
	}
	return nil
}
