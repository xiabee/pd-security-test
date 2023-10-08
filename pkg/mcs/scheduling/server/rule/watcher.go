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

package rule

import (
	"context"
	"strings"
	"sync"

	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

// ruleStorage is an in-memory storage for Placement Rules,
// which will implement the `endpoint.RuleStorage` interface.
type ruleStorage struct {
	// Rule key -> rule value.
	rules sync.Map
	// GroupID -> rule group value.
	groups sync.Map
	// Region rule key -> rule value.
	regionRules sync.Map
}

// LoadRules loads Placement Rules from storage.
func (rs *ruleStorage) LoadRules(f func(k, v string)) error {
	rs.rules.Range(func(k, v interface{}) bool {
		f(k.(string), v.(string))
		return true
	})
	return nil
}

// SaveRule stores a rule cfg to the rulesPathPrefix.
func (rs *ruleStorage) SaveRule(ruleKey string, rule interface{}) error {
	rs.rules.Store(ruleKey, rule)
	return nil
}

// DeleteRule removes a rule from storage.
func (rs *ruleStorage) DeleteRule(ruleKey string) error {
	rs.rules.Delete(ruleKey)
	return nil
}

// LoadRuleGroups loads all rule groups from storage.
func (rs *ruleStorage) LoadRuleGroups(f func(k, v string)) error {
	rs.groups.Range(func(k, v interface{}) bool {
		f(k.(string), v.(string))
		return true
	})
	return nil
}

// SaveRuleGroup stores a rule group config to storage.
func (rs *ruleStorage) SaveRuleGroup(groupID string, group interface{}) error {
	rs.groups.Store(groupID, group)
	return nil
}

// DeleteRuleGroup removes a rule group from storage.
func (rs *ruleStorage) DeleteRuleGroup(groupID string) error {
	rs.groups.Delete(groupID)
	return nil
}

// LoadRegionRules loads region rules from storage.
func (rs *ruleStorage) LoadRegionRules(f func(k, v string)) error {
	rs.regionRules.Range(func(k, v interface{}) bool {
		f(k.(string), v.(string))
		return true
	})
	return nil
}

// SaveRegionRule saves a region rule to the storage.
func (rs *ruleStorage) SaveRegionRule(ruleKey string, rule interface{}) error {
	rs.regionRules.Store(ruleKey, rule)
	return nil
}

// DeleteRegionRule removes a region rule from storage.
func (rs *ruleStorage) DeleteRegionRule(ruleKey string) error {
	rs.regionRules.Delete(ruleKey)
	return nil
}

// Watcher is used to watch the PD API server for any Placement Rule changes.
type Watcher struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// rulesPathPrefix:
	//   - Key: /pd/{cluster_id}/rules/{group_id}-{rule_id}
	//   - Value: placement.Rule
	rulesPathPrefix string
	// ruleGroupPathPrefix:
	//   - Key: /pd/{cluster_id}/rule_group/{group_id}
	//   - Value: placement.RuleGroup
	ruleGroupPathPrefix string
	// regionLabelPathPrefix:
	//   - Key: /pd/{cluster_id}/region_label/{rule_id}
	//  - Value: labeler.LabelRule
	regionLabelPathPrefix string

	etcdClient *clientv3.Client
	ruleStore  *ruleStorage

	ruleWatcher  *etcdutil.LoopWatcher
	groupWatcher *etcdutil.LoopWatcher
	labelWatcher *etcdutil.LoopWatcher
}

// NewWatcher creates a new watcher to watch the Placement Rule change from PD API server.
// Please use `GetRuleStorage` to get the underlying storage to access the Placement Rules.
func NewWatcher(
	ctx context.Context,
	etcdClient *clientv3.Client,
	clusterID uint64,
) (*Watcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	rw := &Watcher{
		ctx:                   ctx,
		cancel:                cancel,
		rulesPathPrefix:       endpoint.RulesPathPrefix(clusterID),
		ruleGroupPathPrefix:   endpoint.RuleGroupPathPrefix(clusterID),
		regionLabelPathPrefix: endpoint.RegionLabelPathPrefix(clusterID),
		etcdClient:            etcdClient,
		ruleStore:             &ruleStorage{},
	}
	err := rw.initializeRuleWatcher()
	if err != nil {
		return nil, err
	}
	err = rw.initializeGroupWatcher()
	if err != nil {
		return nil, err
	}
	err = rw.initializeRegionLabelWatcher()
	if err != nil {
		return nil, err
	}
	return rw, nil
}

func (rw *Watcher) initializeRuleWatcher() error {
	prefixToTrim := rw.rulesPathPrefix + "/"
	putFn := func(kv *mvccpb.KeyValue) error {
		// Since the PD API server will validate the rule before saving it to etcd,
		// so we could directly save the string rule in JSON to the storage here.
		return rw.ruleStore.SaveRule(
			strings.TrimPrefix(string(kv.Key), prefixToTrim),
			string(kv.Value),
		)
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		return rw.ruleStore.DeleteRule(strings.TrimPrefix(string(kv.Key), prefixToTrim))
	}
	postEventFn := func() error {
		return nil
	}
	rw.ruleWatcher = etcdutil.NewLoopWatcher(
		rw.ctx, &rw.wg,
		rw.etcdClient,
		"scheduling-rule-watcher", rw.rulesPathPrefix,
		putFn, deleteFn, postEventFn,
		clientv3.WithPrefix(),
	)
	rw.ruleWatcher.StartWatchLoop()
	return rw.ruleWatcher.WaitLoad()
}

func (rw *Watcher) initializeGroupWatcher() error {
	prefixToTrim := rw.ruleGroupPathPrefix + "/"
	putFn := func(kv *mvccpb.KeyValue) error {
		return rw.ruleStore.SaveRuleGroup(
			strings.TrimPrefix(string(kv.Key), prefixToTrim),
			string(kv.Value),
		)
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		return rw.ruleStore.DeleteRuleGroup(strings.TrimPrefix(string(kv.Key), prefixToTrim))
	}
	postEventFn := func() error {
		return nil
	}
	rw.groupWatcher = etcdutil.NewLoopWatcher(
		rw.ctx, &rw.wg,
		rw.etcdClient,
		"scheduling-rule-group-watcher", rw.ruleGroupPathPrefix,
		putFn, deleteFn, postEventFn,
		clientv3.WithPrefix(),
	)
	rw.groupWatcher.StartWatchLoop()
	return rw.groupWatcher.WaitLoad()
}

func (rw *Watcher) initializeRegionLabelWatcher() error {
	prefixToTrim := rw.regionLabelPathPrefix + "/"
	putFn := func(kv *mvccpb.KeyValue) error {
		return rw.ruleStore.SaveRegionRule(
			strings.TrimPrefix(string(kv.Key), prefixToTrim),
			string(kv.Value),
		)
	}
	deleteFn := func(kv *mvccpb.KeyValue) error {
		return rw.ruleStore.DeleteRegionRule(strings.TrimPrefix(string(kv.Key), prefixToTrim))
	}
	postEventFn := func() error {
		return nil
	}
	rw.labelWatcher = etcdutil.NewLoopWatcher(
		rw.ctx, &rw.wg,
		rw.etcdClient,
		"scheduling-region-label-watcher", rw.regionLabelPathPrefix,
		putFn, deleteFn, postEventFn,
		clientv3.WithPrefix(),
	)
	rw.labelWatcher.StartWatchLoop()
	return rw.labelWatcher.WaitLoad()
}

// Close closes the watcher.
func (rw *Watcher) Close() {
	rw.cancel()
	rw.wg.Wait()
}

// GetRuleStorage returns the rule storage.
func (rw *Watcher) GetRuleStorage() endpoint.RuleStorage {
	return rw.ruleStore
}
