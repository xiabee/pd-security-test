// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,g
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/errs"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

const (
	controllerConfigPath    = "resource_group/controller"
	maxRetry                = 10
	retryInterval           = 50 * time.Millisecond
	maxNotificationChanLen  = 200
	needTokensAmplification = 1.1
	trickleReserveDuration  = 1250 * time.Millisecond

	watchRetryInterval = 30 * time.Second
)

type selectType int

const (
	periodicReport selectType = 0
	lowToken       selectType = 1
)

var enableControllerTraceLog = atomicutil.NewBool(false)

func logControllerTrace(msg string, fields ...zap.Field) {
	if enableControllerTraceLog.Load() {
		log.Info(msg, fields...)
	}
}

// ResourceGroupKVInterceptor is used as quota limit controller for resource group using kv store.
type ResourceGroupKVInterceptor interface {
	// OnRequestWait is used to check whether resource group has enough tokens. It maybe needs to wait some time.
	OnRequestWait(ctx context.Context, resourceGroupName string, info RequestInfo) (*rmpb.Consumption, *rmpb.Consumption, time.Duration, uint32, error)
	// OnResponse is used to consume tokens after receiving response.
	OnResponse(resourceGroupName string, req RequestInfo, resp ResponseInfo) (*rmpb.Consumption, error)
	// IsBackgroundRequest If the resource group has background jobs, we should not record consumption and wait for it.
	IsBackgroundRequest(ctx context.Context, resourceGroupName, requestResource string) bool
}

// ResourceGroupProvider provides some api to interact with resource manager server.
type ResourceGroupProvider interface {
	GetResourceGroup(ctx context.Context, resourceGroupName string, opts ...pd.GetResourceGroupOption) (*rmpb.ResourceGroup, error)
	ListResourceGroups(ctx context.Context, opts ...pd.GetResourceGroupOption) ([]*rmpb.ResourceGroup, error)
	AddResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error)
	ModifyResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error)
	DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error)
	AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error)

	// meta storage client
	LoadResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, int64, error)
	Watch(ctx context.Context, key []byte, opts ...pd.OpOption) (chan []*meta_storagepb.Event, error)
	Get(ctx context.Context, key []byte, opts ...pd.OpOption) (*meta_storagepb.GetResponse, error)
}

// ResourceControlCreateOption create a ResourceGroupsController with the optional settings.
type ResourceControlCreateOption func(controller *ResourceGroupsController)

// EnableSingleGroupByKeyspace is the option to enable single group by keyspace feature.
func EnableSingleGroupByKeyspace() ResourceControlCreateOption {
	return func(controller *ResourceGroupsController) {
		controller.ruConfig.isSingleGroupByKeyspace = true
	}
}

// WithMaxWaitDuration is the option to set the max wait duration for acquiring token buckets.
func WithMaxWaitDuration(d time.Duration) ResourceControlCreateOption {
	return func(controller *ResourceGroupsController) {
		controller.ruConfig.LTBMaxWaitDuration = d
	}
}

var _ ResourceGroupKVInterceptor = (*ResourceGroupsController)(nil)

// ResourceGroupsController implements ResourceGroupKVInterceptor.
type ResourceGroupsController struct {
	clientUniqueID   uint64
	provider         ResourceGroupProvider
	groupsController sync.Map
	ruConfig         *RUConfig

	loopCtx    context.Context
	loopCancel func()

	calculators []ResourceCalculator

	// When a signal is received, it means the number of available token is low.
	lowTokenNotifyChan chan struct{}
	// When a token bucket response received from server, it will be sent to the channel.
	tokenResponseChan chan []*rmpb.TokenBucketResponse
	// When the token bucket of a resource group is updated, it will be sent to the channel.
	tokenBucketUpdateChan chan *groupCostController
	responseDeadlineCh    <-chan time.Time

	run struct {
		responseDeadline *time.Timer
		inDegradedMode   bool
		// currentRequests is used to record the request and resource group.
		// Currently, we don't do multiple `AcquireTokenBuckets`` at the same time, so there are no concurrency problems with `currentRequests`.
		currentRequests []*rmpb.TokenBucketRequest
	}

	opts []ResourceControlCreateOption

	// a cache for ru config and make concurrency safe.
	safeRuConfig atomic.Pointer[RUConfig]
}

// NewResourceGroupController returns a new ResourceGroupsController which impls ResourceGroupKVInterceptor
func NewResourceGroupController(
	ctx context.Context,
	clientUniqueID uint64,
	provider ResourceGroupProvider,
	requestUnitConfig *RequestUnitConfig,
	opts ...ResourceControlCreateOption,
) (*ResourceGroupsController, error) {
	config, err := loadServerConfig(ctx, provider)
	if err != nil {
		return nil, err
	}
	if requestUnitConfig != nil {
		config.RequestUnit = *requestUnitConfig
	}

	ruConfig := GenerateRUConfig(config)
	controller := &ResourceGroupsController{
		clientUniqueID:        clientUniqueID,
		provider:              provider,
		ruConfig:              ruConfig,
		lowTokenNotifyChan:    make(chan struct{}, 1),
		tokenResponseChan:     make(chan []*rmpb.TokenBucketResponse, 1),
		tokenBucketUpdateChan: make(chan *groupCostController, maxNotificationChanLen),
		opts:                  opts,
	}
	for _, opt := range opts {
		opt(controller)
	}
	log.Info("load resource controller config", zap.Reflect("config", config), zap.Reflect("ru-config", controller.ruConfig))
	controller.calculators = []ResourceCalculator{newKVCalculator(controller.ruConfig), newSQLCalculator(controller.ruConfig)}
	controller.safeRuConfig.Store(controller.ruConfig)
	return controller, nil
}

func loadServerConfig(ctx context.Context, provider ResourceGroupProvider) (*Config, error) {
	resp, err := provider.Get(ctx, []byte(controllerConfigPath))
	if err != nil {
		return nil, err
	}
	kvs := resp.GetKvs()
	if len(kvs) == 0 {
		log.Warn("[resource group controller] server does not save config, load config failed")
		return DefaultConfig(), nil
	}
	config := &Config{}
	err = json.Unmarshal(kvs[0].GetValue(), config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// GetConfig returns the config of controller.
func (c *ResourceGroupsController) GetConfig() *RUConfig {
	return c.safeRuConfig.Load()
}

// Source List
const (
	FromPeriodReport = "period_report"
	FromLowRU        = "low_ru"
)

// Start starts ResourceGroupController service.
func (c *ResourceGroupsController) Start(ctx context.Context) {
	c.loopCtx, c.loopCancel = context.WithCancel(ctx)
	go func() {
		if c.ruConfig.DegradedModeWaitDuration > 0 {
			c.run.responseDeadline = time.NewTimer(c.ruConfig.DegradedModeWaitDuration)
			c.run.responseDeadline.Stop()
			defer c.run.responseDeadline.Stop()
		}
		cleanupTicker := time.NewTicker(defaultGroupCleanupInterval)
		defer cleanupTicker.Stop()
		stateUpdateTicker := time.NewTicker(defaultGroupStateUpdateInterval)
		defer stateUpdateTicker.Stop()
		emergencyTokenAcquisitionTicker := time.NewTicker(defaultTargetPeriod)
		defer emergencyTokenAcquisitionTicker.Stop()

		failpoint.Inject("fastCleanup", func() {
			cleanupTicker.Stop()
			cleanupTicker = time.NewTicker(100 * time.Millisecond)
			// because of checking `gc.run.consumption` in cleanupTicker,
			// so should also change the stateUpdateTicker.
			stateUpdateTicker.Stop()
			stateUpdateTicker = time.NewTicker(200 * time.Millisecond)
		})
		failpoint.Inject("acceleratedReportingPeriod", func() {
			stateUpdateTicker.Stop()
			stateUpdateTicker = time.NewTicker(time.Millisecond * 100)
		})

		_, metaRevision, err := c.provider.LoadResourceGroups(ctx)
		if err != nil {
			log.Warn("load resource group revision failed", zap.Error(err))
		}
		resp, err := c.provider.Get(ctx, []byte(controllerConfigPath))
		if err != nil {
			log.Warn("load resource group revision failed", zap.Error(err))
		}
		cfgRevision := resp.GetHeader().GetRevision()
		var watchMetaChannel, watchConfigChannel chan []*meta_storagepb.Event
		if !c.ruConfig.isSingleGroupByKeyspace {
			// Use WithPrevKV() to get the previous key-value pair when get Delete Event.
			watchMetaChannel, err = c.provider.Watch(ctx, pd.GroupSettingsPathPrefixBytes, pd.WithRev(metaRevision), pd.WithPrefix(), pd.WithPrevKV())
			if err != nil {
				log.Warn("watch resource group meta failed", zap.Error(err))
			}
		}

		watchConfigChannel, err = c.provider.Watch(ctx, pd.ControllerConfigPathPrefixBytes, pd.WithRev(cfgRevision), pd.WithPrefix())
		if err != nil {
			log.Warn("watch resource group config failed", zap.Error(err))
		}
		watchRetryTimer := time.NewTimer(watchRetryInterval)
		defer watchRetryTimer.Stop()

		for {
			select {
			/* tickers */
			case <-cleanupTicker.C:
				c.cleanUpResourceGroup()
			case <-stateUpdateTicker.C:
				c.executeOnAllGroups((*groupCostController).updateRunState)
				c.executeOnAllGroups((*groupCostController).updateAvgRequestResourcePerSec)
				if len(c.run.currentRequests) == 0 {
					c.collectTokenBucketRequests(c.loopCtx, FromPeriodReport, periodicReport /* select resource groups which should be reported periodically */)
				}
			case <-watchRetryTimer.C:
				if !c.ruConfig.isSingleGroupByKeyspace && watchMetaChannel == nil {
					// Use WithPrevKV() to get the previous key-value pair when get Delete Event.
					watchMetaChannel, err = c.provider.Watch(ctx, pd.GroupSettingsPathPrefixBytes, pd.WithRev(metaRevision), pd.WithPrefix(), pd.WithPrevKV())
					if err != nil {
						log.Warn("watch resource group meta failed", zap.Error(err))
						watchRetryTimer.Reset(watchRetryInterval)
						failpoint.Inject("watchStreamError", func() {
							watchRetryTimer.Reset(20 * time.Millisecond)
						})
					}
				}
				if watchConfigChannel == nil {
					watchConfigChannel, err = c.provider.Watch(ctx, pd.ControllerConfigPathPrefixBytes, pd.WithRev(cfgRevision), pd.WithPrefix())
					if err != nil {
						log.Warn("watch resource group config failed", zap.Error(err))
						watchRetryTimer.Reset(watchRetryInterval)
					}
				}

			case <-emergencyTokenAcquisitionTicker.C:
				c.executeOnAllGroups((*groupCostController).resetEmergencyTokenAcquisition)
			/* channels */
			case <-c.loopCtx.Done():
				resourceGroupStatusGauge.Reset()
				return
			case <-c.responseDeadlineCh:
				c.run.inDegradedMode = true
				c.executeOnAllGroups((*groupCostController).applyDegradedMode)
				log.Warn("[resource group controller] enter degraded mode")
			case resp := <-c.tokenResponseChan:
				if resp != nil {
					c.executeOnAllGroups((*groupCostController).updateRunState)
					c.handleTokenBucketResponse(resp)
				}
				c.run.currentRequests = nil
			case <-c.lowTokenNotifyChan:
				c.executeOnAllGroups((*groupCostController).updateRunState)
				c.executeOnAllGroups((*groupCostController).updateAvgRequestResourcePerSec)
				if len(c.run.currentRequests) == 0 {
					c.collectTokenBucketRequests(c.loopCtx, FromLowRU, lowToken /* select low tokens resource group */)
				}
				if c.run.inDegradedMode {
					c.executeOnAllGroups((*groupCostController).applyDegradedMode)
				}
			case resp, ok := <-watchMetaChannel:
				failpoint.Inject("disableWatch", func() {
					if c.ruConfig.isSingleGroupByKeyspace {
						panic("disableWatch")
					}
				})
				if !ok {
					watchMetaChannel = nil
					watchRetryTimer.Reset(watchRetryInterval)
					failpoint.Inject("watchStreamError", func() {
						watchRetryTimer.Reset(20 * time.Millisecond)
					})
					continue
				}
				for _, item := range resp {
					metaRevision = item.Kv.ModRevision
					group := &rmpb.ResourceGroup{}
					switch item.Type {
					case meta_storagepb.Event_PUT:
						if err = proto.Unmarshal(item.Kv.Value, group); err != nil {
							continue
						}
						if item, ok := c.groupsController.Load(group.Name); ok {
							gc := item.(*groupCostController)
							gc.modifyMeta(group)
						}
					case meta_storagepb.Event_DELETE:
						if item.PrevKv != nil {
							if err = proto.Unmarshal(item.PrevKv.Value, group); err != nil {
								continue
							}
							if _, ok := c.groupsController.LoadAndDelete(group.Name); ok {
								resourceGroupStatusGauge.DeleteLabelValues(group.Name, group.Name)
							}
						} else {
							// Prev-kv is compacted means there must have been a delete event before this event,
							// which means that this is just a duplicated event, so we can just ignore it.
							log.Info("previous key-value pair has been compacted", zap.String("required-key", string(item.Kv.Key)), zap.String("value", string(item.Kv.Value)))
						}
					}
				}
			case resp, ok := <-watchConfigChannel:
				if !ok {
					watchConfigChannel = nil
					watchRetryTimer.Reset(watchRetryInterval)
					failpoint.Inject("watchStreamError", func() {
						watchRetryTimer.Reset(20 * time.Millisecond)
					})
					continue
				}
				for _, item := range resp {
					cfgRevision = item.Kv.ModRevision
					config := &Config{}
					if err := json.Unmarshal(item.Kv.Value, config); err != nil {
						continue
					}
					c.ruConfig = GenerateRUConfig(config)

					// Stay compatible with serverless
					for _, opt := range c.opts {
						opt(c)
					}
					copyCfg := *c.ruConfig
					c.safeRuConfig.Store(&copyCfg)
					if enableControllerTraceLog.Load() != config.EnableControllerTraceLog {
						enableControllerTraceLog.Store(config.EnableControllerTraceLog)
					}
					log.Info("load resource controller config after config changed", zap.Reflect("config", config), zap.Reflect("ruConfig", c.ruConfig))
				}

			case gc := <-c.tokenBucketUpdateChan:
				now := gc.run.now
				go gc.handleTokenBucketUpdateEvent(c.loopCtx, now)
			}
		}
	}()
}

// Stop stops ResourceGroupController service.
func (c *ResourceGroupsController) Stop() error {
	if c.loopCancel == nil {
		return errors.Errorf("resource groups controller does not start")
	}
	c.loopCancel()
	return nil
}

// tryGetResourceGroup will try to get the resource group controller from local cache first,
// if the local cache misses, it will then call gRPC to fetch the resource group info from server.
func (c *ResourceGroupsController) tryGetResourceGroup(ctx context.Context, name string) (*groupCostController, error) {
	// Get from the local cache first.
	if tmp, ok := c.groupsController.Load(name); ok {
		return tmp.(*groupCostController), nil
	}
	// Call gRPC to fetch the resource group info.
	group, err := c.provider.GetResourceGroup(ctx, name)
	if err != nil {
		return nil, err
	}
	if group == nil {
		return nil, errors.Errorf("%s does not exists", name)
	}
	// Check again to prevent initializing the same resource group concurrently.
	if tmp, ok := c.groupsController.Load(name); ok {
		gc := tmp.(*groupCostController)
		return gc, nil
	}
	// Initialize the resource group controller.
	gc, err := newGroupCostController(group, c.ruConfig, c.lowTokenNotifyChan, c.tokenBucketUpdateChan)
	if err != nil {
		return nil, err
	}
	// TODO: re-init the state if user change mode from RU to RAW mode.
	gc.initRunState()
	// Check again to prevent initializing the same resource group concurrently.
	tmp, loaded := c.groupsController.LoadOrStore(group.GetName(), gc)
	if !loaded {
		resourceGroupStatusGauge.WithLabelValues(name, group.Name).Set(1)
		log.Info("[resource group controller] create resource group cost controller", zap.String("name", group.GetName()))
	}
	return tmp.(*groupCostController), nil
}

func (c *ResourceGroupsController) cleanUpResourceGroup() {
	c.groupsController.Range(func(key, value any) bool {
		resourceGroupName := key.(string)
		gc := value.(*groupCostController)
		// Check for stale resource groups, which will be deleted when consumption is continuously unchanged.
		gc.mu.Lock()
		latestConsumption := *gc.mu.consumption
		gc.mu.Unlock()
		if equalRU(latestConsumption, *gc.run.consumption) {
			if gc.tombstone {
				c.groupsController.Delete(resourceGroupName)
				resourceGroupStatusGauge.DeleteLabelValues(resourceGroupName, resourceGroupName)
				return true
			}
			gc.tombstone = true
		} else {
			gc.tombstone = false
		}
		return true
	})
}

func (c *ResourceGroupsController) executeOnAllGroups(f func(controller *groupCostController)) {
	c.groupsController.Range(func(name, value any) bool {
		f(value.(*groupCostController))
		return true
	})
}

func (c *ResourceGroupsController) handleTokenBucketResponse(resp []*rmpb.TokenBucketResponse) {
	if c.responseDeadlineCh != nil {
		if c.run.responseDeadline.Stop() {
			select {
			case <-c.run.responseDeadline.C:
			default:
			}
		}
		c.responseDeadlineCh = nil
	}
	c.run.inDegradedMode = false
	for _, res := range resp {
		name := res.GetResourceGroupName()
		v, ok := c.groupsController.Load(name)
		if !ok {
			log.Warn("[resource group controller] a non-existent resource group was found when handle token response", zap.String("name", name))
			continue
		}
		gc := v.(*groupCostController)
		gc.handleTokenBucketResponse(res)
	}
}

func (c *ResourceGroupsController) collectTokenBucketRequests(ctx context.Context, source string, typ selectType) {
	c.run.currentRequests = make([]*rmpb.TokenBucketRequest, 0)
	c.groupsController.Range(func(name, value any) bool {
		gc := value.(*groupCostController)
		request := gc.collectRequestAndConsumption(typ)
		if request != nil {
			c.run.currentRequests = append(c.run.currentRequests, request)
			gc.tokenRequestCounter.Inc()
		}
		return true
	})
	if len(c.run.currentRequests) > 0 {
		c.sendTokenBucketRequests(ctx, c.run.currentRequests, source)
	}
}

func (c *ResourceGroupsController) sendTokenBucketRequests(ctx context.Context, requests []*rmpb.TokenBucketRequest, source string) {
	now := time.Now()
	req := &rmpb.TokenBucketsRequest{
		Requests:              requests,
		TargetRequestPeriodMs: uint64(defaultTargetPeriod / time.Millisecond),
		ClientUniqueId:        c.clientUniqueID,
	}
	if c.ruConfig.DegradedModeWaitDuration > 0 && c.responseDeadlineCh == nil {
		c.run.responseDeadline.Reset(c.ruConfig.DegradedModeWaitDuration)
		c.responseDeadlineCh = c.run.responseDeadline.C
	}
	go func() {
		logControllerTrace("[resource group controller] send token bucket request", zap.Time("now", now), zap.Any("req", req.Requests), zap.String("source", source))
		resp, err := c.provider.AcquireTokenBuckets(ctx, req)
		latency := time.Since(now)
		if err != nil {
			// Don't log any errors caused by the stopper canceling the context.
			if !errors.ErrorEqual(err, context.Canceled) {
				log.L().Sugar().Infof("[resource group controller] token bucket rpc error: %v", err)
			}
			resp = nil
			failedTokenRequestDuration.Observe(latency.Seconds())
		} else {
			successfulTokenRequestDuration.Observe(latency.Seconds())
		}
		logControllerTrace("[resource group controller] token bucket response", zap.Time("now", time.Now()), zap.Any("resp", resp), zap.String("source", source), zap.Duration("latency", latency))
		c.tokenResponseChan <- resp
	}()
}

// OnRequestWait is used to check whether resource group has enough tokens. It maybe needs to wait some time.
func (c *ResourceGroupsController) OnRequestWait(
	ctx context.Context, resourceGroupName string, info RequestInfo,
) (*rmpb.Consumption, *rmpb.Consumption, time.Duration, uint32, error) {
	gc, err := c.tryGetResourceGroup(ctx, resourceGroupName)
	if err != nil {
		return nil, nil, time.Duration(0), 0, err
	}
	return gc.onRequestWait(ctx, info)
}

// OnResponse is used to consume tokens after receiving response
func (c *ResourceGroupsController) OnResponse(
	resourceGroupName string, req RequestInfo, resp ResponseInfo,
) (*rmpb.Consumption, error) {
	tmp, ok := c.groupsController.Load(resourceGroupName)
	if !ok {
		log.Warn("[resource group controller] resource group name does not exist", zap.String("name", resourceGroupName))
		return &rmpb.Consumption{}, nil
	}
	return tmp.(*groupCostController).onResponse(req, resp)
}

// IsBackgroundRequest If the resource group has background jobs, we should not record consumption and wait for it.
func (c *ResourceGroupsController) IsBackgroundRequest(ctx context.Context,
	resourceGroupName, requestResource string) bool {
	gc, err := c.tryGetResourceGroup(ctx, resourceGroupName)
	if err != nil {
		failedRequestCounter.WithLabelValues(resourceGroupName).Inc()
		return false
	}

	return c.checkBackgroundSettings(ctx, gc.getMeta().BackgroundSettings, requestResource)
}

func (c *ResourceGroupsController) checkBackgroundSettings(ctx context.Context, bg *rmpb.BackgroundSettings, requestResource string) bool {
	// fallback to default resource group.
	if bg == nil {
		resourceGroupName := "default"
		gc, err := c.tryGetResourceGroup(ctx, resourceGroupName)
		if err != nil {
			failedRequestCounter.WithLabelValues(resourceGroupName).Inc()
			return false
		}
		bg = gc.getMeta().BackgroundSettings
	}

	if bg == nil || len(requestResource) == 0 || len(bg.JobTypes) == 0 {
		return false
	}

	if idx := strings.LastIndex(requestResource, "_"); idx != -1 {
		return slices.Contains(bg.JobTypes, requestResource[idx+1:])
	}

	return false
}

// GetResourceGroup returns the meta setting of the given resource group name.
func (c *ResourceGroupsController) GetResourceGroup(resourceGroupName string) (*rmpb.ResourceGroup, error) {
	gc, err := c.tryGetResourceGroup(c.loopCtx, resourceGroupName)
	if err != nil {
		return nil, err
	}
	return gc.getMeta(), nil
}

type groupCostController struct {
	// invariant attributes
	name    string
	mode    rmpb.GroupMode
	mainCfg *RUConfig
	// meta info
	meta     *rmpb.ResourceGroup
	metaLock sync.RWMutex

	// following fields are used for token limiter.
	calculators    []ResourceCalculator
	handleRespFunc func(*rmpb.TokenBucketResponse)

	successfulRequestDuration  prometheus.Observer
	failedLimitReserveDuration prometheus.Observer
	requestRetryCounter        prometheus.Counter
	failedRequestCounter       prometheus.Counter
	tokenRequestCounter        prometheus.Counter

	mu struct {
		sync.Mutex
		consumption   *rmpb.Consumption
		storeCounter  map[uint64]*rmpb.Consumption
		globalCounter *rmpb.Consumption
	}

	// fast path to make once token limit with un-limit burst.
	burstable *atomic.Bool

	lowRUNotifyChan       chan<- struct{}
	tokenBucketUpdateChan chan<- *groupCostController

	// run contains the state that is updated by the main loop.
	run struct {
		now             time.Time
		lastRequestTime time.Time

		// requestInProgress is set true when sending token bucket request.
		// And it is set false when receiving token bucket response.
		// This triggers a retry attempt on the next tick.
		requestInProgress bool

		// targetPeriod stores the value of the TargetPeriodSetting setting at the
		// last update.
		targetPeriod time.Duration

		// consumptions stores the last value of mu.consumption.
		// requestUnitConsumptions []*rmpb.RequestUnitItem
		// resourceConsumptions    []*rmpb.ResourceItem
		consumption *rmpb.Consumption

		// lastRequestUnitConsumptions []*rmpb.RequestUnitItem
		// lastResourceConsumptions    []*rmpb.ResourceItem
		lastRequestConsumption *rmpb.Consumption

		// initialRequestCompleted is set to true when the first token bucket
		// request completes successfully.
		initialRequestCompleted bool

		resourceTokens    map[rmpb.RawResourceType]*tokenCounter
		requestUnitTokens map[rmpb.RequestUnitType]*tokenCounter
	}

	tombstone bool
}

type tokenCounter struct {
	getTokenBucketFunc func() *rmpb.TokenBucket

	// avgRUPerSec is an exponentially-weighted moving average of the RU
	// consumption per second; used to estimate the RU requirements for the next
	// request.
	avgRUPerSec float64
	// lastSecRU is the consumption.RU value when avgRUPerSec was last updated.
	avgRUPerSecLastRU float64
	avgLastTime       time.Time

	notify struct {
		mu                         sync.Mutex
		setupNotificationCh        <-chan time.Time
		setupNotificationThreshold float64
		setupNotificationTimer     *time.Timer
	}

	lastDeadline time.Time
	lastRate     float64

	limiter *Limiter

	inDegradedMode bool
}

func newGroupCostController(
	group *rmpb.ResourceGroup,
	mainCfg *RUConfig,
	lowRUNotifyChan chan struct{},
	tokenBucketUpdateChan chan *groupCostController,
) (*groupCostController, error) {
	switch group.Mode {
	case rmpb.GroupMode_RUMode:
		if group.RUSettings.RU == nil || group.RUSettings.RU.Settings == nil {
			return nil, errs.ErrClientResourceGroupConfigUnavailable.FastGenByArgs("not configured")
		}
	default:
		return nil, errs.ErrClientResourceGroupConfigUnavailable.FastGenByArgs("not supports the resource type")
	}
	gc := &groupCostController{
		meta:                       group,
		name:                       group.Name,
		mainCfg:                    mainCfg,
		mode:                       group.GetMode(),
		successfulRequestDuration:  successfulRequestDuration.WithLabelValues(group.Name, group.Name),
		failedLimitReserveDuration: failedLimitReserveDuration.WithLabelValues(group.Name, group.Name),
		failedRequestCounter:       failedRequestCounter.WithLabelValues(group.Name, group.Name),
		requestRetryCounter:        requestRetryCounter.WithLabelValues(group.Name, group.Name),
		tokenRequestCounter:        resourceGroupTokenRequestCounter.WithLabelValues(group.Name, group.Name),
		calculators: []ResourceCalculator{
			newKVCalculator(mainCfg),
			newSQLCalculator(mainCfg),
		},
		tokenBucketUpdateChan: tokenBucketUpdateChan,
		lowRUNotifyChan:       lowRUNotifyChan,
		burstable:             &atomic.Bool{},
	}

	switch gc.mode {
	case rmpb.GroupMode_RUMode:
		gc.handleRespFunc = gc.handleRUTokenResponse
	case rmpb.GroupMode_RawMode:
		gc.handleRespFunc = gc.handleRawResourceTokenResponse
	}

	gc.mu.consumption = &rmpb.Consumption{}
	gc.mu.storeCounter = make(map[uint64]*rmpb.Consumption)
	gc.mu.globalCounter = &rmpb.Consumption{}
	return gc, nil
}

func (gc *groupCostController) initRunState() {
	now := time.Now()
	gc.run.now = now
	gc.run.lastRequestTime = now.Add(-defaultTargetPeriod)
	gc.run.targetPeriod = defaultTargetPeriod
	gc.run.consumption = &rmpb.Consumption{}
	gc.run.lastRequestConsumption = &rmpb.Consumption{SqlLayerCpuTimeMs: getSQLProcessCPUTime(gc.mainCfg.isSingleGroupByKeyspace)}

	isBurstable := true
	cfgFunc := func(tb *rmpb.TokenBucket) tokenBucketReconfigureArgs {
		initialToken := float64(tb.Settings.FillRate)
		cfg := tokenBucketReconfigureArgs{
			NewTokens: initialToken,
			NewBurst:  tb.Settings.BurstLimit,
			// This is to trigger token requests as soon as resource group start consuming tokens.
			NotifyThreshold: math.Max(initialToken*tokenReserveFraction, 1),
		}
		if cfg.NewBurst >= 0 {
			cfg.NewBurst = 0
		}
		if tb.Settings.BurstLimit >= 0 {
			isBurstable = false
		}
		return cfg
	}

	gc.metaLock.RLock()
	defer gc.metaLock.RUnlock()
	switch gc.mode {
	case rmpb.GroupMode_RUMode:
		gc.run.requestUnitTokens = make(map[rmpb.RequestUnitType]*tokenCounter)
		for typ := range requestUnitLimitTypeList {
			limiter := NewLimiterWithCfg(now, cfgFunc(getRUTokenBucketSetting(gc.meta, typ)), gc.lowRUNotifyChan)
			counter := &tokenCounter{
				limiter:     limiter,
				avgRUPerSec: 0,
				avgLastTime: now,
				getTokenBucketFunc: func() *rmpb.TokenBucket {
					return getRUTokenBucketSetting(gc.meta, typ)
				},
			}
			gc.run.requestUnitTokens[typ] = counter
		}
	case rmpb.GroupMode_RawMode:
		gc.run.resourceTokens = make(map[rmpb.RawResourceType]*tokenCounter)
		for typ := range requestResourceLimitTypeList {
			limiter := NewLimiterWithCfg(now, cfgFunc(getRawResourceTokenBucketSetting(gc.meta, typ)), gc.lowRUNotifyChan)
			counter := &tokenCounter{
				limiter:     limiter,
				avgRUPerSec: 0,
				avgLastTime: now,
				getTokenBucketFunc: func() *rmpb.TokenBucket {
					return getRawResourceTokenBucketSetting(gc.meta, typ)
				},
			}
			gc.run.resourceTokens[typ] = counter
		}
	}
	gc.burstable.Store(isBurstable)
}

// applyDegradedMode is used to apply degraded mode for resource group which is in low-process.
func (gc *groupCostController) applyDegradedMode() {
	switch gc.mode {
	case rmpb.GroupMode_RawMode:
		gc.applyBasicConfigForRawResourceTokenCounter()
	case rmpb.GroupMode_RUMode:
		gc.applyBasicConfigForRUTokenCounters()
	}
}

func (gc *groupCostController) updateRunState() {
	newTime := time.Now()
	gc.mu.Lock()
	for _, calc := range gc.calculators {
		calc.Trickle(gc.mu.consumption)
	}
	*gc.run.consumption = *gc.mu.consumption
	gc.mu.Unlock()
	logControllerTrace("[resource group controller] update run state", zap.Any("request-unit-consumption", gc.run.consumption))
	gc.run.now = newTime
}

func (gc *groupCostController) updateAvgRequestResourcePerSec() {
	switch gc.mode {
	case rmpb.GroupMode_RawMode:
		gc.updateAvgRaWResourcePerSec()
	case rmpb.GroupMode_RUMode:
		gc.updateAvgRUPerSec()
	}
}

func (gc *groupCostController) resetEmergencyTokenAcquisition() {
	switch gc.mode {
	case rmpb.GroupMode_RawMode:
		for _, counter := range gc.run.resourceTokens {
			counter.limiter.ResetRemainingNotifyTimes()
		}
	case rmpb.GroupMode_RUMode:
		for _, counter := range gc.run.requestUnitTokens {
			counter.limiter.ResetRemainingNotifyTimes()
		}
	}
}

func (gc *groupCostController) handleTokenBucketUpdateEvent(ctx context.Context, now time.Time) {
	switch gc.mode {
	case rmpb.GroupMode_RawMode:
		for _, counter := range gc.run.resourceTokens {
			counter.notify.mu.Lock()
			ch := counter.notify.setupNotificationCh
			counter.notify.mu.Unlock()
			if ch == nil {
				continue
			}
			select {
			case <-ch:
				counter.notify.mu.Lock()
				counter.notify.setupNotificationTimer = nil
				counter.notify.setupNotificationCh = nil
				threshold := counter.notify.setupNotificationThreshold
				counter.notify.mu.Unlock()
				counter.limiter.SetupNotificationThreshold(now, threshold)
			case <-ctx.Done():
				return
			}
		}

	case rmpb.GroupMode_RUMode:
		for _, counter := range gc.run.requestUnitTokens {
			counter.notify.mu.Lock()
			ch := counter.notify.setupNotificationCh
			counter.notify.mu.Unlock()
			if ch == nil {
				continue
			}
			select {
			case <-ch:
				counter.notify.mu.Lock()
				counter.notify.setupNotificationTimer = nil
				counter.notify.setupNotificationCh = nil
				threshold := counter.notify.setupNotificationThreshold
				counter.notify.mu.Unlock()
				counter.limiter.SetupNotificationThreshold(now, threshold)
			case <-ctx.Done():
				return
			}
		}
	}
}

func (gc *groupCostController) updateAvgRaWResourcePerSec() {
	isBurstable := true
	for typ, counter := range gc.run.resourceTokens {
		if counter.limiter.GetBurst() >= 0 {
			isBurstable = false
		}
		if !gc.calcAvg(counter, getRawResourceValueFromConsumption(gc.run.consumption, typ)) {
			continue
		}
		logControllerTrace("[resource group controller] update avg raw resource per sec", zap.String("name", gc.name), zap.String("type", rmpb.RawResourceType_name[int32(typ)]), zap.Float64("avg-ru-per-sec", counter.avgRUPerSec))
	}
	gc.burstable.Store(isBurstable)
}

func (gc *groupCostController) updateAvgRUPerSec() {
	isBurstable := true
	for typ, counter := range gc.run.requestUnitTokens {
		if counter.limiter.GetBurst() >= 0 {
			isBurstable = false
		}
		if !gc.calcAvg(counter, getRUValueFromConsumption(gc.run.consumption, typ)) {
			continue
		}
		logControllerTrace("[resource group controller] update avg ru per sec", zap.String("name", gc.name), zap.String("type", rmpb.RequestUnitType_name[int32(typ)]), zap.Float64("avg-ru-per-sec", counter.avgRUPerSec))
	}
	gc.burstable.Store(isBurstable)
}

func (gc *groupCostController) calcAvg(counter *tokenCounter, new float64) bool {
	deltaDuration := gc.run.now.Sub(counter.avgLastTime)
	failpoint.Inject("acceleratedReportingPeriod", func() {
		deltaDuration = 100 * time.Millisecond
	})
	delta := (new - counter.avgRUPerSecLastRU) / deltaDuration.Seconds()
	counter.avgRUPerSec = movingAvgFactor*counter.avgRUPerSec + (1-movingAvgFactor)*delta
	failpoint.Inject("acceleratedSpeedTrend", func() {
		if delta > 0 {
			counter.avgRUPerSec = 1000
		} else {
			counter.avgRUPerSec = 0
		}
	})
	counter.avgLastTime = gc.run.now
	counter.avgRUPerSecLastRU = new
	return true
}

func (gc *groupCostController) shouldReportConsumption() bool {
	if !gc.run.initialRequestCompleted {
		return true
	}
	timeSinceLastRequest := gc.run.now.Sub(gc.run.lastRequestTime)
	failpoint.Inject("acceleratedReportingPeriod", func() {
		timeSinceLastRequest = extendedReportingPeriodFactor * defaultTargetPeriod
	})
	// Due to `gc.run.lastRequestTime` update operations late in this logic,
	// so `timeSinceLastRequest` is less than defaultGroupStateUpdateInterval a little bit, lead to actual report period is greater than defaultTargetPeriod.
	// Add defaultGroupStateUpdateInterval/2 as duration buffer to avoid it.
	if timeSinceLastRequest+defaultGroupStateUpdateInterval/2 >= defaultTargetPeriod {
		if timeSinceLastRequest >= extendedReportingPeriodFactor*defaultTargetPeriod {
			return true
		}
		switch gc.mode {
		case rmpb.GroupMode_RUMode:
			for typ := range requestUnitLimitTypeList {
				if getRUValueFromConsumption(gc.run.consumption, typ)-getRUValueFromConsumption(gc.run.lastRequestConsumption, typ) >= consumptionsReportingThreshold {
					return true
				}
			}
		case rmpb.GroupMode_RawMode:
			for typ := range requestResourceLimitTypeList {
				if getRawResourceValueFromConsumption(gc.run.consumption, typ)-getRawResourceValueFromConsumption(gc.run.lastRequestConsumption, typ) >= consumptionsReportingThreshold {
					return true
				}
			}
		}
	}
	return false
}

func (gc *groupCostController) handleTokenBucketResponse(resp *rmpb.TokenBucketResponse) {
	gc.run.requestInProgress = false
	gc.handleRespFunc(resp)
	gc.run.initialRequestCompleted = true
}

func (gc *groupCostController) handleRawResourceTokenResponse(resp *rmpb.TokenBucketResponse) {
	for _, grantedTB := range resp.GetGrantedResourceTokens() {
		typ := grantedTB.GetType()
		counter, ok := gc.run.resourceTokens[typ]
		if !ok {
			log.Warn("[resource group controller] not support this resource type", zap.String("type", rmpb.RawResourceType_name[int32(typ)]))
			continue
		}
		gc.modifyTokenCounter(counter, grantedTB.GetGrantedTokens(), grantedTB.GetTrickleTimeMs())
	}
}

func (gc *groupCostController) handleRUTokenResponse(resp *rmpb.TokenBucketResponse) {
	for _, grantedTB := range resp.GetGrantedRUTokens() {
		typ := grantedTB.GetType()
		counter, ok := gc.run.requestUnitTokens[typ]
		if !ok {
			log.Warn("[resource group controller] not support this resource type", zap.String("type", rmpb.RawResourceType_name[int32(typ)]))
			continue
		}
		gc.modifyTokenCounter(counter, grantedTB.GetGrantedTokens(), grantedTB.GetTrickleTimeMs())
	}
}

func (gc *groupCostController) applyBasicConfigForRUTokenCounters() {
	for typ, counter := range gc.run.requestUnitTokens {
		if !counter.limiter.IsLowTokens() {
			continue
		}
		if counter.inDegradedMode {
			continue
		}
		counter.inDegradedMode = true
		initCounterNotify(counter)
		var cfg tokenBucketReconfigureArgs
		fillRate := counter.getTokenBucketFunc().Settings.FillRate
		cfg.NewBurst = int64(fillRate)
		cfg.NewRate = float64(fillRate)
		failpoint.Inject("degradedModeRU", func() {
			cfg.NewRate = 99999999
		})
		counter.limiter.Reconfigure(gc.run.now, cfg, resetLowProcess())
		log.Info("[resource group controller] resource token bucket enter degraded mode", zap.String("resource-group", gc.name), zap.String("type", rmpb.RequestUnitType_name[int32(typ)]))
	}
}

func (gc *groupCostController) applyBasicConfigForRawResourceTokenCounter() {
	for _, counter := range gc.run.resourceTokens {
		if !counter.limiter.IsLowTokens() {
			continue
		}
		initCounterNotify(counter)
		var cfg tokenBucketReconfigureArgs
		fillRate := counter.getTokenBucketFunc().Settings.FillRate
		cfg.NewBurst = int64(fillRate)
		cfg.NewRate = float64(fillRate)
		counter.limiter.Reconfigure(gc.run.now, cfg, resetLowProcess())
	}
}

func (gc *groupCostController) modifyTokenCounter(counter *tokenCounter, bucket *rmpb.TokenBucket, trickleTimeMs int64) {
	granted := bucket.GetTokens()
	if !counter.lastDeadline.IsZero() {
		// If last request came with a trickle duration, we may have RUs that were
		// not made available to the bucket yet; throw them together with the newly
		// granted RUs.
		if since := counter.lastDeadline.Sub(gc.run.now); since > 0 {
			granted += counter.lastRate * since.Seconds()
		}
	}
	initCounterNotify(counter)
	counter.inDegradedMode = false
	var cfg tokenBucketReconfigureArgs
	cfg.NewBurst = bucket.GetSettings().GetBurstLimit()
	// When trickleTimeMs equals zero, server has enough tokens and does not need to
	// limit client consume token. So all token is granted to client right now.
	if trickleTimeMs == 0 {
		cfg.NewTokens = granted
		cfg.NewRate = float64(bucket.GetSettings().FillRate)
		counter.lastDeadline = time.Time{}
		cfg.NotifyThreshold = math.Min(granted+counter.limiter.AvailableTokens(gc.run.now), counter.avgRUPerSec*defaultTargetPeriod.Seconds()) * notifyFraction
		if cfg.NewBurst < 0 {
			cfg.NewTokens = float64(counter.getTokenBucketFunc().Settings.FillRate)
		}
	} else {
		// Otherwise the granted token is delivered to the client by fill rate.
		cfg.NewTokens = 0
		trickleDuration := time.Duration(trickleTimeMs) * time.Millisecond
		deadline := gc.run.now.Add(trickleDuration)
		cfg.NewRate = float64(bucket.GetSettings().FillRate) + granted/trickleDuration.Seconds()

		timerDuration := trickleDuration - trickleReserveDuration
		if timerDuration <= 0 {
			timerDuration = (trickleDuration + trickleReserveDuration) / 2
		}
		counter.notify.mu.Lock()
		counter.notify.setupNotificationTimer = time.NewTimer(timerDuration)
		counter.notify.setupNotificationCh = counter.notify.setupNotificationTimer.C
		counter.notify.setupNotificationThreshold = 1
		counter.notify.mu.Unlock()
		counter.lastDeadline = deadline
		select {
		case gc.tokenBucketUpdateChan <- gc:
		default:
		}
	}

	counter.lastRate = cfg.NewRate
	counter.limiter.Reconfigure(gc.run.now, cfg, resetLowProcess())
}

func initCounterNotify(counter *tokenCounter) {
	counter.notify.mu.Lock()
	if counter.notify.setupNotificationTimer != nil {
		counter.notify.setupNotificationTimer.Stop()
		counter.notify.setupNotificationTimer = nil
		counter.notify.setupNotificationCh = nil
	}
	counter.notify.mu.Unlock()
}

func (gc *groupCostController) collectRequestAndConsumption(selectTyp selectType) *rmpb.TokenBucketRequest {
	req := &rmpb.TokenBucketRequest{
		ResourceGroupName: gc.name,
	}
	// collect request resource
	selected := gc.run.requestInProgress
	failpoint.Inject("triggerUpdate", func() {
		selected = true
	})
	switch gc.mode {
	case rmpb.GroupMode_RawMode:
		requests := make([]*rmpb.RawResourceItem, 0, len(requestResourceLimitTypeList))
		for typ, counter := range gc.run.resourceTokens {
			switch selectTyp {
			case periodicReport:
				selected = selected || gc.shouldReportConsumption()
				fallthrough
			case lowToken:
				if counter.limiter.IsLowTokens() {
					selected = true
				}
			}
			request := &rmpb.RawResourceItem{
				Type:  typ,
				Value: gc.calcRequest(counter),
			}
			requests = append(requests, request)
		}
		req.Request = &rmpb.TokenBucketRequest_RawResourceItems{
			RawResourceItems: &rmpb.TokenBucketRequest_RequestRawResource{
				RequestRawResource: requests,
			},
		}
	case rmpb.GroupMode_RUMode:
		requests := make([]*rmpb.RequestUnitItem, 0, len(requestUnitLimitTypeList))
		for typ, counter := range gc.run.requestUnitTokens {
			switch selectTyp {
			case periodicReport:
				selected = selected || gc.shouldReportConsumption()
				fallthrough
			case lowToken:
				if counter.limiter.IsLowTokens() {
					selected = true
				}
			}
			request := &rmpb.RequestUnitItem{
				Type:  typ,
				Value: gc.calcRequest(counter),
			}
			requests = append(requests, request)
		}
		req.Request = &rmpb.TokenBucketRequest_RuItems{
			RuItems: &rmpb.TokenBucketRequest_RequestRU{
				RequestRU: requests,
			},
		}
	}
	if !selected {
		return nil
	}
	req.ConsumptionSinceLastRequest = updateDeltaConsumption(gc.run.lastRequestConsumption, gc.run.consumption)
	gc.run.lastRequestTime = time.Now()
	gc.run.requestInProgress = true
	return req
}

func (gc *groupCostController) getMeta() *rmpb.ResourceGroup {
	gc.metaLock.RLock()
	defer gc.metaLock.RUnlock()
	return gc.meta
}

func (gc *groupCostController) modifyMeta(newMeta *rmpb.ResourceGroup) {
	gc.metaLock.Lock()
	defer gc.metaLock.Unlock()
	gc.meta = newMeta
}

func (gc *groupCostController) calcRequest(counter *tokenCounter) float64 {
	// `needTokensAmplification` is used to properly amplify a need. The reason is that in the current implementation,
	// the token returned from the server determines the average consumption speed.
	// Therefore, when the fillrate of resource group increases, `needTokensAmplification` can enable the client to obtain more tokens.
	value := counter.avgRUPerSec * gc.run.targetPeriod.Seconds() * needTokensAmplification
	value -= counter.limiter.AvailableTokens(gc.run.now)
	if value < 0 {
		value = 0
	}
	return value
}

func (gc *groupCostController) onRequestWait(
	ctx context.Context, info RequestInfo,
) (*rmpb.Consumption, *rmpb.Consumption, time.Duration, uint32, error) {
	delta := &rmpb.Consumption{}
	for _, calc := range gc.calculators {
		calc.BeforeKVRequest(delta, info)
	}

	gc.mu.Lock()
	add(gc.mu.consumption, delta)
	gc.mu.Unlock()
	var waitDuration time.Duration

	if !gc.burstable.Load() {
		var err error
		now := time.Now()
		var i int
		var d time.Duration
	retryLoop:
		for i = 0; i < maxRetry; i++ {
			switch gc.mode {
			case rmpb.GroupMode_RawMode:
				res := make([]*Reservation, 0, len(requestResourceLimitTypeList))
				for typ, counter := range gc.run.resourceTokens {
					if v := getRawResourceValueFromConsumption(delta, typ); v > 0 {
						res = append(res, counter.limiter.Reserve(ctx, gc.mainCfg.LTBMaxWaitDuration, now, v))
					}
				}
				if d, err = WaitReservations(ctx, now, res); err == nil {
					break retryLoop
				}
			case rmpb.GroupMode_RUMode:
				res := make([]*Reservation, 0, len(requestUnitLimitTypeList))
				for typ, counter := range gc.run.requestUnitTokens {
					if v := getRUValueFromConsumption(delta, typ); v > 0 {
						res = append(res, counter.limiter.Reserve(ctx, gc.mainCfg.LTBMaxWaitDuration, now, v))
					}
				}
				if d, err = WaitReservations(ctx, now, res); err == nil {
					break retryLoop
				}
			}
			gc.requestRetryCounter.Inc()
			time.Sleep(retryInterval)
			waitDuration += retryInterval
		}
		if err != nil {
			gc.failedRequestCounter.Inc()
			if d.Seconds() > 0 {
				gc.failedLimitReserveDuration.Observe(d.Seconds())
			}
			gc.mu.Lock()
			sub(gc.mu.consumption, delta)
			gc.mu.Unlock()
			failpoint.Inject("triggerUpdate", func() {
				gc.lowRUNotifyChan <- struct{}{}
			})
			return nil, nil, waitDuration, 0, err
		}
		gc.successfulRequestDuration.Observe(d.Seconds())
		waitDuration += d
	}

	gc.mu.Lock()
	// Calculate the penalty of the store
	penalty := &rmpb.Consumption{}
	if storeCounter, exist := gc.mu.storeCounter[info.StoreID()]; exist {
		*penalty = *gc.mu.globalCounter
		sub(penalty, storeCounter)
	} else {
		gc.mu.storeCounter[info.StoreID()] = &rmpb.Consumption{}
	}
	// More accurately, it should be reset when the request succeed. But it would cause all concurrent requests piggyback large delta which inflates penalty.
	// So here resets it directly as failure is rare.
	*gc.mu.storeCounter[info.StoreID()] = *gc.mu.globalCounter
	gc.mu.Unlock()

	return delta, penalty, waitDuration, gc.getMeta().GetPriority(), nil
}

func (gc *groupCostController) onResponse(
	req RequestInfo, resp ResponseInfo,
) (*rmpb.Consumption, error) {
	delta := &rmpb.Consumption{}
	for _, calc := range gc.calculators {
		calc.AfterKVRequest(delta, req, resp)
	}
	if !gc.burstable.Load() {
		switch gc.mode {
		case rmpb.GroupMode_RawMode:
			for typ, counter := range gc.run.resourceTokens {
				if v := getRawResourceValueFromConsumption(delta, typ); v > 0 {
					counter.limiter.RemoveTokens(time.Now(), v)
				}
			}
		case rmpb.GroupMode_RUMode:
			for typ, counter := range gc.run.requestUnitTokens {
				if v := getRUValueFromConsumption(delta, typ); v > 0 {
					counter.limiter.RemoveTokens(time.Now(), v)
				}
			}
		}
	}

	gc.mu.Lock()
	// Record the consumption of the request
	add(gc.mu.consumption, delta)
	// Record the consumption of the request by store
	count := &rmpb.Consumption{}
	*count = *delta
	// As the penalty is only counted when the request is completed, so here needs to calculate the write cost which is added in `BeforeKVRequest`
	for _, calc := range gc.calculators {
		calc.BeforeKVRequest(count, req)
	}
	add(gc.mu.storeCounter[req.StoreID()], count)
	add(gc.mu.globalCounter, count)
	gc.mu.Unlock()

	return delta, nil
}

// GetActiveResourceGroup is used to get action resource group.
// This is used for test only.
func (c *ResourceGroupsController) GetActiveResourceGroup(resourceGroupName string) *rmpb.ResourceGroup {
	tmp, ok := c.groupsController.Load(resourceGroupName)
	if !ok {
		return nil
	}
	return tmp.(*groupCostController).getMeta()
}

// This is used for test only.
func (gc *groupCostController) getKVCalculator() *KVCalculator {
	for _, calc := range gc.calculators {
		if kvCalc, ok := calc.(*KVCalculator); ok {
			return kvCalc
		}
	}
	return nil
}
