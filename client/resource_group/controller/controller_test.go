// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/errs"
)

func createTestGroupCostController(re *require.Assertions) *groupCostController {
	group := &rmpb.ResourceGroup{
		Name: "test",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: 1000,
				},
			},
		},
	}
	ch1 := make(chan notifyMsg)
	ch2 := make(chan *groupCostController)
	gc, err := newGroupCostController(group, DefaultRUConfig(), ch1, ch2)
	re.NoError(err)
	return gc
}

func TestGroupControlBurstable(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	gc.initRunState()
	args := tokenBucketReconfigureArgs{
		NewRate:  1000,
		NewBurst: -1,
	}
	for _, counter := range gc.run.requestUnitTokens {
		counter.limiter.Reconfigure(time.Now(), args)
	}
	gc.updateAvgRequestResourcePerSec()
	re.Equal(gc.burstable.Load(), true)
}

func TestRequestAndResponseConsumption(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	gc.initRunState()
	testCases := []struct {
		req  *TestRequestInfo
		resp *TestResponseInfo
	}{
		// Write request
		{
			req: &TestRequestInfo{
				isWrite:    true,
				writeBytes: 100,
			},
			resp: &TestResponseInfo{
				readBytes: 100,
				succeed:   true,
			},
		},
		// Read request
		{
			req: &TestRequestInfo{
				isWrite:    false,
				writeBytes: 0,
			},
			resp: &TestResponseInfo{
				readBytes: 100,
				kvCPU:     100 * time.Millisecond,
				succeed:   true,
			},
		},
	}
	kvCalculator := gc.getKVCalculator()
	for idx, testCase := range testCases {
		caseNum := fmt.Sprintf("case %d", idx)
		consumption, _, err := gc.onRequestWait(context.TODO(), testCase.req)
		re.NoError(err, caseNum)
		expectedConsumption := &rmpb.Consumption{}
		if testCase.req.IsWrite() {
			kvCalculator.calculateWriteCost(expectedConsumption, testCase.req)
			re.Equal(expectedConsumption.WRU, consumption.WRU)
		}
		consumption, err = gc.onResponse(testCase.req, testCase.resp)
		re.NoError(err, caseNum)
		kvCalculator.calculateReadCost(expectedConsumption, testCase.resp)
		kvCalculator.calculateCPUCost(expectedConsumption, testCase.resp)
		re.Equal(expectedConsumption.RRU, consumption.RRU, caseNum)
		re.Equal(expectedConsumption.TotalCpuTimeMs, consumption.TotalCpuTimeMs, caseNum)
	}
}

func TestResourceGroupThrottledError(t *testing.T) {
	re := require.New(t)
	gc := createTestGroupCostController(re)
	gc.initRunState()
	req := &TestRequestInfo{
		isWrite:    true,
		writeBytes: 10000000,
	}
	// The group is throttled
	_, _, err := gc.onRequestWait(context.TODO(), req)
	re.Error(err)
	re.True(errs.ErrClientResourceGroupThrottled.Equal(err))
}

// MockResourceGroupProvider is a mock implementation of the ResourceGroupProvider interface.
type MockResourceGroupProvider struct {
	mock.Mock
}

func (m *MockResourceGroupProvider) GetResourceGroup(ctx context.Context, resourceGroupName string) (*rmpb.ResourceGroup, error) {
	args := m.Called(ctx, resourceGroupName)
	return args.Get(0).(*rmpb.ResourceGroup), args.Error(1)
}

func (m *MockResourceGroupProvider) ListResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error) {
	args := m.Called(ctx)
	return args.Get(0).([]*rmpb.ResourceGroup), args.Error(1)
}

func (m *MockResourceGroupProvider) AddResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error) {
	args := m.Called(ctx, metaGroup)
	return args.String(0), args.Error(1)
}

func (m *MockResourceGroupProvider) ModifyResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error) {
	args := m.Called(ctx, metaGroup)
	return args.String(0), args.Error(1)
}

func (m *MockResourceGroupProvider) DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error) {
	args := m.Called(ctx, resourceGroupName)
	return args.String(0), args.Error(1)
}

func (m *MockResourceGroupProvider) AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error) {
	args := m.Called(ctx, request)
	return args.Get(0).([]*rmpb.TokenBucketResponse), args.Error(1)
}

func (m *MockResourceGroupProvider) LoadResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, int64, error) {
	args := m.Called(ctx)
	return args.Get(0).([]*rmpb.ResourceGroup), args.Get(1).(int64), args.Error(2)
}

func (m *MockResourceGroupProvider) Watch(ctx context.Context, key []byte, opts ...pd.OpOption) (chan []*meta_storagepb.Event, error) {
	args := m.Called(ctx, key, opts)
	return args.Get(0).(chan []*meta_storagepb.Event), args.Error(1)
}

func (m *MockResourceGroupProvider) Get(ctx context.Context, key []byte, opts ...pd.OpOption) (*meta_storagepb.GetResponse, error) {
	args := m.Called(ctx, key, opts)
	return args.Get(0).(*meta_storagepb.GetResponse), args.Error(1)
}

func TestControllerWithTwoGroupRequestConcurrency(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockProvider := new(MockResourceGroupProvider)

	mockProvider.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta_storagepb.GetResponse{}, nil)
	// LoadResourceGroups
	mockProvider.On("LoadResourceGroups", mock.Anything).Return([]*rmpb.ResourceGroup{}, int64(0), nil)
	// Watch
	mockProvider.On("Watch", mock.Anything, mock.Anything, mock.Anything).Return(make(chan []*meta_storagepb.Event), nil)

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/triggerPeriodicReport", fmt.Sprintf("return(\"%s\")", "default")))
	defer failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/triggerPeriodicReport")
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/triggerLowRUReport", fmt.Sprintf("return(\"%s\")", "test-group")))
	defer failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/triggerLowRUReport")

	controller, _ := NewResourceGroupController(ctx, 1, mockProvider, nil)
	controller.Start(ctx)

	defaultResourceGroup := &rmpb.ResourceGroup{Name: "default", Mode: rmpb.GroupMode_RUMode, RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000}}}}
	testResourceGroup := &rmpb.ResourceGroup{Name: "test-group", Mode: rmpb.GroupMode_RUMode, RUSettings: &rmpb.GroupRequestUnitSettings{RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{FillRate: 1000000}}}}
	mockProvider.On("GetResourceGroup", mock.Anything, "default", mock.Anything).Return(defaultResourceGroup, nil)
	mockProvider.On("GetResourceGroup", mock.Anything, "test-group", mock.Anything).Return(testResourceGroup, nil)

	c1, err := controller.tryGetResourceGroup(ctx, "default")
	re.NoError(err)
	re.Equal(defaultResourceGroup, c1.meta)

	c2, err := controller.tryGetResourceGroup(ctx, "test-group")
	re.NoError(err)
	re.Equal(testResourceGroup, c2.meta)

	var expectResp []*rmpb.TokenBucketResponse
	recTestGroupAcquireTokenRequest := make(chan bool)
	mockProvider.On("AcquireTokenBuckets", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		request := args.Get(1).(*rmpb.TokenBucketsRequest)
		var responses []*rmpb.TokenBucketResponse
		for _, req := range request.Requests {
			if req.ResourceGroupName == "default" {
				// no response the default group request, that's mean `len(c.run.currentRequests) != 0` always.
				time.Sleep(100 * time.Second)
				responses = append(responses, &rmpb.TokenBucketResponse{
					ResourceGroupName: "default",
					GrantedRUTokens: []*rmpb.GrantedRUTokenBucket{
						{
							GrantedTokens: &rmpb.TokenBucket{
								Tokens: 100000,
							},
						},
					},
				})
			} else {
				responses = append(responses, &rmpb.TokenBucketResponse{
					ResourceGroupName: req.ResourceGroupName,
					GrantedRUTokens: []*rmpb.GrantedRUTokenBucket{
						{
							GrantedTokens: &rmpb.TokenBucket{
								Tokens: 100000,
							},
						},
					},
				})
			}
		}
		// receive test-group request
		if len(request.Requests) == 1 && request.Requests[0].ResourceGroupName == "test-group" {
			recTestGroupAcquireTokenRequest <- true
		}
		expectResp = responses
	}).Return(expectResp, nil)
	// wait default group request token by PeriodicReport.
	time.Sleep(2 * time.Second)
	counter := c2.run.requestUnitTokens[0]
	counter.limiter.mu.Lock()
	counter.limiter.notify()
	counter.limiter.mu.Unlock()
	select {
	case res := <-recTestGroupAcquireTokenRequest:
		re.True(res)
	case <-time.After(5 * time.Second):
		re.Fail("timeout")
	}
}
