// Copyright 2022 TiKV Project Authors.
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

package resourcemanager_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/resource_group/controller"
	"github.com/tikv/pd/pkg/mcs/resource_manager/server"
	"github.com/tikv/pd/pkg/utils/testutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/tests"
	"go.uber.org/goleak"

	// Register Service
	_ "github.com/tikv/pd/pkg/mcs/registry"
	_ "github.com/tikv/pd/pkg/mcs/resource_manager/server/install"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, testutil.LeakOptions...)
}

type resourceManagerClientTestSuite struct {
	suite.Suite
	ctx        context.Context
	clean      context.CancelFunc
	cluster    *tests.TestCluster
	client     pd.Client
	initGroups []*rmpb.ResourceGroup
}

func TestResourceManagerClientTestSuite(t *testing.T) {
	suite.Run(t, new(resourceManagerClientTestSuite))
}

func (suite *resourceManagerClientTestSuite) SetupSuite() {
	var err error
	re := suite.Require()

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/resource_manager/server/enableDegradedMode", `return(true)`))

	suite.ctx, suite.clean = context.WithCancel(context.Background())

	suite.cluster, err = tests.NewTestCluster(suite.ctx, 2)
	re.NoError(err)

	err = suite.cluster.RunInitialServers()
	re.NoError(err)

	suite.client, err = pd.NewClientWithContext(suite.ctx, suite.cluster.GetConfig().GetClientURLs(), pd.SecurityOption{})
	re.NoError(err)
	leader := suite.cluster.GetServer(suite.cluster.WaitLeader())
	suite.waitLeader(suite.client, leader.GetAddr())

	suite.initGroups = []*rmpb.ResourceGroup{
		{
			Name: "test1",
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{
						FillRate: 10000,
					},
					Tokens: 100000,
				},
			},
		},
		{
			Name: "test2",
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{
						FillRate:   20000,
						BurstLimit: -1,
					},
					Tokens: 100000,
				},
			},
		},
		{
			Name: "test3",
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{
						FillRate:   100,
						BurstLimit: 5000000,
					},
					Tokens: 5000000,
				},
			},
		},
		{
			Name: "test4",
			Mode: rmpb.GroupMode_RUMode,
			RUSettings: &rmpb.GroupRequestUnitSettings{
				RU: &rmpb.TokenBucket{
					Settings: &rmpb.TokenLimitSettings{
						FillRate:   1000,
						BurstLimit: 5000000,
					},
					Tokens: 5000000,
				},
			},
		},
	}
}

func (suite *resourceManagerClientTestSuite) waitLeader(cli pd.Client, leaderAddr string) {
	innerCli, ok := cli.(interface{ GetServiceDiscovery() pd.ServiceDiscovery })
	suite.True(ok)
	suite.NotNil(innerCli)
	testutil.Eventually(suite.Require(), func() bool {
		innerCli.GetServiceDiscovery().ScheduleCheckMemberChanged()
		return innerCli.GetServiceDiscovery().GetServingAddr() == leaderAddr
	})
}

func (suite *resourceManagerClientTestSuite) TearDownSuite() {
	re := suite.Require()
	suite.client.Close()
	suite.cluster.Destroy()
	suite.clean()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resource_manager/server/enableDegradedMode"))
}

func (suite *resourceManagerClientTestSuite) TearDownTest() {
	suite.cleanupResourceGroups()
}

func (suite *resourceManagerClientTestSuite) cleanupResourceGroups() {
	cli := suite.client
	groups, err := cli.ListResourceGroups(suite.ctx)
	suite.NoError(err)
	for _, group := range groups {
		deleteResp, err := cli.DeleteResourceGroup(suite.ctx, group.GetName())
		if group.Name == "default" {
			suite.Contains(err.Error(), "cannot delete reserved group")
			continue
		}
		suite.NoError(err)
		suite.Contains(deleteResp, "Success!")
	}
}

func (suite *resourceManagerClientTestSuite) resignAndWaitLeader() {
	suite.NoError(suite.cluster.ResignLeader())
	newLeader := suite.cluster.GetServer(suite.cluster.WaitLeader())
	suite.NotNil(newLeader)
	suite.waitLeader(suite.client, newLeader.GetAddr())
}

func (suite *resourceManagerClientTestSuite) TestWatchResourceGroup() {
	re := suite.Require()
	cli := suite.client
	groupNamePrefix := "watch_test"
	group := &rmpb.ResourceGroup{
		Name: groupNamePrefix,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: 10000,
				},
			},
		},
	}
	controller, _ := controller.NewResourceGroupController(suite.ctx, 1, cli, nil)
	controller.Start(suite.ctx)
	defer controller.Stop()

	// Mock add resource groups
	var meta *rmpb.ResourceGroup
	groupsNum := 10
	for i := 0; i < groupsNum; i++ {
		group.Name = groupNamePrefix + strconv.Itoa(i)
		resp, err := cli.AddResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(resp, "Success!")

		// Make sure the resource group active
		meta, err = controller.GetResourceGroup(group.Name)
		re.NotNil(meta)
		re.NoError(err)
		meta = controller.GetActiveResourceGroup(group.Name)
		re.NotNil(meta)
	}
	// Mock modify resource groups
	modifySettings := func(gs *rmpb.ResourceGroup, fillRate uint64) {
		gs.RUSettings = &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: fillRate,
				},
			},
		}
	}
	for i := 0; i < groupsNum; i++ {
		group.Name = groupNamePrefix + strconv.Itoa(i)
		modifySettings(group, 20000)
		resp, err := cli.ModifyResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(resp, "Success!")
	}
	for i := 0; i < groupsNum; i++ {
		testutil.Eventually(re, func() bool {
			name := groupNamePrefix + strconv.Itoa(i)
			meta = controller.GetActiveResourceGroup(name)
			if meta != nil {
				return meta.RUSettings.RU.Settings.FillRate == uint64(20000)
			}
			return false
		}, testutil.WithTickInterval(50*time.Millisecond))
	}

	// Mock reset watch stream
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/watchStreamError", "return(true)"))
	group.Name = groupNamePrefix + strconv.Itoa(groupsNum)
	resp, err := cli.AddResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")
	// Make sure the resource group active
	meta, err = controller.GetResourceGroup(group.Name)
	re.NotNil(meta)
	re.NoError(err)
	modifySettings(group, 30000)
	resp, err = cli.ModifyResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")
	testutil.Eventually(re, func() bool {
		meta = controller.GetActiveResourceGroup(group.Name)
		return meta.RUSettings.RU.Settings.FillRate == uint64(30000)
	}, testutil.WithTickInterval(100*time.Millisecond))
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/watchStreamError"))

	// Mock delete resource groups
	suite.cleanupResourceGroups()
	for i := 0; i < groupsNum; i++ {
		testutil.Eventually(re, func() bool {
			name := groupNamePrefix + strconv.Itoa(i)
			meta = controller.GetActiveResourceGroup(name)
			return meta == nil
		}, testutil.WithTickInterval(50*time.Millisecond))
	}
}

func (suite *resourceManagerClientTestSuite) TestWatchWithSingleGroupByKeyspace() {
	re := suite.Require()
	cli := suite.client

	// We need to disable watch stream for `isSingleGroupByKeyspace`.
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/disableWatch", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/disableWatch"))
	}()
	// Distinguish the controller with and without enabling `isSingleGroupByKeyspace`.
	controllerKeySpace, _ := controller.NewResourceGroupController(suite.ctx, 1, cli, nil, controller.EnableSingleGroupByKeyspace())
	controller, _ := controller.NewResourceGroupController(suite.ctx, 2, cli, nil)
	controller.Start(suite.ctx)
	controllerKeySpace.Start(suite.ctx)
	defer controllerKeySpace.Stop()
	defer controller.Stop()

	// Mock add resource group.
	group := &rmpb.ResourceGroup{
		Name: "keyspace_test",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: 10000,
				},
				Tokens: 100000,
			},
		},
	}
	resp, err := cli.AddResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")

	tcs := tokenConsumptionPerSecond{rruTokensAtATime: 100}
	controller.OnRequestWait(suite.ctx, group.Name, tcs.makeReadRequest())
	meta := controller.GetActiveResourceGroup(group.Name)
	re.Equal(meta.RUSettings.RU, group.RUSettings.RU)

	controllerKeySpace.OnRequestWait(suite.ctx, group.Name, tcs.makeReadRequest())
	metaKeySpace := controllerKeySpace.GetActiveResourceGroup(group.Name)
	re.Equal(metaKeySpace.RUSettings.RU, group.RUSettings.RU)

	// Mock modify resource groups
	modifySettings := func(gs *rmpb.ResourceGroup, fillRate uint64) {
		gs.RUSettings = &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: fillRate,
				},
			},
		}
	}
	modifySettings(group, 20000)
	resp, err = cli.ModifyResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")

	testutil.Eventually(re, func() bool {
		meta = controller.GetActiveResourceGroup(group.Name)
		return meta.RUSettings.RU.Settings.FillRate == uint64(20000)
	}, testutil.WithTickInterval(100*time.Millisecond))
	metaKeySpace = controllerKeySpace.GetActiveResourceGroup(group.Name)
	re.Equal(metaKeySpace.RUSettings.RU.Settings.FillRate, uint64(10000))
}

const buffDuration = time.Millisecond * 300

type tokenConsumptionPerSecond struct {
	rruTokensAtATime float64
	wruTokensAtATime float64
	times            int
	waitDuration     time.Duration
}

func (t tokenConsumptionPerSecond) makeReadRequest() *controller.TestRequestInfo {
	return controller.NewTestRequestInfo(false, 0, 0)
}

func (t tokenConsumptionPerSecond) makeWriteRequest() *controller.TestRequestInfo {
	return controller.NewTestRequestInfo(true, uint64(t.wruTokensAtATime-1), 0)
}

func (t tokenConsumptionPerSecond) makeReadResponse() *controller.TestResponseInfo {
	return controller.NewTestResponseInfo(
		uint64((t.rruTokensAtATime-1)/2),
		time.Duration(t.rruTokensAtATime/2)*time.Millisecond,
		false,
	)
}

func (t tokenConsumptionPerSecond) makeWriteResponse() *controller.TestResponseInfo {
	return controller.NewTestResponseInfo(
		0,
		time.Duration(0),
		true,
	)
}

func (suite *resourceManagerClientTestSuite) TestResourceGroupController() {
	re := suite.Require()
	cli := suite.client

	rg := &rmpb.ResourceGroup{
		Name: "controller_test",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: 10000,
				},
				Tokens: 100000,
			},
		},
	}
	resp, err := cli.AddResourceGroup(suite.ctx, rg)
	re.NoError(err)
	re.Contains(resp, "Success!")

	cfg := &controller.RequestUnitConfig{
		ReadBaseCost:     1,
		ReadCostPerByte:  1,
		WriteBaseCost:    1,
		WriteCostPerByte: 1,
		CPUMsCost:        1,
	}

	controller, _ := controller.NewResourceGroupController(suite.ctx, 1, cli, cfg, controller.EnableSingleGroupByKeyspace())
	controller.Start(suite.ctx)
	defer controller.Stop()

	testCases := []struct {
		resourceGroupName string
		tcs               []tokenConsumptionPerSecond
		len               int
	}{
		{
			resourceGroupName: rg.Name,
			len:               8,
			tcs: []tokenConsumptionPerSecond{
				{rruTokensAtATime: 50, wruTokensAtATime: 20, times: 100, waitDuration: 0},
				{rruTokensAtATime: 50, wruTokensAtATime: 100, times: 100, waitDuration: 0},
				{rruTokensAtATime: 50, wruTokensAtATime: 100, times: 100, waitDuration: 0},
				{rruTokensAtATime: 20, wruTokensAtATime: 40, times: 250, waitDuration: 0},
				{rruTokensAtATime: 25, wruTokensAtATime: 50, times: 200, waitDuration: 0},
				{rruTokensAtATime: 30, wruTokensAtATime: 60, times: 165, waitDuration: 0},
				{rruTokensAtATime: 40, wruTokensAtATime: 80, times: 125, waitDuration: 0},
				{rruTokensAtATime: 50, wruTokensAtATime: 100, times: 100, waitDuration: 0},
			},
		},
	}
	tricker := time.NewTicker(time.Second)
	defer tricker.Stop()
	i := 0
	for {
		v := false
		<-tricker.C
		for _, cas := range testCases {
			if i >= cas.len {
				continue
			}
			v = true
			sum := time.Duration(0)
			for j := 0; j < cas.tcs[i].times; j++ {
				rreq := cas.tcs[i].makeReadRequest()
				wreq := cas.tcs[i].makeWriteRequest()
				rres := cas.tcs[i].makeReadResponse()
				wres := cas.tcs[i].makeWriteResponse()
				startTime := time.Now()
				_, _, err := controller.OnRequestWait(suite.ctx, cas.resourceGroupName, rreq)
				re.NoError(err)
				_, _, err = controller.OnRequestWait(suite.ctx, cas.resourceGroupName, wreq)
				re.NoError(err)
				sum += time.Since(startTime)
				controller.OnResponse(cas.resourceGroupName, rreq, rres)
				controller.OnResponse(cas.resourceGroupName, wreq, wres)
				time.Sleep(1000 * time.Microsecond)
			}
			re.LessOrEqual(sum, buffDuration+cas.tcs[i].waitDuration)
		}
		i++
		if !v {
			break
		}
	}
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/triggerUpdate", "return(true)"))
	tcs := tokenConsumptionPerSecond{rruTokensAtATime: 1, wruTokensAtATime: 900000000, times: 1, waitDuration: 0}
	wreq := tcs.makeWriteRequest()
	_, _, err = controller.OnRequestWait(suite.ctx, rg.Name, wreq)
	re.Error(err)
	time.Sleep(time.Millisecond * 200)
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/triggerUpdate"))
}

// TestSwitchBurst is used to test https://github.com/tikv/pd/issues/6209
func (suite *resourceManagerClientTestSuite) TestSwitchBurst() {
	re := suite.Require()
	cli := suite.client
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/acceleratedReportingPeriod", "return(true)"))

	for _, group := range suite.initGroups {
		resp, err := cli.AddResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(resp, "Success!")
	}

	cfg := &controller.RequestUnitConfig{
		ReadBaseCost:     1,
		ReadCostPerByte:  1,
		WriteBaseCost:    1,
		WriteCostPerByte: 1,
		CPUMsCost:        1,
	}
	testCases := []struct {
		resourceGroupName string
		tcs               []tokenConsumptionPerSecond
		len               int
	}{
		{
			resourceGroupName: suite.initGroups[0].Name,
			len:               8,
			tcs: []tokenConsumptionPerSecond{
				{rruTokensAtATime: 50, wruTokensAtATime: 20, times: 100, waitDuration: 0},
				{rruTokensAtATime: 50, wruTokensAtATime: 100, times: 100, waitDuration: 0},
				{rruTokensAtATime: 50, wruTokensAtATime: 100, times: 100, waitDuration: 0},
				{rruTokensAtATime: 20, wruTokensAtATime: 40, times: 250, waitDuration: 0},
				{rruTokensAtATime: 25, wruTokensAtATime: 50, times: 200, waitDuration: 0},
				{rruTokensAtATime: 30, wruTokensAtATime: 60, times: 165, waitDuration: 0},
				{rruTokensAtATime: 40, wruTokensAtATime: 80, times: 125, waitDuration: 0},
				{rruTokensAtATime: 50, wruTokensAtATime: 100, times: 100, waitDuration: 0},
			},
		},
	}
	controller, _ := controller.NewResourceGroupController(suite.ctx, 1, cli, cfg, controller.EnableSingleGroupByKeyspace())
	controller.Start(suite.ctx)
	defer controller.Stop()
	resourceGroupName := suite.initGroups[1].Name
	tcs := tokenConsumptionPerSecond{rruTokensAtATime: 1, wruTokensAtATime: 2, times: 100, waitDuration: 0}
	for j := 0; j < tcs.times; j++ {
		rreq := tcs.makeReadRequest()
		wreq := tcs.makeWriteRequest()
		rres := tcs.makeReadResponse()
		wres := tcs.makeWriteResponse()
		_, _, err := controller.OnRequestWait(suite.ctx, resourceGroupName, rreq)
		re.NoError(err)
		_, _, err = controller.OnRequestWait(suite.ctx, resourceGroupName, wreq)
		re.NoError(err)
		controller.OnResponse(resourceGroupName, rreq, rres)
		controller.OnResponse(resourceGroupName, wreq, wres)
	}
	time.Sleep(2 * time.Second)
	cli.ModifyResourceGroup(suite.ctx, &rmpb.ResourceGroup{
		Name: "test2",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   20000,
					BurstLimit: 20000,
				},
			},
		},
	})
	time.Sleep(100 * time.Millisecond)
	tricker := time.NewTicker(time.Second)
	defer tricker.Stop()
	i := 0
	for {
		v := false
		<-tricker.C
		for _, cas := range testCases {
			if i >= cas.len {
				continue
			}
			v = true
			sum := time.Duration(0)
			for j := 0; j < cas.tcs[i].times; j++ {
				rreq := cas.tcs[i].makeReadRequest()
				wreq := cas.tcs[i].makeWriteRequest()
				rres := cas.tcs[i].makeReadResponse()
				wres := cas.tcs[i].makeWriteResponse()
				startTime := time.Now()
				_, _, err := controller.OnRequestWait(suite.ctx, resourceGroupName, rreq)
				re.NoError(err)
				_, _, err = controller.OnRequestWait(suite.ctx, resourceGroupName, wreq)
				re.NoError(err)
				sum += time.Since(startTime)
				controller.OnResponse(resourceGroupName, rreq, rres)
				controller.OnResponse(resourceGroupName, wreq, wres)
				time.Sleep(1000 * time.Microsecond)
			}
			re.LessOrEqual(sum, buffDuration+cas.tcs[i].waitDuration)
		}
		i++
		if !v {
			break
		}
	}

	resourceGroupName2 := suite.initGroups[2].Name
	tcs = tokenConsumptionPerSecond{rruTokensAtATime: 1, wruTokensAtATime: 100000, times: 1, waitDuration: 0}
	wreq := tcs.makeWriteRequest()
	_, _, err := controller.OnRequestWait(suite.ctx, resourceGroupName2, wreq)
	re.NoError(err)

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/acceleratedSpeedTrend", "return(true)"))
	resourceGroupName3 := suite.initGroups[3].Name
	tcs = tokenConsumptionPerSecond{rruTokensAtATime: 1, wruTokensAtATime: 1000, times: 1, waitDuration: 0}
	wreq = tcs.makeWriteRequest()
	_, _, err = controller.OnRequestWait(suite.ctx, resourceGroupName3, wreq)
	re.NoError(err)
	time.Sleep(110 * time.Millisecond)
	tcs = tokenConsumptionPerSecond{rruTokensAtATime: 1, wruTokensAtATime: 10, times: 1010, waitDuration: 0}
	duration := time.Duration(0)
	for i := 0; i < tcs.times; i++ {
		wreq = tcs.makeWriteRequest()
		startTime := time.Now()
		_, _, err = controller.OnRequestWait(suite.ctx, resourceGroupName3, wreq)
		duration += time.Since(startTime)
		re.NoError(err)
	}
	re.Less(duration, 100*time.Millisecond)
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/acceleratedReportingPeriod"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/acceleratedSpeedTrend"))
}

func (suite *resourceManagerClientTestSuite) TestResourcePenalty() {
	re := suite.Require()
	cli := suite.client

	groupNames := []string{"penalty_test1", "penalty_test2"}
	// Mock add 2 resource groups.
	group := &rmpb.ResourceGroup{
		Name: groupNames[0],
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate: 10000,
				},
				Tokens: 100000,
			},
		},
	}
	for _, name := range groupNames {
		group.Name = name
		resp, err := cli.AddResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(resp, "Success!")
	}

	cfg := &controller.RequestUnitConfig{
		ReadBaseCost:     1,
		ReadCostPerByte:  1,
		WriteBaseCost:    1,
		WriteCostPerByte: 1,
		CPUMsCost:        1,
	}
	c, _ := controller.NewResourceGroupController(suite.ctx, 1, cli, cfg, controller.EnableSingleGroupByKeyspace())
	c.Start(suite.ctx)
	defer c.Stop()

	resourceGroupName := groupNames[0]
	// init
	req := controller.NewTestRequestInfo(false, 0, 2 /* store2 */)
	resp := controller.NewTestResponseInfo(0, time.Duration(30), true)
	_, penalty, err := c.OnRequestWait(suite.ctx, resourceGroupName, req)
	re.NoError(err)
	re.Equal(penalty.WriteBytes, float64(0))
	re.Equal(penalty.TotalCpuTimeMs, 0.0)
	_, err = c.OnResponse(resourceGroupName, req, resp)
	re.NoError(err)

	req = controller.NewTestRequestInfo(true, 60, 1 /* store1 */)
	resp = controller.NewTestResponseInfo(0, time.Duration(10), true)
	_, penalty, err = c.OnRequestWait(suite.ctx, resourceGroupName, req)
	re.NoError(err)
	re.Equal(penalty.WriteBytes, float64(0))
	re.Equal(penalty.TotalCpuTimeMs, 0.0)
	_, err = c.OnResponse(resourceGroupName, req, resp)
	re.NoError(err)

	// failed request, shouldn't be counted in penalty
	req = controller.NewTestRequestInfo(true, 20, 1 /* store1 */)
	resp = controller.NewTestResponseInfo(0, time.Duration(0), false)
	_, penalty, err = c.OnRequestWait(suite.ctx, resourceGroupName, req)
	re.NoError(err)
	re.Equal(penalty.WriteBytes, float64(0))
	re.Equal(penalty.TotalCpuTimeMs, 0.0)
	_, err = c.OnResponse(resourceGroupName, req, resp)
	re.NoError(err)

	// from same store, should be zero
	req1 := controller.NewTestRequestInfo(false, 0, 1 /* store1 */)
	resp1 := controller.NewTestResponseInfo(0, time.Duration(10), true)
	_, penalty, err = c.OnRequestWait(suite.ctx, resourceGroupName, req1)
	re.NoError(err)
	re.Equal(penalty.WriteBytes, float64(0))
	_, err = c.OnResponse(resourceGroupName, req1, resp1)
	re.NoError(err)

	// from different store, should be non-zero
	req2 := controller.NewTestRequestInfo(true, 50, 2 /* store2 */)
	resp2 := controller.NewTestResponseInfo(0, time.Duration(10), true)
	_, penalty, err = c.OnRequestWait(suite.ctx, resourceGroupName, req2)
	re.NoError(err)
	re.Equal(penalty.WriteBytes, float64(60))
	re.InEpsilon(penalty.TotalCpuTimeMs, 10.0/1000.0/1000.0, 1e-6)
	_, err = c.OnResponse(resourceGroupName, req2, resp2)
	re.NoError(err)

	// from new store, should be zero
	req3 := controller.NewTestRequestInfo(true, 0, 3 /* store3 */)
	resp3 := controller.NewTestResponseInfo(0, time.Duration(10), true)
	_, penalty, err = c.OnRequestWait(suite.ctx, resourceGroupName, req3)
	re.NoError(err)
	re.Equal(penalty.WriteBytes, float64(0))
	_, err = c.OnResponse(resourceGroupName, req3, resp3)
	re.NoError(err)

	// from different group, should be zero
	resourceGroupName = groupNames[1]
	req4 := controller.NewTestRequestInfo(true, 50, 1 /* store2 */)
	resp4 := controller.NewTestResponseInfo(0, time.Duration(10), true)
	_, penalty, err = c.OnRequestWait(suite.ctx, resourceGroupName, req4)
	re.NoError(err)
	re.Equal(penalty.WriteBytes, float64(0))
	_, err = c.OnResponse(resourceGroupName, req4, resp4)
	re.NoError(err)
}

func (suite *resourceManagerClientTestSuite) TestAcquireTokenBucket() {
	re := suite.Require()
	cli := suite.client

	groups := suite.initGroups
	for _, group := range groups {
		resp, err := cli.AddResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(resp, "Success!")
	}
	reqs := &rmpb.TokenBucketsRequest{
		Requests:              make([]*rmpb.TokenBucketRequest, 0),
		TargetRequestPeriodMs: uint64(time.Second * 10 / time.Millisecond),
	}
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/resource_manager/server/fastPersist", `return(true)`))
	suite.resignAndWaitLeader()
	for i := 0; i < 3; i++ {
		for _, group := range groups {
			requests := make([]*rmpb.RequestUnitItem, 0)
			requests = append(requests, &rmpb.RequestUnitItem{
				Type:  rmpb.RequestUnitType_RU,
				Value: 30000,
			})
			req := &rmpb.TokenBucketRequest{
				ResourceGroupName: group.Name,
				Request: &rmpb.TokenBucketRequest_RuItems{
					RuItems: &rmpb.TokenBucketRequest_RequestRU{
						RequestRU: requests,
					},
				},
			}
			reqs.Requests = append(reqs.Requests, req)
		}
		aresp, err := cli.AcquireTokenBuckets(suite.ctx, reqs)
		re.NoError(err)
		for _, resp := range aresp {
			re.Len(resp.GrantedRUTokens, 1)
			re.Equal(resp.GrantedRUTokens[0].GrantedTokens.Tokens, float64(30000.))
			if resp.ResourceGroupName == "test2" {
				re.Equal(int64(-1), resp.GrantedRUTokens[0].GrantedTokens.GetSettings().GetBurstLimit())
			}
		}
		gresp, err := cli.GetResourceGroup(suite.ctx, groups[0].GetName())
		re.NoError(err)
		re.Less(gresp.RUSettings.RU.Tokens, groups[0].RUSettings.RU.Tokens)

		checkFunc := func(g1 *rmpb.ResourceGroup, g2 *rmpb.ResourceGroup) {
			re.Equal(g1.GetName(), g2.GetName())
			re.Equal(g1.GetMode(), g2.GetMode())
			re.Equal(g1.GetRUSettings().RU.Settings.FillRate, g2.GetRUSettings().RU.Settings.FillRate)
			// now we don't persistent tokens in running state, so tokens is original.
			re.Less(g1.GetRUSettings().RU.Tokens, g2.GetRUSettings().RU.Tokens)
			re.NoError(err)
		}
		time.Sleep(250 * time.Millisecond)
		// to test persistent
		suite.resignAndWaitLeader()
		gresp, err = cli.GetResourceGroup(suite.ctx, groups[0].GetName())
		re.NoError(err)
		checkFunc(gresp, groups[0])
	}
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resource_manager/server/fastPersist"))
}

func (suite *resourceManagerClientTestSuite) TestBasicResourceGroupCURD() {
	re := suite.Require()
	cli := suite.client
	testCasesSet1 := []struct {
		name           string
		mode           rmpb.GroupMode
		isNewGroup     bool
		modifySuccess  bool
		expectMarshal  string
		modifySettings func(*rmpb.ResourceGroup)
	}{
		{"CRUD_test1", rmpb.GroupMode_RUMode, true, true,
			`{"name":"CRUD_test1","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":10000},"state":{"initialized":false}}},"priority":0}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RUSettings = &rmpb.GroupRequestUnitSettings{
					RU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate: 10000,
						},
					},
				}
			},
		},

		{"CRUD_test2", rmpb.GroupMode_RUMode, true, true,
			`{"name":"CRUD_test2","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":20000},"state":{"initialized":false}}},"priority":0}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RUSettings = &rmpb.GroupRequestUnitSettings{
					RU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate: 20000,
						},
					},
				}
			},
		},
		{"CRUD_test2", rmpb.GroupMode_RUMode, false, true,
			`{"name":"CRUD_test2","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":30000,"burst_limit":-1},"state":{"initialized":false}}},"priority":0}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RUSettings = &rmpb.GroupRequestUnitSettings{
					RU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate:   30000,
							BurstLimit: -1,
						},
					},
				}
			},
		},
		{"default", rmpb.GroupMode_RUMode, false, true,
			`{"name":"default","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":10000,"burst_limit":-1},"state":{"initialized":false}}},"priority":0}`,
			func(gs *rmpb.ResourceGroup) {
				gs.RUSettings = &rmpb.GroupRequestUnitSettings{
					RU: &rmpb.TokenBucket{
						Settings: &rmpb.TokenLimitSettings{
							FillRate:   10000,
							BurstLimit: -1,
						},
					},
				}
			},
		},
	}

	checkErr := func(err error, success bool) {
		if success {
			re.NoError(err)
		} else {
			re.Error(err)
		}
	}

	finalNum := 1
	// Test Resource Group CURD via gRPC
	for i, tcase := range testCasesSet1 {
		group := &rmpb.ResourceGroup{
			Name: tcase.name,
			Mode: tcase.mode,
		}
		// Create Resource Group
		resp, err := cli.AddResourceGroup(suite.ctx, group)
		checkErr(err, true)
		if tcase.isNewGroup {
			finalNum++
			re.Contains(resp, "Success!")
		}

		// Modify Resource Group
		tcase.modifySettings(group)
		mresp, err := cli.ModifyResourceGroup(suite.ctx, group)
		checkErr(err, tcase.modifySuccess)
		if tcase.modifySuccess {
			re.Contains(mresp, "Success!")
		}

		// Get Resource Group
		gresp, err := cli.GetResourceGroup(suite.ctx, tcase.name)
		re.NoError(err)
		re.Equal(tcase.name, gresp.Name)
		if tcase.modifySuccess {
			re.Equal(group, gresp)
		}

		// Last one, Check list and delete all resource groups
		if i == len(testCasesSet1)-1 {
			// List Resource Groups
			lresp, err := cli.ListResourceGroups(suite.ctx)
			re.NoError(err)
			re.Equal(finalNum, len(lresp))

			for _, g := range lresp {
				// Delete Resource Group
				dresp, err := cli.DeleteResourceGroup(suite.ctx, g.Name)
				if g.Name == "default" {
					re.Contains(err.Error(), "cannot delete reserved group")
					continue
				}
				re.NoError(err)
				re.Contains(dresp, "Success!")
				_, err = cli.GetResourceGroup(suite.ctx, g.Name)
				re.EqualError(err, fmt.Sprintf("get resource group %v failed, rpc error: code = Unknown desc = resource group not found", g.Name))
			}

			// to test the deletion of persistence
			suite.resignAndWaitLeader()
			// List Resource Group
			lresp, err = cli.ListResourceGroups(suite.ctx)
			re.NoError(err)
			re.Equal(1, len(lresp))
		}
	}

	// Test Resource Group CURD via HTTP
	finalNum = 1
	getAddr := func(i int) string {
		server := suite.cluster.GetServer(suite.cluster.GetLeader())
		if i%2 == 1 {
			server = suite.cluster.GetServer(suite.cluster.GetFollower())
		}
		return server.GetAddr()
	}
	for i, tcase := range testCasesSet1 {
		// Create Resource Group
		group := &rmpb.ResourceGroup{
			Name: tcase.name,
			Mode: tcase.mode,
		}
		createJSON, err := json.Marshal(group)
		re.NoError(err)
		resp, err := http.Post(getAddr(i)+"/resource-manager/api/v1/config/group", "application/json", strings.NewReader(string(createJSON)))
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		if tcase.isNewGroup {
			finalNum++
		}

		// Modify Resource Group
		tcase.modifySettings(group)
		modifyJSON, err := json.Marshal(group)
		re.NoError(err)
		req, err := http.NewRequest(http.MethodPut, getAddr(i+1)+"/resource-manager/api/v1/config/group", strings.NewReader(string(modifyJSON)))
		re.NoError(err)
		req.Header.Set("Content-Type", "application/json")
		resp, err = http.DefaultClient.Do(req)
		re.NoError(err)
		defer resp.Body.Close()
		if tcase.modifySuccess {
			re.Equal(http.StatusOK, resp.StatusCode)
		} else {
			re.Equal(http.StatusInternalServerError, resp.StatusCode)
		}

		// Get Resource Group
		resp, err = http.Get(getAddr(i) + "/resource-manager/api/v1/config/group/" + tcase.name)
		re.NoError(err)
		defer resp.Body.Close()
		re.Equal(http.StatusOK, resp.StatusCode)
		respString, err := io.ReadAll(resp.Body)
		re.NoError(err)
		re.Contains(string(respString), tcase.name)
		if tcase.modifySuccess {
			re.Equal(string(respString), tcase.expectMarshal)
		}

		// Last one, Check list and delete all resource groups
		if i == len(testCasesSet1)-1 {
			resp, err := http.Get(getAddr(i) + "/resource-manager/api/v1/config/groups")
			re.NoError(err)
			defer resp.Body.Close()
			re.Equal(http.StatusOK, resp.StatusCode)
			respString, err := io.ReadAll(resp.Body)
			re.NoError(err)
			groups := make([]*server.ResourceGroup, 0)
			json.Unmarshal(respString, &groups)
			re.Equal(finalNum, len(groups))

			// Delete all resource groups
			for _, g := range groups {
				req, err := http.NewRequest(http.MethodDelete, getAddr(i+1)+"/resource-manager/api/v1/config/group/"+g.Name, nil)
				re.NoError(err)
				resp, err := http.DefaultClient.Do(req)
				re.NoError(err)
				defer resp.Body.Close()
				respString, err := io.ReadAll(resp.Body)
				re.NoError(err)
				if g.Name == "default" {
					re.Contains(string(respString), "cannot delete reserved group")
					continue
				}
				re.Equal(http.StatusOK, resp.StatusCode)
				re.Contains(string(respString), "Success!")
			}

			// verify again
			resp1, err := http.Get(getAddr(i) + "/resource-manager/api/v1/config/groups")
			re.NoError(err)
			defer resp1.Body.Close()
			re.Equal(http.StatusOK, resp1.StatusCode)
			respString1, err := io.ReadAll(resp1.Body)
			re.NoError(err)
			groups1 := make([]server.ResourceGroup, 0)
			json.Unmarshal(respString1, &groups1)
			re.Equal(1, len(groups1))
		}
	}

	// test restart cluster
	groups, err := cli.ListResourceGroups(suite.ctx)
	re.NoError(err)
	servers := suite.cluster.GetServers()
	re.NoError(suite.cluster.StopAll())
	serverList := make([]*tests.TestServer, 0, len(servers))
	for _, s := range servers {
		serverList = append(serverList, s)
	}
	re.NoError(suite.cluster.RunServers(serverList))
	suite.cluster.WaitLeader()
	var newGroups []*rmpb.ResourceGroup
	testutil.Eventually(suite.Require(), func() bool {
		var err error
		newGroups, err = cli.ListResourceGroups(suite.ctx)
		return err == nil
	}, testutil.WithWaitFor(time.Second))
	re.Equal(groups, newGroups)
}

func (suite *resourceManagerClientTestSuite) TestResourceManagerClientFailover() {
	re := suite.Require()
	cli := suite.client

	group := &rmpb.ResourceGroup{
		Name: "failover_test",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{}},
		},
	}
	addResp, err := cli.AddResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(addResp, "Success!")
	getResp, err := cli.GetResourceGroup(suite.ctx, group.GetName())
	re.NoError(err)
	re.NotNil(getResp)
	re.Equal(*group, *getResp)

	// Change the leader after each time we modify the resource group.
	for i := 0; i < 4; i++ {
		group.RUSettings.RU.Settings.FillRate += uint64(i)
		modifyResp, err := cli.ModifyResourceGroup(suite.ctx, group)
		re.NoError(err)
		re.Contains(modifyResp, "Success!")
		suite.resignAndWaitLeader()
		getResp, err = cli.GetResourceGroup(suite.ctx, group.GetName())
		re.NoError(err)
		re.NotNil(getResp)
		re.Equal(group.RUSettings.RU.Settings.FillRate, getResp.RUSettings.RU.Settings.FillRate)
	}
}

func (suite *resourceManagerClientTestSuite) TestResourceManagerClientDegradedMode() {
	re := suite.Require()
	cli := suite.client

	groupName := "mode_test"
	group := &rmpb.ResourceGroup{
		Name: groupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   10,
					BurstLimit: 10,
				},
				Tokens: 10,
			},
		},
	}
	addResp, err := cli.AddResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(addResp, "Success!")

	cfg := &controller.RequestUnitConfig{
		ReadBaseCost:     1,
		ReadCostPerByte:  1,
		WriteBaseCost:    1,
		WriteCostPerByte: 1,
		CPUMsCost:        1,
	}
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/resource_manager/server/acquireFailed", `return(true)`))
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/degradedModeRU", "return(true)"))
	controller, _ := controller.NewResourceGroupController(suite.ctx, 1, cli, cfg)
	controller.Start(suite.ctx)
	defer controller.Stop()
	tc := tokenConsumptionPerSecond{
		rruTokensAtATime: 0,
		wruTokensAtATime: 10000,
	}
	tc2 := tokenConsumptionPerSecond{
		rruTokensAtATime: 0,
		wruTokensAtATime: 2,
	}
	controller.OnRequestWait(suite.ctx, groupName, tc.makeWriteRequest())
	time.Sleep(time.Second * 2)
	beginTime := time.Now()
	// This is used to make sure resource group in lowRU.
	for i := 0; i < 100; i++ {
		controller.OnRequestWait(suite.ctx, groupName, tc2.makeWriteRequest())
	}
	for i := 0; i < 100; i++ {
		controller.OnRequestWait(suite.ctx, groupName, tc.makeWriteRequest())
	}
	endTime := time.Now()
	// we can not check `inDegradedMode` because of data race.
	re.True(endTime.Before(beginTime.Add(time.Second)))
	controller.Stop()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/resource_manager/server/acquireFailed"))
	re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/degradedModeRU"))
}

func (suite *resourceManagerClientTestSuite) TestLoadRequestUnitConfig() {
	re := suite.Require()
	cli := suite.client
	// Test load from resource manager.
	ctr, err := controller.NewResourceGroupController(suite.ctx, 1, cli, nil)
	re.NoError(err)
	config := ctr.GetConfig()
	re.NotNil(config)
	expectedConfig := controller.DefaultRUConfig()
	re.Equal(expectedConfig.ReadBaseCost, config.ReadBaseCost)
	re.Equal(expectedConfig.ReadBytesCost, config.ReadBytesCost)
	re.Equal(expectedConfig.WriteBaseCost, config.WriteBaseCost)
	re.Equal(expectedConfig.WriteBytesCost, config.WriteBytesCost)
	re.Equal(expectedConfig.CPUMsCost, config.CPUMsCost)
	// Test init from given config.
	ruConfig := &controller.RequestUnitConfig{
		ReadBaseCost:     1,
		ReadCostPerByte:  2,
		WriteBaseCost:    3,
		WriteCostPerByte: 4,
		CPUMsCost:        5,
	}
	ctr, err = controller.NewResourceGroupController(suite.ctx, 1, cli, ruConfig)
	re.NoError(err)
	config = ctr.GetConfig()
	re.NotNil(config)
	controllerConfig := controller.DefaultConfig()
	controllerConfig.RequestUnit = *ruConfig
	expectedConfig = controller.GenerateRUConfig(controllerConfig)
	re.Equal(expectedConfig.ReadBaseCost, config.ReadBaseCost)
	re.Equal(expectedConfig.ReadBytesCost, config.ReadBytesCost)
	re.Equal(expectedConfig.WriteBaseCost, config.WriteBaseCost)
	re.Equal(expectedConfig.WriteBytesCost, config.WriteBytesCost)
	re.Equal(expectedConfig.CPUMsCost, config.CPUMsCost)
	// refer github.com/tikv/pd/pkg/mcs/resource_manager/server/enableDegradedMode, check with 1s.
	re.Equal(time.Second, config.DegradedModeWaitDuration)
}

func (suite *resourceManagerClientTestSuite) TestRemoveStaleResourceGroup() {
	re := suite.Require()
	cli := suite.client

	// Mock add resource group.
	group := &rmpb.ResourceGroup{
		Name: "stale_test",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{}},
		},
	}
	resp, err := cli.AddResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")

	re.NoError(failpoint.Enable("github.com/tikv/pd/client/resource_group/controller/fastCleanup", `return(true)`))
	controller, _ := controller.NewResourceGroupController(suite.ctx, 1, cli, nil)
	controller.Start(suite.ctx)
	defer controller.Stop()

	testConfig := struct {
		tcs   tokenConsumptionPerSecond
		times int
	}{
		tcs: tokenConsumptionPerSecond{
			rruTokensAtATime: 100,
		},
		times: 100,
	}
	// Mock client binds one resource group and then closed
	rreq := testConfig.tcs.makeReadRequest()
	rres := testConfig.tcs.makeReadResponse()
	for j := 0; j < testConfig.times; j++ {
		controller.OnRequestWait(suite.ctx, group.Name, rreq)
		controller.OnResponse(group.Name, rreq, rres)
		time.Sleep(100 * time.Microsecond)
	}
	time.Sleep(1 * time.Second)

	re.Nil(controller.GetActiveResourceGroup(group.Name))

	re.NoError(failpoint.Disable("github.com/tikv/pd/client/resource_group/controller/fastCleanup"))
}

func (suite *resourceManagerClientTestSuite) TestResourceGroupControllerConfigChanged() {
	re := suite.Require()
	cli := suite.client
	// Mock add resource group.
	group := &rmpb.ResourceGroup{
		Name: "config_change_test",
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{Settings: &rmpb.TokenLimitSettings{}},
		},
	}
	resp, err := cli.AddResourceGroup(suite.ctx, group)
	re.NoError(err)
	re.Contains(resp, "Success!")

	c1, err := controller.NewResourceGroupController(suite.ctx, 1, cli, nil)
	re.NoError(err)
	c1.Start(suite.ctx)
	// with client option
	c2, err := controller.NewResourceGroupController(suite.ctx, 2, cli, nil, controller.WithMaxWaitDuration(time.Hour))
	re.NoError(err)
	c2.Start(suite.ctx)
	// helper function for sending HTTP requests and checking responses
	sendRequest := func(method, url string, body io.Reader) []byte {
		req, err := http.NewRequest(method, url, body)
		re.NoError(err)
		resp, err := http.DefaultClient.Do(req)
		re.NoError(err)
		defer resp.Body.Close()
		bytes, err := io.ReadAll(resp.Body)
		re.NoError(err)
		if resp.StatusCode != http.StatusOK {
			re.Fail(string(bytes))
		}
		return bytes
	}

	getAddr := func() string {
		server := suite.cluster.GetServer(suite.cluster.GetLeader())
		if rand.Intn(100)%2 == 1 {
			server = suite.cluster.GetServer(suite.cluster.GetFollower())
		}
		return server.GetAddr()
	}

	configURL := "/resource-manager/api/v1/config/controller"
	waitDuration := 10 * time.Second
	tokenRPCMaxDelay := 2 * time.Second
	readBaseCost := 1.5
	defaultCfg := controller.DefaultConfig()
	expectCfg := server.ControllerConfig{
		// failpoint enableDegradedMode will setup and set it be 1s.
		DegradedModeWaitDuration: typeutil.NewDuration(time.Second),
		LTBMaxWaitDuration:       typeutil.Duration(defaultCfg.LTBMaxWaitDuration),
		LTBTokenRPCMaxDelay:      typeutil.Duration(defaultCfg.LTBTokenRPCMaxDelay),
		RequestUnit:              server.RequestUnitConfig(defaultCfg.RequestUnit),
		EnableControllerTraceLog: defaultCfg.EnableControllerTraceLog,
	}
	expectRUCfg := controller.GenerateRUConfig(defaultCfg)
	expectRUCfg.DegradedModeWaitDuration = time.Second
	// initial config verification
	respString := sendRequest("GET", getAddr()+configURL, nil)
	expectStr, err := json.Marshal(expectCfg)
	re.NoError(err)
	re.JSONEq(string(respString), string(expectStr))
	re.EqualValues(expectRUCfg, c1.GetConfig())

	testCases := []struct {
		configJSON string
		value      interface{}
		expected   func(ruConfig *controller.RUConfig)
	}{
		{
			configJSON: fmt.Sprintf(`{"degraded-mode-wait-duration": "%v"}`, waitDuration),
			value:      waitDuration,
			expected:   func(ruConfig *controller.RUConfig) { ruConfig.DegradedModeWaitDuration = waitDuration },
		},
		{
			configJSON: fmt.Sprintf(`{"ltb-token-rpc-max-delay": "%v"}`, tokenRPCMaxDelay),
			value:      waitDuration,
			expected: func(ruConfig *controller.RUConfig) {
				ruConfig.WaitRetryTimes = int(tokenRPCMaxDelay / ruConfig.WaitRetryInterval)
			},
		},
		{
			configJSON: fmt.Sprintf(`{"ltb-max-wait-duration": "%v"}`, waitDuration),
			value:      waitDuration,
			expected:   func(ruConfig *controller.RUConfig) { ruConfig.LTBMaxWaitDuration = waitDuration },
		},
		{
			configJSON: fmt.Sprintf(`{"read-base-cost": %v}`, readBaseCost),
			value:      readBaseCost,
			expected:   func(ruConfig *controller.RUConfig) { ruConfig.ReadBaseCost = controller.RequestUnit(readBaseCost) },
		},
		{
			configJSON: fmt.Sprintf(`{"write-base-cost": %v}`, readBaseCost*2),
			value:      readBaseCost * 2,
			expected:   func(ruConfig *controller.RUConfig) { ruConfig.WriteBaseCost = controller.RequestUnit(readBaseCost * 2) },
		},
		{
			// reset the degraded-mode-wait-duration to default in test.
			configJSON: fmt.Sprintf(`{"degraded-mode-wait-duration": "%v"}`, time.Second),
			value:      time.Second,
			expected:   func(ruConfig *controller.RUConfig) { ruConfig.DegradedModeWaitDuration = time.Second },
		},
	}
	// change properties one by one and verify each time
	for _, t := range testCases {
		sendRequest("POST", getAddr()+configURL, strings.NewReader(t.configJSON))
		time.Sleep(500 * time.Millisecond)
		t.expected(expectRUCfg)
		re.EqualValues(expectRUCfg, c1.GetConfig())

		expectRUCfg2 := *expectRUCfg
		// always apply the client option
		expectRUCfg2.LTBMaxWaitDuration = time.Hour
		re.EqualValues(&expectRUCfg2, c2.GetConfig())
	}
	// restart c1
	c1.Stop()
	c1, err = controller.NewResourceGroupController(suite.ctx, 1, cli, nil)
	re.NoError(err)
	re.EqualValues(expectRUCfg, c1.GetConfig())
	c1.Stop()
	c2.Stop()
}
