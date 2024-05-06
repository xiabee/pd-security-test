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

package pd

import (
	"context"
	"runtime/trace"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/pd/client/tsoutil"
	"go.uber.org/zap"
)

type tsoBatchController struct {
	maxBatchSize int
	// bestBatchSize is a dynamic size that changed based on the current batch effect.
	bestBatchSize int

	tsoRequestCh          chan *tsoRequest
	collectedRequests     []*tsoRequest
	collectedRequestCount int

	batchStartTime time.Time
}

func newTSOBatchController(tsoRequestCh chan *tsoRequest, maxBatchSize int) *tsoBatchController {
	return &tsoBatchController{
		maxBatchSize:          maxBatchSize,
		bestBatchSize:         8, /* Starting from a low value is necessary because we need to make sure it will be converged to (current_batch_size - 4) */
		tsoRequestCh:          tsoRequestCh,
		collectedRequests:     make([]*tsoRequest, maxBatchSize+1),
		collectedRequestCount: 0,
	}
}

// fetchPendingRequests will start a new round of the batch collecting from the channel.
// It returns true if everything goes well, otherwise false which means we should stop the service.
func (tbc *tsoBatchController) fetchPendingRequests(ctx context.Context, maxBatchWaitInterval time.Duration) error {
	var firstRequest *tsoRequest
	select {
	case <-ctx.Done():
		return ctx.Err()
	case firstRequest = <-tbc.tsoRequestCh:
	}
	// Start to batch when the first TSO request arrives.
	tbc.batchStartTime = time.Now()
	tbc.collectedRequestCount = 0
	tbc.pushRequest(firstRequest)

	// This loop is for trying best to collect more requests, so we use `tbc.maxBatchSize` here.
fetchPendingRequestsLoop:
	for tbc.collectedRequestCount < tbc.maxBatchSize {
		select {
		case tsoReq := <-tbc.tsoRequestCh:
			tbc.pushRequest(tsoReq)
		case <-ctx.Done():
			return ctx.Err()
		default:
			break fetchPendingRequestsLoop
		}
	}

	// Check whether we should fetch more pending TSO requests from the channel.
	// TODO: maybe consider the actual load that returns through a TSO response from PD server.
	if tbc.collectedRequestCount >= tbc.maxBatchSize || maxBatchWaitInterval <= 0 {
		return nil
	}

	// Fetches more pending TSO requests from the channel.
	// Try to collect `tbc.bestBatchSize` requests, or wait `maxBatchWaitInterval`
	// when `tbc.collectedRequestCount` is less than the `tbc.bestBatchSize`.
	if tbc.collectedRequestCount < tbc.bestBatchSize {
		after := time.NewTimer(maxBatchWaitInterval)
		defer after.Stop()
		for tbc.collectedRequestCount < tbc.bestBatchSize {
			select {
			case tsoReq := <-tbc.tsoRequestCh:
				tbc.pushRequest(tsoReq)
			case <-ctx.Done():
				return ctx.Err()
			case <-after.C:
				return nil
			}
		}
	}

	// Do an additional non-block try. Here we test the length with `tbc.maxBatchSize` instead
	// of `tbc.bestBatchSize` because trying best to fetch more requests is necessary so that
	// we can adjust the `tbc.bestBatchSize` dynamically later.
	for tbc.collectedRequestCount < tbc.maxBatchSize {
		select {
		case tsoReq := <-tbc.tsoRequestCh:
			tbc.pushRequest(tsoReq)
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	return nil
}

func (tbc *tsoBatchController) pushRequest(tsoReq *tsoRequest) {
	tbc.collectedRequests[tbc.collectedRequestCount] = tsoReq
	tbc.collectedRequestCount++
}

func (tbc *tsoBatchController) getCollectedRequests() []*tsoRequest {
	return tbc.collectedRequests[:tbc.collectedRequestCount]
}

// adjustBestBatchSize stabilizes the latency with the AIAD algorithm.
func (tbc *tsoBatchController) adjustBestBatchSize() {
	tsoBestBatchSize.Observe(float64(tbc.bestBatchSize))
	length := tbc.collectedRequestCount
	if length < tbc.bestBatchSize && tbc.bestBatchSize > 1 {
		// Waits too long to collect requests, reduce the target batch size.
		tbc.bestBatchSize--
	} else if length > tbc.bestBatchSize+4 /* Hard-coded number, in order to make `tbc.bestBatchSize` stable */ &&
		tbc.bestBatchSize < tbc.maxBatchSize {
		tbc.bestBatchSize++
	}
}

func (tbc *tsoBatchController) finishCollectedRequests(physical, firstLogical int64, suffixBits uint32, err error) {
	for i := 0; i < tbc.collectedRequestCount; i++ {
		tsoReq := tbc.collectedRequests[i]
		tsoReq.physical, tsoReq.logical = physical, tsoutil.AddLogical(firstLogical, int64(i), suffixBits)
		defer trace.StartRegion(tsoReq.requestCtx, "pdclient.tsoReqDequeue").End()
		tsoReq.tryDone(err)
	}
	// Prevent the finished requests from being processed again.
	tbc.collectedRequestCount = 0
}

func (tbc *tsoBatchController) revokePendingRequests(err error) {
	for i := 0; i < len(tbc.tsoRequestCh); i++ {
		req := <-tbc.tsoRequestCh
		req.tryDone(err)
	}
}

func (tbc *tsoBatchController) clear() {
	log.Info("[pd] clear the tso batch controller",
		zap.Int("max-batch-size", tbc.maxBatchSize), zap.Int("best-batch-size", tbc.bestBatchSize),
		zap.Int("collected-request-count", tbc.collectedRequestCount), zap.Int("pending-request-count", len(tbc.tsoRequestCh)))
	tsoErr := errors.WithStack(errClosing)
	tbc.finishCollectedRequests(0, 0, 0, tsoErr)
	tbc.revokePendingRequests(tsoErr)
}
