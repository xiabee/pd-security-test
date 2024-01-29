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

package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/errs"
	"github.com/tikv/pd/client/retry"
	"go.uber.org/zap"
)

const (
	// defaultCallerID marks the default caller ID of the PD HTTP client.
	defaultCallerID = "pd-http-client"
	// defaultInnerCallerID marks the default caller ID of the inner PD HTTP client.
	// It's used to distinguish the requests sent by the inner client via some internal logic.
	defaultInnerCallerID = "pd-http-client-inner"
	httpScheme           = "http"
	httpsScheme          = "https"
	networkErrorStatus   = "network error"

	defaultMembersInfoUpdateInterval = time.Minute
	defaultTimeout                   = 30 * time.Second
)

// respHandleFunc is the function to handle the HTTP response.
type respHandleFunc func(resp *http.Response, res interface{}) error

// clientInner is the inner implementation of the PD HTTP client, which contains some fundamental fields.
// It is wrapped by the `client` struct to make sure the inner implementation won't be exposed and could
// be consistent during the copy.
type clientInner struct {
	ctx    context.Context
	cancel context.CancelFunc

	sd pd.ServiceDiscovery

	// source is used to mark the source of the client creation,
	// it will also be used in the caller ID of the inner client.
	source  string
	tlsConf *tls.Config
	cli     *http.Client

	requestCounter    *prometheus.CounterVec
	executionDuration *prometheus.HistogramVec
	// defaultSD indicates whether the client is created with the default service discovery.
	defaultSD bool
}

func newClientInner(ctx context.Context, cancel context.CancelFunc, source string) *clientInner {
	return &clientInner{ctx: ctx, cancel: cancel, source: source}
}

func (ci *clientInner) init(sd pd.ServiceDiscovery) {
	// Init the HTTP client if it's not configured.
	if ci.cli == nil {
		ci.cli = &http.Client{Timeout: defaultTimeout}
		if ci.tlsConf != nil {
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.TLSClientConfig = ci.tlsConf
			ci.cli.Transport = transport
		}
	}
	ci.sd = sd
}

func (ci *clientInner) close() {
	ci.cancel()
	if ci.cli != nil {
		ci.cli.CloseIdleConnections()
	}
	// only close the service discovery if it's created by the client.
	if ci.defaultSD && ci.sd != nil {
		ci.sd.Close()
	}
}

func (ci *clientInner) reqCounter(name, status string) {
	if ci.requestCounter == nil {
		return
	}
	ci.requestCounter.WithLabelValues(name, status).Inc()
}

func (ci *clientInner) execDuration(name string, duration time.Duration) {
	if ci.executionDuration == nil {
		return
	}
	ci.executionDuration.WithLabelValues(name).Observe(duration.Seconds())
}

// requestWithRetry will first try to send the request to the PD leader, if it fails, it will try to send
// the request to the other PD followers to gain a better availability.
func (ci *clientInner) requestWithRetry(
	ctx context.Context,
	reqInfo *requestInfo,
	headerOpts ...HeaderOption,
) error {
	var (
		statusCode int
		err        error
	)
	execFunc := func() error {
		// It will try to send the request to the PD leader first and then try to send the request to the other PD followers.
		clients := ci.sd.GetAllServiceClients()
		if len(clients) == 0 {
			return errs.ErrClientNoAvailableMember
		}
		for _, cli := range clients {
			addr := cli.GetHTTPAddress()
			statusCode, err = ci.doRequest(ctx, addr, reqInfo, headerOpts...)
			if err == nil || noNeedRetry(statusCode) {
				return err
			}
			log.Debug("[pd] request addr failed",
				zap.String("source", ci.source), zap.Bool("is-leader", cli.IsConnectedToLeader()), zap.String("addr", addr), zap.Error(err))
		}
		return err
	}
	if reqInfo.bo == nil {
		return execFunc()
	}
	// Copy a new backoffer for each request.
	bo := *reqInfo.bo
	// Backoffer also needs to check the status code to determine whether to retry.
	bo.SetRetryableChecker(func(err error) bool {
		return err != nil && !noNeedRetry(statusCode)
	})
	return bo.Exec(ctx, execFunc)
}

func noNeedRetry(statusCode int) bool {
	return statusCode == http.StatusNotFound ||
		statusCode == http.StatusForbidden ||
		statusCode == http.StatusBadRequest
}

func (ci *clientInner) doRequest(
	ctx context.Context,
	addr string, reqInfo *requestInfo,
	headerOpts ...HeaderOption,
) (int, error) {
	var (
		source      = ci.source
		callerID    = reqInfo.callerID
		name        = reqInfo.name
		url         = reqInfo.getURL(addr)
		method      = reqInfo.method
		body        = reqInfo.body
		res         = reqInfo.res
		respHandler = reqInfo.respHandler
	)
	logFields := []zap.Field{
		zap.String("source", source),
		zap.String("name", name),
		zap.String("url", url),
		zap.String("method", method),
		zap.String("caller-id", callerID),
	}
	log.Debug("[pd] request the http url", logFields...)
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(body))
	if err != nil {
		log.Error("[pd] create http request failed", append(logFields, zap.Error(err))...)
		return -1, errors.Trace(err)
	}
	for _, opt := range headerOpts {
		opt(req.Header)
	}
	req.Header.Set(xCallerIDKey, callerID)

	start := time.Now()
	resp, err := ci.cli.Do(req)
	if err != nil {
		ci.reqCounter(name, networkErrorStatus)
		log.Error("[pd] do http request failed", append(logFields, zap.Error(err))...)
		return -1, errors.Trace(err)
	}
	ci.execDuration(name, time.Since(start))
	ci.reqCounter(name, resp.Status)

	// Give away the response handling to the caller if the handler is set.
	if respHandler != nil {
		return resp.StatusCode, respHandler(resp, res)
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Warn("[pd] close http response body failed", append(logFields, zap.Error(err))...)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		logFields = append(logFields, zap.String("status", resp.Status))

		bs, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			logFields = append(logFields, zap.NamedError("read-body-error", err))
		} else {
			logFields = append(logFields, zap.ByteString("body", bs))
		}

		log.Error("[pd] request failed with a non-200 status", logFields...)
		return resp.StatusCode, errors.Errorf("request pd http api failed with status: '%s'", resp.Status)
	}

	if res == nil {
		return resp.StatusCode, nil
	}

	err = json.NewDecoder(resp.Body).Decode(res)
	if err != nil {
		return resp.StatusCode, errors.Trace(err)
	}
	return resp.StatusCode, nil
}

type client struct {
	inner *clientInner

	callerID    string
	respHandler respHandleFunc
	bo          *retry.Backoffer
}

// ClientOption configures the HTTP client.
type ClientOption func(c *client)

// WithHTTPClient configures the client with the given initialized HTTP client.
func WithHTTPClient(cli *http.Client) ClientOption {
	return func(c *client) {
		c.inner.cli = cli
	}
}

// WithTLSConfig configures the client with the given TLS config.
// This option won't work if the client is configured with WithHTTPClient.
func WithTLSConfig(tlsConf *tls.Config) ClientOption {
	return func(c *client) {
		c.inner.tlsConf = tlsConf
	}
}

// WithMetrics configures the client with metrics.
func WithMetrics(
	requestCounter *prometheus.CounterVec,
	executionDuration *prometheus.HistogramVec,
) ClientOption {
	return func(c *client) {
		c.inner.requestCounter = requestCounter
		c.inner.executionDuration = executionDuration
	}
}

// NewClientWithServiceDiscovery creates a PD HTTP client with the given PD service discovery.
func NewClientWithServiceDiscovery(
	source string,
	sd pd.ServiceDiscovery,
	opts ...ClientOption,
) Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{inner: newClientInner(ctx, cancel, source), callerID: defaultCallerID}
	// Apply the options first.
	for _, opt := range opts {
		opt(c)
	}
	c.inner.init(sd)
	return c
}

// NewClient creates a PD HTTP client with the given PD addresses and TLS config.
func NewClient(
	source string,
	pdAddrs []string,
	opts ...ClientOption,
) Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{inner: newClientInner(ctx, cancel, source), callerID: defaultCallerID}
	// Apply the options first.
	for _, opt := range opts {
		opt(c)
	}
	sd := pd.NewDefaultPDServiceDiscovery(ctx, cancel, pdAddrs, c.inner.tlsConf)
	if err := sd.Init(); err != nil {
		log.Error("[pd] init service discovery failed", zap.String("source", source), zap.Strings("pd-addrs", pdAddrs), zap.Error(err))
		return nil
	}
	c.inner.init(sd)
	c.inner.defaultSD = true
	return c
}

// Close gracefully closes the HTTP client.
func (c *client) Close() {
	c.inner.close()
	log.Info("[pd] http client closed", zap.String("source", c.inner.source))
}

// WithCallerID sets and returns a new client with the given caller ID.
func (c *client) WithCallerID(callerID string) Client {
	newClient := *c
	newClient.callerID = callerID
	return &newClient
}

// WithRespHandler sets and returns a new client with the given HTTP response handler.
func (c *client) WithRespHandler(
	handler func(resp *http.Response, res interface{}) error,
) Client {
	newClient := *c
	newClient.respHandler = handler
	return &newClient
}

// WithBackoffer sets and returns a new client with the given backoffer.
func (c *client) WithBackoffer(bo *retry.Backoffer) Client {
	newClient := *c
	newClient.bo = bo
	return &newClient
}

// Header key definition constants.
const (
	pdAllowFollowerHandleKey = "PD-Allow-Follower-Handle"
	xCallerIDKey             = "X-Caller-ID"
)

// HeaderOption configures the HTTP header.
type HeaderOption func(header http.Header)

// WithAllowFollowerHandle sets the header field to allow a PD follower to handle this request.
func WithAllowFollowerHandle() HeaderOption {
	return func(header http.Header) {
		header.Set(pdAllowFollowerHandleKey, "true")
	}
}

func (c *client) request(ctx context.Context, reqInfo *requestInfo, headerOpts ...HeaderOption) error {
	return c.inner.requestWithRetry(ctx, reqInfo.
		WithCallerID(c.callerID).
		WithRespHandler(c.respHandler).
		WithBackoffer(c.bo),
		headerOpts...)
}

/* The following functions are only for test */
// requestChecker is used to check the HTTP request sent by the client.
type requestChecker func(req *http.Request) error

// RoundTrip implements the `http.RoundTripper` interface.
func (rc requestChecker) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	return &http.Response{StatusCode: http.StatusOK}, rc(req)
}

// NewHTTPClientWithRequestChecker returns a http client with checker.
func NewHTTPClientWithRequestChecker(checker requestChecker) *http.Client {
	return &http.Client{
		Transport: checker,
	}
}

// newClientWithoutInitServiceDiscovery creates a PD HTTP client
// with the given PD addresses and TLS config without init service discovery.
func newClientWithoutInitServiceDiscovery(
	source string,
	pdAddrs []string,
	opts ...ClientOption,
) Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{inner: newClientInner(ctx, cancel, source), callerID: defaultCallerID}
	// Apply the options first.
	for _, opt := range opts {
		opt(c)
	}
	sd := pd.NewDefaultPDServiceDiscovery(ctx, cancel, pdAddrs, c.inner.tlsConf)
	c.inner.init(sd)
	return c
}
