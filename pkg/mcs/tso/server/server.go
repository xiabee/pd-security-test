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

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/tsopb"
	"github.com/pingcap/log"
	"github.com/pingcap/sysutil"
	"github.com/soheilhy/cmux"
	"github.com/spf13/cobra"
	bs "github.com/tikv/pd/pkg/basicserver"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/discovery"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/member"
	"github.com/tikv/pd/pkg/systimemon"
	"github.com/tikv/pd/pkg/tso"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/grpcutil"
	"github.com/tikv/pd/pkg/utils/logutil"
	"github.com/tikv/pd/pkg/utils/metricutil"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/pkg/versioninfo"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// pdRootPath is the old path for storing the tso related root path.
	pdRootPath        = "/pd"
	msServiceRootPath = "/ms"
	// tsoSvcRootPathFormat defines the root path for all etcd paths used for different purposes.
	// format: "/ms/{cluster_id}/tso".
	tsoSvcRootPathFormat = msServiceRootPath + "/%d/" + mcsutils.TSOServiceName
)

var _ bs.Server = (*Server)(nil)
var _ tso.ElectionMember = (*member.Participant)(nil)

// Server is the TSO server, and it implements bs.Server.
type Server struct {
	diagnosticspb.DiagnosticsServer

	// Server state. 0 is not running, 1 is running.
	isRunning int64
	// Server start timestamp
	startTimestamp int64

	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	handler *Handler

	cfg         *Config
	clusterID   uint64
	listenURL   *url.URL
	backendUrls []url.URL

	// etcd client
	etcdClient *clientv3.Client
	// http client
	httpClient *http.Client

	muxListener          net.Listener
	service              *Service
	keyspaceGroupManager *tso.KeyspaceGroupManager
	// Store as map[string]*grpc.ClientConn
	clientConns sync.Map
	// tsoDispatcher is used to dispatch the TSO requests to
	// the corresponding forwarding TSO channels.
	tsoDispatcher *tsoutil.TSODispatcher
	// tsoProtoFactory is the abstract factory for creating tso
	// related data structures defined in the tso grpc protocol
	tsoProtoFactory *tsoutil.TSOProtoFactory

	// Callback functions for different stages
	// startCallbacks will be called after the server is started.
	startCallbacks []func()

	// for service registry
	serviceID       *discovery.ServiceRegistryEntry
	serviceRegister *discovery.ServiceRegister
}

// Implement the following methods defined in bs.Server

// Name returns the unique Name for this server in the TSO cluster.
func (s *Server) Name() string {
	return s.cfg.Name
}

// Context returns the context of server.
func (s *Server) Context() context.Context {
	return s.ctx
}

// GetHandler returns the handler.
func (s *Server) GetHandler() *Handler {
	return s.handler
}

// GetBasicServer returns the basic server.
func (s *Server) GetBasicServer() bs.Server {
	return s
}

// GetAddr returns the address of the server.
func (s *Server) GetAddr() string {
	return s.cfg.ListenAddr
}

// Run runs the TSO server.
func (s *Server) Run() error {
	go systimemon.StartMonitor(s.ctx, time.Now, func() {
		log.Error("system time jumps backward", errs.ZapError(errs.ErrIncorrectSystemTime))
		timeJumpBackCounter.Inc()
	})

	if err := s.initClient(); err != nil {
		return err
	}
	return s.startServer()
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.isRunning, 1, 0) {
		// server is already closed
		return
	}

	log.Info("closing tso server ...")
	// close tso service loops in the keyspace group manager
	s.keyspaceGroupManager.Close()
	s.serviceRegister.Deregister()
	s.muxListener.Close()
	s.serverLoopCancel()
	s.serverLoopWg.Wait()

	if s.etcdClient != nil {
		if err := s.etcdClient.Close(); err != nil {
			log.Error("close etcd client meet error", errs.ZapError(errs.ErrCloseEtcdClient, err))
		}
	}

	if s.httpClient != nil {
		s.httpClient.CloseIdleConnections()
	}
	log.Info("tso server is closed")
}

// GetClient returns builtin etcd client.
func (s *Server) GetClient() *clientv3.Client {
	return s.etcdClient
}

// GetHTTPClient returns builtin http client.
func (s *Server) GetHTTPClient() *http.Client {
	return s.httpClient
}

// AddStartCallback adds a callback in the startServer phase.
func (s *Server) AddStartCallback(callbacks ...func()) {
	s.startCallbacks = append(s.startCallbacks, callbacks...)
}

// IsServing implements basicserver. It returns whether the server is the leader
// if there is embedded etcd, or the primary otherwise.
func (s *Server) IsServing() bool {
	return s.IsKeyspaceServing(mcsutils.DefaultKeyspaceID, mcsutils.DefaultKeyspaceGroupID)
}

// IsKeyspaceServing returns whether the server is the primary of the given keyspace.
// TODO: update basicserver interface to support keyspace.
func (s *Server) IsKeyspaceServing(keyspaceID, keyspaceGroupID uint32) bool {
	if atomic.LoadInt64(&s.isRunning) == 0 {
		return false
	}

	member, err := s.keyspaceGroupManager.GetElectionMember(
		keyspaceID, keyspaceGroupID)
	if err != nil {
		log.Error("failed to get election member", errs.ZapError(err))
		return false
	}
	return member.IsLeader()
}

// GetLeaderListenUrls gets service endpoints from the leader in election group.
// The entry at the index 0 is the primary's service endpoint.
func (s *Server) GetLeaderListenUrls() []string {
	member, err := s.keyspaceGroupManager.GetElectionMember(
		mcsutils.DefaultKeyspaceID, mcsutils.DefaultKeyspaceGroupID)
	if err != nil {
		log.Error("failed to get election member", errs.ZapError(err))
		return nil
	}

	return member.GetLeaderListenUrls()
}

// AddServiceReadyCallback implements basicserver.
// It adds callbacks when it's ready for providing tso service.
func (s *Server) AddServiceReadyCallback(callbacks ...func(context.Context)) {
	// Do nothing here. The primary of each keyspace group assigned to this host
	// will respond to the requests accordingly.
}

// Implement the other methods

// ClusterID returns the cluster ID of this server.
func (s *Server) ClusterID() uint64 {
	return s.clusterID
}

// IsClosed checks if the server loop is closed
func (s *Server) IsClosed() bool {
	return atomic.LoadInt64(&s.isRunning) == 0
}

// GetTSOAllocatorManager returns the manager of TSO Allocator.
func (s *Server) GetTSOAllocatorManager(keyspaceGroupID uint32) (*tso.AllocatorManager, error) {
	return s.keyspaceGroupManager.GetAllocatorManager(keyspaceGroupID)
}

// IsLocalRequest checks if the forwarded host is the current host
func (s *Server) IsLocalRequest(forwardedHost string) bool {
	// TODO: Check if the forwarded host is the current host.
	// The logic is depending on etcd service mode -- if the TSO service
	// uses the embedded etcd, check against ClientUrls; otherwise check
	// against the cluster membership.
	return forwardedHost == ""
}

// GetDelegateClient returns grpc client connection talking to the forwarded host
func (s *Server) GetDelegateClient(ctx context.Context, forwardedHost string) (*grpc.ClientConn, error) {
	client, ok := s.clientConns.Load(forwardedHost)
	if !ok {
		tlsConfig, err := s.GetTLSConfig().ToTLSConfig()
		if err != nil {
			return nil, err
		}
		cc, err := grpcutil.GetClientConn(ctx, forwardedHost, tlsConfig)
		if err != nil {
			return nil, err
		}
		client = cc
		s.clientConns.Store(forwardedHost, cc)
	}
	return client.(*grpc.ClientConn), nil
}

// ValidateInternalRequest checks if server is closed, which is used to validate
// the gRPC communication between TSO servers internally.
// TODO: Check if the sender is from the global TSO allocator
func (s *Server) ValidateInternalRequest(_ *tsopb.RequestHeader, _ bool) error {
	if s.IsClosed() {
		return ErrNotStarted
	}
	return nil
}

// ValidateRequest checks if the keyspace replica is the primary and clusterID is matched.
// TODO: Check if the keyspace replica is the primary
func (s *Server) ValidateRequest(header *tsopb.RequestHeader) error {
	if s.IsClosed() {
		return ErrNotStarted
	}
	if header.GetClusterId() != s.clusterID {
		return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.clusterID, header.GetClusterId())
	}
	return nil
}

// GetExternalTS returns external timestamp from the cache or the persistent storage.
// TODO: Implement GetExternalTS
func (s *Server) GetExternalTS() uint64 {
	return 0
}

// SetExternalTS saves external timestamp to cache and the persistent storage.
// TODO: Implement SetExternalTS
func (s *Server) SetExternalTS(externalTS uint64) error {
	return nil
}

// GetConfig gets the config.
func (s *Server) GetConfig() *Config {
	return s.cfg
}

// GetTLSConfig gets the security config.
func (s *Server) GetTLSConfig() *grpcutil.TLSConfig {
	return &s.cfg.Security.TLSConfig
}

func (s *Server) initClient() error {
	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	s.backendUrls, err = types.NewURLs(strings.Split(s.cfg.BackendEndpoints, ","))
	if err != nil {
		return err
	}
	s.etcdClient, s.httpClient, err = etcdutil.CreateClientsWithMultiEndpoint(tlsConfig, s.backendUrls)
	return err
}

func (s *Server) startGRPCServer(l net.Listener) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	gs := grpc.NewServer()
	s.service.RegisterGRPCService(gs)
	diagnosticspb.RegisterDiagnosticsServer(gs, s)
	serverr := gs.Serve(l)
	log.Info("grpc server stopped serving")

	// Attempt graceful stop (waits for pending RPCs), but force a stop if
	// it doesn't happen in a reasonable amount of time.
	done := make(chan struct{})
	go func() {
		defer logutil.LogPanic()
		log.Info("try to gracefully stop the server now")
		gs.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(mcsutils.DefaultGRPCGracefulStopTimeout):
		log.Info("stopping grpc gracefully is taking longer than expected and force stopping now")
		gs.Stop()
	}

	if s.IsClosed() {
		log.Info("grpc server stopped")
	} else {
		log.Fatal("grpc server stopped unexpectedly", errs.ZapError(serverr))
	}
}

func (s *Server) startHTTPServer(l net.Listener) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	handler, _ := SetUpRestHandler(s.service)
	hs := &http.Server{
		Handler:           handler,
		ReadTimeout:       5 * time.Minute,
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverr := hs.Serve(l)
	log.Info("http server stopped serving")

	ctx, cancel := context.WithTimeout(context.Background(), mcsutils.DefaultHTTPGracefulShutdownTimeout)
	defer cancel()
	if err := hs.Shutdown(ctx); err != nil {
		log.Error("http server shutdown encountered problem", errs.ZapError(err))
	} else {
		log.Info("all http(s) requests finished")
	}
	if s.IsClosed() {
		log.Info("http server stopped")
	} else {
		log.Fatal("http server stopped unexpectedly", errs.ZapError(serverr))
	}
}

func (s *Server) startGRPCAndHTTPServers(l net.Listener) {
	defer logutil.LogPanic()
	defer s.serverLoopWg.Done()

	mux := cmux.New(l)
	grpcL := mux.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpL := mux.Match(cmux.Any())

	s.serverLoopWg.Add(2)
	go s.startGRPCServer(grpcL)
	go s.startHTTPServer(httpL)

	if err := mux.Serve(); err != nil {
		if s.IsClosed() {
			log.Info("mux stop serving", errs.ZapError(err))
		} else {
			log.Panic("mux stop serving unexpectedly", errs.ZapError(err))
		}
	}
}

func (s *Server) startServer() (err error) {
	if s.clusterID, err = mcsutils.InitClusterID(s.ctx, s.etcdClient); err != nil {
		return err
	}
	log.Info("init cluster id", zap.Uint64("cluster-id", s.clusterID))

	// It may lose accuracy if use float64 to store uint64. So we store the cluster id in label.
	metadataGauge.WithLabelValues(fmt.Sprintf("cluster%d", s.clusterID)).Set(0)
	// The independent TSO service still reuses PD version info since PD and TSO are just
	// different service modes provided by the same pd-server binary
	serverInfo.WithLabelValues(versioninfo.PDReleaseVersion, versioninfo.PDGitHash).Set(float64(time.Now().Unix()))

	s.listenURL, err = url.Parse(s.cfg.ListenAddr)
	if err != nil {
		return err
	}

	// Initialize the TSO service.
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(s.ctx)
	legacySvcRootPath := path.Join(pdRootPath, strconv.FormatUint(s.clusterID, 10))
	tsoSvcRootPath := fmt.Sprintf(tsoSvcRootPathFormat, s.clusterID)
	s.serviceID = &discovery.ServiceRegistryEntry{ServiceAddr: s.cfg.AdvertiseListenAddr}
	s.keyspaceGroupManager = tso.NewKeyspaceGroupManager(
		s.serverLoopCtx, s.serviceID, s.etcdClient, s.httpClient, s.listenURL.Host, legacySvcRootPath, tsoSvcRootPath, s.cfg)
	if err := s.keyspaceGroupManager.Initialize(); err != nil {
		return err
	}

	s.tsoProtoFactory = &tsoutil.TSOProtoFactory{}
	s.service = &Service{Server: s}

	tlsConfig, err := s.cfg.Security.ToTLSConfig()
	if err != nil {
		return err
	}
	if tlsConfig != nil {
		s.muxListener, err = tls.Listen(mcsutils.TCPNetworkStr, s.listenURL.Host, tlsConfig)
	} else {
		s.muxListener, err = net.Listen(mcsutils.TCPNetworkStr, s.listenURL.Host)
	}
	if err != nil {
		return err
	}

	s.serverLoopWg.Add(1)
	go s.startGRPCAndHTTPServers(s.muxListener)

	// Run callbacks
	log.Info("triggering the start callback functions")
	for _, cb := range s.startCallbacks {
		cb()
	}

	// Server has started.
	serializedEntry, err := s.serviceID.Serialize()
	if err != nil {
		return err
	}
	s.serviceRegister = discovery.NewServiceRegister(s.ctx, s.etcdClient, strconv.FormatUint(s.clusterID, 10),
		mcsutils.TSOServiceName, s.cfg.AdvertiseListenAddr, serializedEntry, discovery.DefaultLeaseInSeconds)
	if err := s.serviceRegister.Register(); err != nil {
		log.Error("failed to register the service", zap.String("service-name", mcsutils.TSOServiceName), errs.ZapError(err))
		return err
	}

	atomic.StoreInt64(&s.isRunning, 1)
	return nil
}

// CreateServer creates the Server
func CreateServer(ctx context.Context, cfg *Config) *Server {
	svr := &Server{
		DiagnosticsServer: sysutil.NewDiagnosticsServer(cfg.Log.File.Filename),
		startTimestamp:    time.Now().Unix(),
		cfg:               cfg,
		ctx:               ctx,
	}
	svr.handler = newHandler(svr)
	return svr
}

// CreateServerWrapper encapsulates the configuration/log/metrics initialization and create the server
func CreateServerWrapper(cmd *cobra.Command, args []string) {
	cmd.Flags().Parse(args)
	cfg := NewConfig()
	flagSet := cmd.Flags()
	err := cfg.Parse(flagSet)
	defer logutil.LogPanic()

	if err != nil {
		cmd.Println(err)
		return
	}

	if printVersion, err := flagSet.GetBool("version"); err != nil {
		cmd.Println(err)
		return
	} else if printVersion {
		versioninfo.Print()
		exit(0)
	}

	// New zap logger
	err = logutil.SetupLogger(cfg.Log, &cfg.Logger, &cfg.LogProps, cfg.Security.RedactInfoLog)
	if err == nil {
		log.ReplaceGlobals(cfg.Logger, cfg.LogProps)
	} else {
		log.Fatal("initialize logger error", errs.ZapError(err))
	}
	// Flushing any buffered log entries
	defer log.Sync()

	versioninfo.Log("TSO")
	log.Info("TSO config", zap.Reflect("config", cfg))

	grpcprometheus.EnableHandlingTimeHistogram()
	metricutil.Push(&cfg.Metric)

	ctx, cancel := context.WithCancel(context.Background())
	svr := CreateServer(ctx, cfg)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	if err := svr.Run(); err != nil {
		log.Fatal("run server failed", errs.ZapError(err))
	}

	<-ctx.Done()
	log.Info("got signal to exit", zap.String("signal", sig.String()))

	svr.Close()
	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func exit(code int) {
	log.Sync()
	os.Exit(code)
}
