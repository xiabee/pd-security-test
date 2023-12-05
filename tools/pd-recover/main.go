package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"
)

var (
	v         bool
	endpoints string
	allocID   uint64
	clusterID uint64
	caPath    string
	certPath  string
	keyPath   string
)

const (
	requestTimeout = 10 * time.Second
	etcdTimeout    = 3 * time.Second

	pdRootPath      = "/pd"
	pdClusterIDPath = "/pd/cluster_id"
)

func exitErr(err error) {
	fmt.Println(err.Error())
	os.Exit(1)
}

func main() {
	fs := flag.NewFlagSet("pd-recover", flag.ExitOnError)
	fs.BoolVar(&v, "V", false, "print version information")
	fs.StringVar(&endpoints, "endpoints", "http://127.0.0.1:2379", "endpoints urls")
	fs.Uint64Var(&allocID, "alloc-id", 0, "please make sure alloced ID is safe")
	fs.Uint64Var(&clusterID, "cluster-id", 0, "please make cluster ID match with tikv")
	fs.StringVar(&caPath, "cacert", "", "path of file that contains list of trusted SSL CAs")
	fs.StringVar(&certPath, "cert", "", "path of file that contains list of trusted SSL CAs")
	fs.StringVar(&keyPath, "key", "", "path of file that contains X509 key in PEM format")

	if len(os.Args[1:]) == 0 {
		fs.Usage()
		return
	}
	if err := fs.Parse(os.Args[1:]); err != nil {
		exitErr(err)
	}
	if v {
		server.PrintPDInfo()
		return
	}
	if clusterID == 0 {
		fmt.Println("please specify safe cluster-id")
		return
	}
	if allocID == 0 {
		fmt.Println("please specify safe alloc-id")
		return
	}

	rootPath := path.Join(pdRootPath, strconv.FormatUint(clusterID, 10))
	clusterRootPath := path.Join(rootPath, "raft")
	raftBootstrapTimeKey := path.Join(clusterRootPath, "status", "raft_bootstrap_time")

	urls := strings.Split(endpoints, ",")

	tlsInfo := transport.TLSInfo{
		CertFile:      certPath,
		KeyFile:       keyPath,
		TrustedCAFile: caPath,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		fmt.Println("failed to connect: err")
		return
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   urls,
		DialTimeout: etcdTimeout,
		TLS:         tlsConfig,
	})
	if err != nil {
		exitErr(err)
	}
	ctx, cancel := context.WithTimeout(client.Ctx(), requestTimeout)
	defer cancel()

	var ops []clientv3.Op
	// recover cluster_id
	ops = append(ops, clientv3.OpPut(pdClusterIDPath, string(typeutil.Uint64ToBytes(clusterID))))
	// recover alloc_id
	allocIDPath := path.Join(rootPath, "alloc_id")
	ops = append(ops, clientv3.OpPut(allocIDPath, string(typeutil.Uint64ToBytes(allocID))))

	// recover bootstrap
	// recover meta of cluster
	clusterMeta := metapb.Cluster{Id: clusterID}
	clusterValue, err := clusterMeta.Marshal()
	if err != nil {
		exitErr(err)
	}
	ops = append(ops, clientv3.OpPut(clusterRootPath, string(clusterValue)))

	// set raft bootstrap time
	nano := time.Now().UnixNano()
	timeData := typeutil.Uint64ToBytes(uint64(nano))
	ops = append(ops, clientv3.OpPut(raftBootstrapTimeKey, string(timeData)))

	// the new pd cluster should not bootstrapped by tikv
	bootstrapCmp := clientv3.Compare(clientv3.CreateRevision(clusterRootPath), "=", 0)
	resp, err := client.Txn(ctx).If(bootstrapCmp).Then(ops...).Commit()
	if err != nil {
		exitErr(err)
	}
	if !resp.Succeeded {
		fmt.Println("failed to recover: the cluster is already bootstrapped")
		return
	}
	fmt.Println("recover success! please restart the PD cluster")
}
