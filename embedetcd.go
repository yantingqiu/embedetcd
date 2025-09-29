package embedetcd

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

const (
	etcdStartTimeout = 60 * time.Second
	dialEtcdTimeout  = 5 * time.Second
)

type Option func(*embed.Config)

func WithDataDir(dir string) Option {
	return func(c *embed.Config) {
		c.Dir = dir
	}
}

func WithName(name string) Option {
	return func(c *embed.Config) {
		c.Name = name
	}
}

func WithListenClientURLs(urls ...url.URL) Option {
	return func(c *embed.Config) {
		c.ListenClientUrls = urls
	}
}

func WithAdvertiseClientURLs(urls ...url.URL) Option {
	return func(c *embed.Config) {
		c.AdvertiseClientUrls = urls
	}
}

func WithListenPeerURLs(urls ...url.URL) Option {
	return func(c *embed.Config) {
		c.ListenPeerUrls = urls
	}
}

func WithAdvertisePeerURLs(urls ...url.URL) Option {
	return func(c *embed.Config) {
		c.AdvertisePeerUrls = urls
	}
}

func WithInitialCluster(cluster string) Option {
	return func(c *embed.Config) {
		c.InitialCluster = cluster
	}
}

func WithClusterState(state string) Option {
	return func(c *embed.Config) {
		c.ClusterState = state
	}
}

func WithInitialClusterToken(token string) Option {
	return func(c *embed.Config) {
		c.InitialClusterToken = token
	}
}

func WithClientTLS(certFile, keyFile, caFile string) Option {
	return func(c *embed.Config) {
		c.ClientTLSInfo.CertFile = certFile
		c.ClientTLSInfo.KeyFile = keyFile
		c.ClientTLSInfo.TrustedCAFile = caFile
		c.ClientTLSInfo.ClientCertAuth = true
	}
}

func WithPeerTLS(certFile, keyFile, caFile string) Option {
	return func(c *embed.Config) {
		c.PeerTLSInfo.CertFile = certFile
		c.PeerTLSInfo.KeyFile = keyFile
		c.PeerTLSInfo.TrustedCAFile = caFile
		c.PeerTLSInfo.ClientCertAuth = true
	}
}

func WithPeerAutoTLS() Option {
	return func(c *embed.Config) {
		c.PeerAutoTLS = true
	}
}

func WithClientAutoTLS() Option {
	return func(c *embed.Config) {
		c.ClientAutoTLS = true
	}
}

type EmbedEtcd struct {
	Config     *embed.Config
	EtcdServer *embed.Etcd
	mu         sync.RWMutex
}

func NewEmbedEtcd(opts ...Option) *EmbedEtcd {
	cfg := embed.NewConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	return &EmbedEtcd{
		Config: cfg,
	}
}

func (e *EmbedEtcd) Start() error {
	return e.StartWithTimeout(etcdStartTimeout)
}

func (e *EmbedEtcd) StartWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return e.StartWithContext(ctx)
}

func (e *EmbedEtcd) StartWithContext(ctx context.Context) error {
	if e.EtcdServer != nil {
		return fmt.Errorf("etcd server is already running")
	}

	etcd, err := embed.StartEtcd(e.Config)
	if err != nil {
		return fmt.Errorf("failed to start etcd server: %w", err)
	}
	e.EtcdServer = etcd

	select {
	case <-etcd.Server.ReadyNotify():
		return nil
	case <-ctx.Done():
		etcd.Server.Stop()
		return ctx.Err()
	}
}

func (e *EmbedEtcd) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.EtcdServer == nil {
		return nil
	}

	e.EtcdServer.Close()
	e.EtcdServer = nil
	return nil
}

func (e *EmbedEtcd) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.EtcdServer != nil
}

func (e *EmbedEtcd) GetClient() (*clientv3.Client, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.EtcdServer == nil {
		return nil, fmt.Errorf("etcd server is not running")
	}
	return GetEmbedEtcdClient(e.EtcdServer.Server)
}

func (e *EmbedEtcd) GetClusterClient(clusterEndpoints []string) (*clientv3.Client, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.EtcdServer == nil {
		return nil, fmt.Errorf("etcd server is not running")
	}
	return GetClusterClient(clusterEndpoints)
}

func GetEmbedEtcdClient(server *etcdserver.EtcdServer) (*clientv3.Client, error) {
	if server == nil {
		return nil, fmt.Errorf("etcd server is not running")
	}
	client := v3client.New(server)
	return client, nil
}

func GetClusterClient(clusterEndpoints []string) (*clientv3.Client, error) {
	if len(clusterEndpoints) == 0 {
		return nil, fmt.Errorf("cluster endpoints can not be empty")
	}
	cfg := clientv3.Config{
		Endpoints:   clusterEndpoints,
		DialTimeout: dialEtcdTimeout,
	}

	return clientv3.New(cfg)
}
