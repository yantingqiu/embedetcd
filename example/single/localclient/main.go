package main

import (
	"context"
	"fmt"
	"github.com/yantingqiu/embedetcd"
	"go.etcd.io/etcd/server/v3/embed"
	"net/url"
	"sync"
)

const (
	key1 = "/key1"
)

var (
	embedEtcd *embedetcd.EmbedEtcd
	ready     = make(chan struct{})
	mu        sync.RWMutex
)

func Server() {
	defer close(ready)

	clientListenURL := "http://0.0.0.0:3379"
	clientAdvertiseURL := "http://0.0.0.0:4379"
	peerListenURL := "http://0.0.0.0:3380"
	peerAdvertiseURL := "http://localhost:4380"

	clientListen, err := url.Parse(clientListenURL)
	if err != nil {
		fmt.Printf("client listen url parse failed: %v\n", err)
		return
	}

	clientAdvertise, err := url.Parse(clientAdvertiseURL)
	if err != nil {
		fmt.Printf("client advertise url parse failed: %v\n", err)
		return
	}

	peerListen, err := url.Parse(peerListenURL)
	if err != nil {
		fmt.Printf("peer listen url parse failed: %v\n", err)
		return
	}

	peerAdvertise, err := url.Parse(peerAdvertiseURL)
	if err != nil {
		fmt.Printf("peer advertise url parse failed: %v\n", err)
		return
	}

	etcd := embedetcd.NewEmbedEtcd(
		embedetcd.WithDataDir("./etcd-data"),
		embedetcd.WithName("single-local"),
		embedetcd.WithListenClientURLs(*clientListen),
		embedetcd.WithAdvertiseClientURLs(*clientAdvertise),
		embedetcd.WithListenPeerURLs(*peerListen),
		embedetcd.WithAdvertisePeerURLs(*peerAdvertise),
		embedetcd.WithInitialCluster("single-local=http://localhost:4380"),
		embedetcd.WithInitialClusterToken("etcd-cluster-local-client"),
		embedetcd.WithClusterState(embed.ClusterStateFlagNew),
	)

	if err := etcd.Start(); err != nil {
		fmt.Printf("single embedded etcd start failed: %v\n", err)
		return
	}

	mu.Lock()
	embedEtcd = etcd
	mu.Unlock()

	fmt.Println("single embed etcd start success")
}

func getEtcdClient() (*embedetcd.EmbedEtcd, error) {
	mu.RLock()
	defer mu.RUnlock()

	if embedEtcd == nil {
		return nil, fmt.Errorf("etcd server not ready")
	}
	return embedEtcd, nil
}

func Client() {
	<-ready
	etcd, err := getEtcdClient()
	if err != nil {
		fmt.Printf("failed to get etcd client: %v\n", err)
		return
	}

	ctx := context.Background()
	embedClient, err := embedetcd.GetEmbedEtcdClient(etcd.EtcdServer.Server)
	if err != nil {
		fmt.Printf("failed to get embed client of etcd: %v\n", err)
		return
	}

	value1 := "hello, world!"
	_, err = embedClient.Put(ctx, key1, value1)
	if err != nil {
		fmt.Printf("failed to put key %s, value %s, error: %v\n", key1, value1, err)
		return
	}

	resp, err := embedClient.Get(ctx, key1)
	if err != nil {
		fmt.Printf("failed to get key %s, error: %v\n", key1, err)
		return
	}

	if len(resp.Kvs) == 0 {
		fmt.Printf("key %s not found\n", key1)
		return
	}

	value := string(resp.Kvs[0].Value)
	fmt.Printf("++++++++++++++++++++++++++++++++++++ key: %s, value: %s ++++++++++++++++++++++++\n", key1, value)
}

func Watch() {
	<-ready

	etcd, err := getEtcdClient()
	if err != nil {
		fmt.Printf("failed to get etcd client: %v\n", err)
		return
	}

	ctx := context.Background()
	embedClient, err := embedetcd.GetEmbedEtcdClient(etcd.EtcdServer.Server)
	if err != nil {
		fmt.Printf("failed to get embed client of etcd: %v\n", err)
		return
	}

	fmt.Println("Starting watch...")
	watchChan := embedClient.Watch(ctx, key1)

	for {
		select {
		case event, ok := <-watchChan:
			if !ok {
				fmt.Println("Watch channel closed")
				return
			}

			if len(event.Events) == 0 {
				continue
			}

			for _, ev := range event.Events {
				fmt.Printf("================================  Watch event - key: %s, value: %s, type: %s =========================\n",
					string(ev.Kv.Key), string(ev.Kv.Value), ev.Type)
			}
		case <-ctx.Done():
			fmt.Println("Watch context cancelled")
			return
		}
	}
}

func main() {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		Server()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		Watch()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		Client()
	}()

	select {}
}
