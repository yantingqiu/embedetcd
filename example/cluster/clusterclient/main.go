package main

import (
	"context"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/yantingqiu/embedetcd"
)

func main() {
	baseDir := "./etcd-cluster-data"
	os.RemoveAll(baseDir)
	os.MkdirAll(baseDir, 0755)

	// 定义集群配置
	clusterToken := "etcd-cluster-token"
	initialCluster := "node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2381,node3=http://127.0.0.1:2382"

	node1 := embedetcd.NewEmbedEtcd(
		embedetcd.WithName("node1"),
		embedetcd.WithDataDir(filepath.Join(baseDir, "node1")),
		embedetcd.WithListenPeerURLs(mustParseURL("http://0.0.0.0:2380")),
		embedetcd.WithAdvertisePeerURLs(mustParseURL("http://127.0.0.1:2380")),
		embedetcd.WithListenClientURLs(mustParseURL("http://0.0.0.0:2379")),
		embedetcd.WithAdvertiseClientURLs(mustParseURL("http://127.0.0.1:2379")),
		embedetcd.WithInitialCluster(initialCluster),
		embedetcd.WithClusterState("new"),
		embedetcd.WithInitialClusterToken(clusterToken),
	)

	node2 := embedetcd.NewEmbedEtcd(
		embedetcd.WithName("node2"),
		embedetcd.WithDataDir(filepath.Join(baseDir, "node2")),
		embedetcd.WithListenPeerURLs(mustParseURL("http://0.0.0.0:2381")),
		embedetcd.WithAdvertisePeerURLs(mustParseURL("http://127.0.0.1:2381")),
		embedetcd.WithListenClientURLs(mustParseURL("http://0.0.0.0:2378")),
		embedetcd.WithAdvertiseClientURLs(mustParseURL("http://127.0.0.1:2378")),
		embedetcd.WithInitialCluster(initialCluster),
		embedetcd.WithClusterState("new"),
		embedetcd.WithInitialClusterToken(clusterToken),
	)

	node3 := embedetcd.NewEmbedEtcd(
		embedetcd.WithName("node3"),
		embedetcd.WithDataDir(filepath.Join(baseDir, "node3")),
		embedetcd.WithListenPeerURLs(mustParseURL("http://0.0.0.0:2382")),
		embedetcd.WithAdvertisePeerURLs(mustParseURL("http://127.0.0.1:2382")),
		embedetcd.WithListenClientURLs(mustParseURL("http://0.0.0.0:2377")),
		embedetcd.WithAdvertiseClientURLs(mustParseURL("http://127.0.0.1:2377")),
		embedetcd.WithInitialCluster(initialCluster),
		embedetcd.WithClusterState("new"),
		embedetcd.WithInitialClusterToken(clusterToken),
	)

	// 同时启动所有节点
	var wg sync.WaitGroup
	errors := make(chan error, 3)

	wg.Add(3)

	go func() {
		defer wg.Done()
		log.Println("------------------------------ Starting node1 ----------------------------")
		if err := node1.Start(); err != nil {
			log.Printf("failed to start node1: %v", err)
			errors <- err
		} else {
			log.Println("-------------------------- Start node1 success ---------------------------")
		}
	}()

	go func() {
		defer wg.Done()
		log.Println("------------------------------ Starting node2 ----------------------------")
		if err := node2.Start(); err != nil {
			log.Printf("failed to start node2: %v", err)
			errors <- err
		} else {
			log.Println("-------------------------- Start node2 success ---------------------------")
		}
	}()

	go func() {
		defer wg.Done()
		log.Println("------------------------------ Starting node3 ----------------------------")
		if err := node3.Start(); err != nil {
			log.Printf("failed to start node3: %v", err)
			errors <- err
		} else {
			log.Println("-------------------------- Start node3 success ---------------------------")
		}
	}()

	wg.Wait()
	close(errors)

	hasError := false
	for err := range errors {
		if err != nil {
			hasError = true
			log.Printf("Startup error: %v", err)
		}
	}

	if hasError {
		log.Fatal("Cluster startup failed")
	}

	time.Sleep(3 * time.Second)

	testCluster([]string{
		"http://127.0.0.1:2379",
		"http://127.0.0.1:2378",
		"http://127.0.0.1:2377",
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("close cluster")
	node1.Stop()
	node2.Stop()
	node3.Stop()
	log.Println("cluster has been closed")
}

func mustParseURL(rawURL string) url.URL {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	return *u
}

func testCluster(endpoints []string) {
	client, err := embedetcd.GetClusterClient(endpoints)
	if err != nil {
		log.Fatalf("failed to create cluster client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.Put(ctx, "/test-key", "test-value")
	if err != nil {
		log.Printf("put value failed: %v", err)
		return
	}

	resp, err := client.Get(ctx, "/test-key")
	if err != nil {
		log.Printf("get value failed: %v", err)
		return
	}

	if len(resp.Kvs) > 0 {
		log.Printf("get value success: key=%s, value=%s", resp.Kvs[0].Key, resp.Kvs[0].Value)
	}

	memberResp, err := client.MemberList(ctx)
	if err != nil {
		log.Printf("get cluster member failed: %v", err)
		return
	}

	log.Printf("cluster member: %d", len(memberResp.Members))
	for _, member := range memberResp.Members {
		log.Printf("  - ID: %x, Name: %s, ClientURLs: %v", member.ID, member.Name, member.ClientURLs)
	}
}
