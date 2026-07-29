// main.go —— raft-kv HTTP 网关启动入口
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"raftkv/src/cluster"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP 网关监听地址")
	dataDir := flag.String("data-dir", "", "节点状态落盘目录（空=内存）")
	tcpCfg := flag.String("tcp-config", "", "跨机部署节点地址清单 JSON（本进程起全部节点，节点间走真实 TCP）")
	connectCfg := flag.String("connect", "", "纯客户端接入已运行的跨机集群（节点由 kvnode 进程各自承载），值为同一份部署 JSON")
	flag.Parse()

	var c *cluster.Cluster
	switch {
	case *connectCfg != "":
		// 真·跨机：节点在别处（kvnode 进程/其他机器），本进程只做 HTTP↔Clerk 翻译。
		cfg, err := cluster.LoadTCPConfig(*connectCfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "load connect config:", err)
			os.Exit(1)
		}
		c, err = cluster.ConnectTCP(cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "connect cluster:", err)
			os.Exit(1)
		}
		fmt.Printf("raft-kv gateway 远程接入模式: %s (groups=%d)\n", *connectCfg, len(c.Groups))
	case *tcpCfg != "":
		// 跨机部署：节点间 RPC 走真实 TCP（src/transport），可分布到不同进程/机器。
		cfg, err := cluster.LoadTCPConfig(*tcpCfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "load tcp config:", err)
			os.Exit(1)
		}
		c, err = cluster.StartClusterFromConfig(cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "start cluster:", err)
			os.Exit(1)
		}
		fmt.Printf("raft-kv gateway 跨机模式: %s (groups=%d)\n", *tcpCfg, len(c.Groups))
	case *dataDir != "":
		// 真部署化：把每个节点的 Raft 状态/快照落盘，进程崩溃重启复用同一目录即可恢复。
		c = cluster.StartClusterWithPersister(2, 3, 3, 0, cluster.FilePersisterFactory(*dataDir))
		fmt.Printf("raft-kv gateway 持久化目录: %s\n", *dataDir)
	default:
		c = cluster.StartCluster(2, 3, 3, 0)
	}
	defer c.Cleanup()

	nGroups := len(c.Groups)
	s := NewServer(c)
	s.Init(nGroups)

	srv := &http.Server{Addr: *addr, Handler: s.Handler()}
	s.SetHTTPServer(srv)

	// 优雅退出：捕获 SIGINT/SIGTERM，先等待在途请求完成、再关闭监听，最后 defer 清理集群。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("raft-kv gateway listening on %s (groups=%d)\n", *addr, nGroups)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintln(os.Stderr, "gateway error:", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	fmt.Println("\n>> 收到终止信号，优雅关闭网关中...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(shutdownCtx); err != nil {
		fmt.Fprintln(os.Stderr, "gateway shutdown error:", err)
	}
}
