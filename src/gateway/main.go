// main.go —— raft-kv HTTP 网关启动入口
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"raftkv/src/cluster"
)

func main() {
	nGroups := 2
	addr := ":8080"
	dataDir := os.Getenv("RAFT_KV_DATA_DIR")
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	if len(os.Args) > 2 {
		dataDir = os.Args[2]
	}

	var c *cluster.Cluster
	if dataDir != "" {
		// 真部署化：把每个节点的 Raft 状态/快照落盘，进程崩溃重启复用同一目录即可恢复。
		c = cluster.StartClusterWithPersister(nGroups, 3, 3, 0, cluster.FilePersisterFactory(dataDir))
		fmt.Printf("raft-kv gateway 持久化目录: %s\n", dataDir)
	} else {
		c = cluster.StartCluster(nGroups, 3, 3, 0)
	}
	defer c.Cleanup()

	s := NewServer(c)
	s.Init(nGroups)

	srv := &http.Server{Addr: addr, Handler: s.Handler()}
	s.SetHTTPServer(srv)

	// 优雅退出：捕获 SIGINT/SIGTERM，先等待在途请求完成、再关闭监听，最后 defer 清理集群。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("raft-kv gateway listening on %s (groups=%d)\n", addr, nGroups)
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
