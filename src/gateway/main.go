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
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	c := cluster.StartCluster(nGroups, 3, 3, 0)
	defer c.Cleanup()

	s := NewServer(c)
	s.Init(nGroups)

	srv := &http.Server{Addr: addr, Handler: s.Handler()}

	// 优雅退出：捕获 SIGINT/SIGTERM，先关闭监听、再给在途请求 5s 宽限，最后 defer 清理集群。
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
	if err := srv.Shutdown(shutdownCtx); err != nil {
		fmt.Fprintln(os.Stderr, "gateway shutdown error:", err)
	}
}
