// main.go —— raft-kv HTTP 网关启动入口
package main

import (
	"fmt"
	"net/http"
	"os"

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

	fmt.Printf("raft-kv gateway listening on %s (groups=%d)\n", addr, nGroups)
	if err := http.ListenAndServe(addr, s.Handler()); err != nil {
		fmt.Fprintln(os.Stderr, "gateway error:", err)
		os.Exit(1)
	}
}
