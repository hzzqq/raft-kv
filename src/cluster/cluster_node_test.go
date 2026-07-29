// cluster_node_test.go —— I21/I22 回归：单节点启动（StartNodeTCP）+ 纯客户端接入
// （ConnectTCP）在真实 TCP 上组成完整集群并正确服务读写与容错。
//
// 与 cluster_tcp_test.go 的区别：那边走 StartClusterTCP（一个 Cluster 对象内起全部
// 节点，节点间共享 make_end 连接缓存）；这里每个节点是**独立的 TCPNode 实例**，各持
// 各的出向连接与状态机——除了同在一个 OS 进程，拓扑上与「每台机器一个进程」完全一致。
// OS 进程级实测见 scripts/cross_machine_test.py。
package cluster

import (
	"fmt"
	"net"
	"testing"
	"time"

	"raftkv/src/shardmaster"
)

// freePorts 向内核申请 n 个空闲端口（listen :0 再关闭）。理论上有复用竞态，
// 本测试串行运行且立即复用，实际足够稳。
func freePorts(t *testing.T, n int) []int {
	t.Helper()
	ports := make([]int, n)
	liss := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("freePorts: %v", err)
		}
		liss[i] = lis
		ports[i] = lis.Addr().(*net.TCPAddr).Port
	}
	for _, lis := range liss {
		lis.Close()
	}
	return ports
}

// TestNodePerProcessCluster 用 9 个独立 TCPNode（3 SM + 2 group × 3 副本）拼出集群，
// ConnectTCP 远程客户端做 Join + 读写，然后停掉每组一个少数派副本验证仍可服务。
func TestNodePerProcessCluster(t *testing.T) {
	const nGroups, nReplicas, nSM = 2, 3, 3
	total := nSM + nGroups*nReplicas
	ports := freePorts(t, total)

	cfg := ClusterTCPConfig{NGroups: nGroups, NReplicas: nReplicas, NSM: nSM}
	idx := 0
	for j := 0; j < nSM; j++ {
		cfg.Nodes = append(cfg.Nodes, TCPNodeAddr{Name: fmt.Sprintf("m%d", j), Addr: fmt.Sprintf("127.0.0.1:%d", ports[idx])})
		idx++
	}
	for g := 0; g < nGroups; g++ {
		for r := 0; r < nReplicas; r++ {
			cfg.Nodes = append(cfg.Nodes, TCPNodeAddr{Name: fmt.Sprintf("g%d-%d", g, r), Addr: fmt.Sprintf("127.0.0.1:%d", ports[idx])})
			idx++
		}
	}

	// 逐节点独立启动（等价于 9 个 kvnode 进程）。
	nodes := make(map[string]*TCPNode, total)
	for _, na := range cfg.Nodes {
		n, err := StartNodeTCP(cfg, na.Name)
		if err != nil {
			t.Fatalf("StartNodeTCP(%s): %v", na.Name, err)
		}
		nodes[na.Name] = n
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	// 纯客户端接入。
	c, err := ConnectTCP(cfg)
	if err != nil {
		t.Fatalf("ConnectTCP: %v", err)
	}
	defer c.Cleanup()
	if !c.Remote() {
		t.Fatalf("ConnectTCP 应返回远程视图 (Remote()=true)")
	}

	// 远程探针：SM 集群选出 leader 后应能报出配置号（初始配置 num=0）。
	deadline := time.Now().Add(15 * time.Second)
	for c.SMLatestConfigNum() < 0 {
		if time.Now().After(deadline) {
			t.Fatalf("SM 集群 15s 内未选出 leader")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Join 两个 group（走远程 WaitConfig：SM 配置号推进即视为发布完成）。
	done := make(chan struct{})
	go func() {
		for g := 0; g < nGroups; g++ {
			c.Join(g)
			c.WaitConfig(g, 0, g+1)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("Join/WaitConfig 30s 超时")
	}

	// 读写正确性：覆盖所有分片。
	ck := c.Clerk()
	putDone := make(chan struct{})
	go func() {
		for i := 0; i < 2*shardmaster.NShards; i++ {
			ck.Put(fmt.Sprintf("xk-%d", i), fmt.Sprintf("v%d", i))
		}
		close(putDone)
	}()
	select {
	case <-putDone:
	case <-time.After(60 * time.Second):
		t.Fatalf("Put x%d 60s 超时", 2*shardmaster.NShards)
	}

	// 容错：每组停掉一个副本（少数派），读写必须仍然成功。
	nodes["g0-2"].Stop()
	nodes["g1-2"].Stop()
	delete(nodes, "g0-2")
	delete(nodes, "g1-2")

	verifyDone := make(chan string, 1)
	go func() {
		for i := 0; i < 2*shardmaster.NShards; i++ {
			k := fmt.Sprintf("xk-%d", i)
			if got, want := ck.Get(k), fmt.Sprintf("v%d", i); got != want {
				verifyDone <- fmt.Sprintf("key %s: got %q want %q", k, got, want)
				return
			}
		}
		ck.Append("xk-0", "|tail")
		if got := ck.Get("xk-0"); got != "v0|tail" {
			verifyDone <- fmt.Sprintf("append 后 xk-0: got %q want %q", got, "v0|tail")
			return
		}
		verifyDone <- ""
	}()
	select {
	case msg := <-verifyDone:
		if msg != "" {
			t.Fatal(msg)
		}
	case <-time.After(90 * time.Second):
		t.Fatalf("少数派宕机后读写验证 90s 超时")
	}
}
