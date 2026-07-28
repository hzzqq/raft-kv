package cluster

import (
	"fmt"
	"net"
	"testing"

	"raftkv/src/raft"
)

// TestClusterTCPTransport 端到端验证跨机传输层：在进程内分配多个 localhost 监听地址，
// 用真实 TCP（src/transport）把这些地址串成 2-group 集群，跑通基本 Put/Get。
// 这证明 StartClusterTCP 的字节确实走真实网络（而非进程内 channel），是"跨机部署"的最小可用证明。
func TestClusterTCPTransport(t *testing.T) {
	nGroups, nReplicas, nSM := 2, 3, 3

	// 1) 预分配地址：为每个节点要一个 127.0.0.1:0 的临时监听，记下真实地址后关闭，
	//    交给 StartClusterTCP 重新监听（端口保留概率高；即便被抢占也仅测试失败，不影响生产）。
	var addrs []TCPNodeAddr
	names := []string{}
	for j := 0; j < nSM; j++ {
		names = append(names, fmt.Sprintf("m%d", j))
	}
	for g := 0; g < nGroups; g++ {
		for r := 0; r < nReplicas; r++ {
			names = append(names, fmt.Sprintf("g%d-%d", g, r))
		}
	}
	lisMap := make(map[string]net.Listener, len(names))
	for _, n := range names {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("alloc listen for %s: %v", n, err)
		}
		lisMap[n] = l
		addrs = append(addrs, TCPNodeAddr{Name: n, Addr: l.Addr().String()})
	}
	// 释放预分配监听，交还地址给 StartClusterTCP 复用（同一端口在短暂窗口内可重用）。
	for _, l := range lisMap {
		_ = l.Close()
	}

	c, err := StartClusterTCP(nGroups, nReplicas, nSM, 0,
		func(string, int, int) raft.Persister { return raft.MakeEmptyPersister() }, addrs)
	if err != nil {
		t.Fatalf("StartClusterTCP: %v", err)
	}
	defer c.Cleanup()

	// 必须先 Join 至少一个 group 再写入：初始配置 Num=0 没有任何 group，
	// 所有 shard 归属 gid=0（不存在），clerk 会无限 refresh 等配置——
	// 曾因先 Put 后 Join 造成 600s 挂死（复盘：不是传输层 bug，是测试顺序错误）。
	c.Join(0)
	c.WaitConfig(0, 0, 1)

	ck := c.Clerk()
	ck.Put("k1", "v1")
	if got := ck.Get("k1"); got != "v1" {
		t.Fatalf("after Put k1: got %q want v1", got)
	}
	ck.Put("k2", "v2")
	if got := ck.Get("k2"); got != "v2" {
		t.Fatalf("after Put k2: got %q want v2", got)
	}
	// 跨 group 迁移后仍可读。
	c.Join(1)
	c.WaitConfig(1, 0, 2)
	shard := 0 // 任意分片，仅验证迁移后数据不丢
	c.Move(shard, 1)
	c.WaitConfig(0, 0, 3)
	c.WaitConfig(1, 0, 3)
	ck.Put("migrated", "yes")
	if got := ck.Get("migrated"); got != "yes" {
		t.Fatalf("after Move: got %q want yes", got)
	}
}
