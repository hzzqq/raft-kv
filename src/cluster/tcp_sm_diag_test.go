package cluster

import (
	"fmt"
	"net"
	"testing"
	"time"

	"raftkv/src/raft"
	"raftkv/src/shardmaster"
)

// allocAddrs 预分配 n 个 127.0.0.1 临时端口地址（与 TestClusterTCPTransport 同法）。
func allocAddrs(t *testing.T, names []string) []TCPNodeAddr {
	t.Helper()
	var addrs []TCPNodeAddr
	var ls []net.Listener
	for _, n := range names {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("alloc listen for %s: %v", n, err)
		}
		ls = append(ls, l)
		addrs = append(addrs, TCPNodeAddr{Name: n, Addr: l.Addr().String()})
	}
	for _, l := range ls {
		_ = l.Close()
	}
	return addrs
}

// TestTCPSMElection 分层诊断第 1 层：只起 3 个 ShardMaster over TCP（nGroups=0），
// 进程内直接调 sm.Query（内部即 rf.GetState）轮询，验证 raft 在真实 TCP 上能否选出 leader。
// 若本测试失败 → 问题在 raft 选举/复制跨 TCP；若通过 → 问题在 clerk 的 TCP 客户端路径。
func TestTCPSMElection(t *testing.T) {
	nSM := 3
	names := make([]string, 0, nSM)
	for j := 0; j < nSM; j++ {
		names = append(names, fmt.Sprintf("m%d", j))
	}
	addrs := allocAddrs(t, names)

	c, err := StartClusterTCP(0, 0, nSM, 0,
		func(string, int, int) raft.Persister { return raft.MakeEmptyPersister() }, addrs)
	if err != nil {
		t.Fatalf("StartClusterTCP: %v", err)
	}
	defer c.Cleanup()

	// 进程内直接探测：最长 10s 内应有恰好 1 个 SM 自认 leader 且 Query 返回 OK。
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		leaders := 0
		for j := 0; j < nSM; j++ {
			reply := &shardmaster.QueryReply{}
			c.SM[j].Query(&shardmaster.QueryArgs{Num: -1}, reply)
			if reply.Err == shardmaster.OK {
				leaders++
			}
		}
		if leaders == 1 {
			t.Logf("TCP SM 集群选主成功（进程内探测）")
			// 第 2 层：leader 已存在，改走真实 TCP 客户端路径查同一个 Query。
			ck := shardmaster.MakeClerk(c.SMNames, c.make_end)
			done := make(chan shardmaster.Config, 1)
			go func() { done <- ck.Query(-1) }()
			select {
			case cfg := <-done:
				t.Logf("TCP clerk Query 成功: Num=%d", cfg.Num)
				return
			case <-time.After(15 * time.Second):
				t.Fatalf("第 2 层失败：进程内有 leader，但 clerk 走 TCP 的 Query 15s 不返回 → 问题在 TCP 客户端/服务端路径")
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("第 1 层失败：10s 内 TCP SM 集群未选出 leader → 问题在 raft 选举/复制跨 TCP")
}
