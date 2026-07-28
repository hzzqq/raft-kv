package raft

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestSetElectionTimeoutFn 验证可注入的选举超时生成器：
// 1) 注入后重置计时器走注入函数（调用计数增长）；
// 2) 注入超短超时的节点应率先发起选举成为 leader（时序可控性证明）；
// 3) 传 nil 恢复默认随机区间不崩溃、集群仍有唯一 leader。
func TestSetElectionTimeoutFn(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers)
	defer cfg.cleanup()

	var calls0 atomic.Int64
	// 节点 0 注入超短超时（80ms 远小于其他节点注入的 2s），应率先当选。
	cfg.rafts[0].SetElectionTimeoutFn(func() time.Duration {
		calls0.Add(1)
		return 80 * time.Millisecond
	})
	for i := 1; i < servers; i++ {
		cfg.rafts[i].SetElectionTimeoutFn(func() time.Duration { return 2 * time.Second })
	}

	deadline := time.Now().Add(3 * time.Second)
	won := false
	for time.Now().Before(deadline) {
		if _, isLeader := cfg.rafts[0].GetState(); isLeader {
			won = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !won {
		t.Fatal("注入超短选举超时的节点 0 应当选 leader")
	}
	if calls0.Load() == 0 {
		t.Fatal("注入的超时函数应被调用")
	}

	// 恢复默认随机区间：集群应保持/重新收敛出唯一 leader。
	for i := 0; i < servers; i++ {
		cfg.rafts[i].SetElectionTimeoutFn(nil)
	}
	cfg.checkOneLeader()
}
