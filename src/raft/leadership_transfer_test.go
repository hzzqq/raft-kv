package raft

import (
	"testing"
	"time"
)

// TestLeadershipTransfer 验证领导权转移：当前 leader 调用 LeadershipTransfer 把领导权
// 平滑移交给指定 follower，转移后该 follower 成为新 leader（旧 leader 主动退位让路）。
func TestLeadershipTransfer(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers)
	defer cfg.cleanup()
	leader1, _ := cfg.checkOneLeader()

	// 选一个非 leader 的目标节点。
	target := (leader1 + 1) % servers
	if target == leader1 {
		target = (leader1 + 2) % servers
	}
	if !cfg.rafts[leader1].LeadershipTransfer(target) {
		t.Fatalf("LeadershipTransfer(%d) returned false", target)
	}

	// 等待转移完成（选举超时 + 心跳 + 余量）。
	time.Sleep(ElectionTimeoutMax + HeartbeatInterval + 300*time.Millisecond)
	leader2, _ := cfg.checkOneLeader()
	if leader2 != target {
		t.Fatalf("leadership did not transfer to target %d (got %d)", target, leader2)
	}
}
