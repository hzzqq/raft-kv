package shardkv

import (
	"testing"
	"time"

	"raftkv/src/raft"
)

// TestShardKVRaftStatus 集成级验证 ShardKV.RaftStatus() 把底层 Raft 节点只读快照透出，
// 与 cycle #220 的 raft 健康能力对齐：leader 的 LeaderID 应等于自身 me，follower 认知的
// leader 应等于 leader 的 me。用真实集群底座（makeSKVConfig），与仓库既有 shardkv 测试风格一致。
func TestShardKVRaftStatus(t *testing.T) {
	cfg := makeSKVConfig(t, 1, 3, 3, 0)
	defer cfg.cleanup()
	cfg.joinGroup(0)
	cfg.waitGroupConfig(0, 0, 1)

	leader := cfg.leaderOf(0)
	if leader < 0 {
		t.Fatalf("no leader elected")
	}
	leaderSt := cfg.groups[0][leader].RaftStatus()
	if leaderSt.Role != raft.Leader {
		t.Fatalf("leader 副本 Role 应为 Leader，实际 %v", leaderSt.Role)
	}
	if leaderSt.LeaderID != leaderSt.Me {
		t.Fatalf("role=Leader 时 LeaderID 应等于自身 me=%d，实际 %d", leaderSt.Me, leaderSt.LeaderID)
	}
	if !leaderSt.HasLeaderLease {
		t.Fatalf("稳定 leader 应已建立多数派租约")
	}

	// follower 应认知到 leader：等待 AppendEntries 刷新 leaderId 后断言。
	follower := (leader + 1) % 3
	deadline := time.Now().Add(3 * time.Second)
	var fSt raft.RaftStatus
	for time.Now().Before(deadline) {
		fSt = cfg.groups[0][follower].RaftStatus()
		if fSt.LeaderID == leaderSt.Me {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if fSt.Role == raft.Leader {
		t.Fatalf("副本 %d 不应是 leader（leader 为 %d）", follower, leader)
	}
	if fSt.LeaderID != leaderSt.Me {
		t.Fatalf("follower 认知的 LeaderID 应为 leader 的 me=%d，实际 %d（role=%v）", leaderSt.Me, fSt.LeaderID, fSt.Role)
	}
}
