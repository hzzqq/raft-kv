// raftstatus_test.go —— ShardMaster.RaftStatus() 直接单测。
// 跨机部署下每个 kvnode 进程只持有单一 ShardMaster 副本，其 RaftStatus()
// 是控制面健康快照的唯一一手来源（供 cluster.TCPNode.Diagnostics 暴露）。
// 该导出符号此前仅被 kvnode 跨包间接调用，静态覆盖率扫描无法识别间接覆盖，
// 故补一份直接单测钉死其契约，消除「高信号未覆盖导出符号」告警。

package shardmaster

import (
	"testing"

	"raftkv/src/raft"
)

// TestShardMasterRaftStatus 验证 RaftStatus() 返回本副本的只读共识快照，
// 字段自洽且类型稳定（跨机部署下被 cluster.NodeDiagnostics.Raft 直接持有）。
func TestShardMasterRaftStatus(t *testing.T) {
	cfg := makeSMConfig(t, 3)
	defer cfg.cleanup()

	for i := 0; i < cfg.n; i++ {
		rs := cfg.sm[i].RaftStatus()
		// Me 必须回指本副本编号，否则快照张冠李戴。
		if rs.Me != i {
			t.Fatalf("sm%d RaftStatus.Me = %d, want %d", i, rs.Me, i)
		}
		// Role 必须是合法的共识角色枚举值（Follower/Candidate/Leader）。
		switch rs.Role {
		case raft.Follower, raft.Candidate, raft.Leader:
		default:
			t.Fatalf("sm%d RaftStatus.Role = %v, want Follower/Candidate/Leader", i, rs.Role)
		}
		// 任期从 0 单调前进，不应为负。
		if rs.Term < 0 {
			t.Fatalf("sm%d RaftStatus.Term = %d, want >= 0", i, rs.Term)
		}
		// VotedFor 必须落在 [-1, n) —— -1 表示本任期未投票。
		if rs.VotedFor < -1 || rs.VotedFor >= cfg.n {
			t.Fatalf("sm%d RaftStatus.VotedFor = %d, want in [-1,%d)", i, rs.VotedFor, cfg.n)
		}
	}

	// 选出 leader 后，其快照必须自报 Leader 且 LeaderID 回指自身。
	// （follower 的 LeaderID 依赖心跳传播，属运行时收敛细节，不钉死在快照契约内。）
	li := cfg.leader()
	if li < 0 {
		t.Fatalf("no leader elected among %d replicas", cfg.n)
	}
	lr := cfg.sm[li].RaftStatus()
	if lr.Role != raft.Leader {
		t.Fatalf("expected leader sm%d to report Role=Leader, got %v", li, lr.Role)
	}
	if lr.LeaderID != li {
		t.Fatalf("leader sm%d RaftStatus.LeaderID = %d, want %d", li, lr.LeaderID, li)
	}
	if lr.Me != li {
		t.Fatalf("leader sm%d RaftStatus.Me = %d, want %d", li, lr.Me, li)
	}
}
