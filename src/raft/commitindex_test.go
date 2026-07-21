package raft

import (
	"testing"
	"time"
)

// TestCommitIndexPersistenceRecovery 守护 cycle 87 末修复（n=21）：
// raft 现在把 commitIndex 纳入持久化，重启节点据恢复出的 commitIndex 立即把已提交
// 命令重放给状态机，而不必等新 leader 重发 LeaderCommit。
//
// 验证方式：先在 3 副本集群提交 n 条命令；杀掉一个 follower 并重启，随后断开其余副本
// 使其孤立（永远收不到 LeaderCommit）。若该副本仅凭持久化的 commitIndex 仍把前 n 条
// 已提交命令重放给状态机，则修复有效；若回归到「不持久化 commitIndex」，则孤立副本
// commitIndex 恒为 0、applier 永远等待，测试失败（已用负向验证确认无修复时 0/n 复现）。
func TestCommitIndexPersistenceRecovery(t *testing.T) {
	cfg := makeConfig(t, 3)
	defer cfg.cleanup()

	const n = 5
	for i := 0; i < n; i++ {
		cfg.one(i, 3)
	}

	leader := cfg.leader()
	if leader < 0 {
		t.Fatalf("no leader after initial agreement")
	}
	f := (leader + 1) % cfg.n

	cfg.kill(f)
	cfg.restart(f)

	// 断开其余副本：f 孤立，再也收不到任何 LeaderCommit —— 只能靠持久化的 commitIndex 重放。
	for i := 0; i < cfg.n; i++ {
		if i != f {
			cfg.disconnect(i)
		}
	}

	// 清空测试侧记录，强制「重放」而非复用重启前的旧记录。
	cfg.mu.Lock()
	cfg.logs[f] = cfg.logs[f][:0]
	cfg.mu.Unlock()

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		cfg.mu.Lock()
		sz := len(cfg.logs[f])
		cfg.mu.Unlock()
		if sz >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("isolated restarted follower %d only replayed %d/%d committed entries; commitIndex persistence/replay broken",
		f, len(cfg.logs[f]), n)
}
