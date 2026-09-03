package raft

import (
	"testing"
	"time"
)

// TestReadIndexLeaseRequiresCommittedCurrentTerm：端到端锁定 I195 的基石不变式。
//
// ShardKV 的线性一致读快路径（Get）与迁移传输守卫（GetShard）都依赖
// 「HasLeaderLease() && HasCommittedCurrentTerm()」为真，才敢基于 commitIndex 直接读本地
// 状态机 / 传出分片快照。该组合守卫的正确性，最终取决于 raft 层「committedCurrentTerm」语义：
//
//   (1) 新当选 leader 在「重新提交本任期 no-op」提交之前，committedCurrentTerm 必须恒为 false。
//       此窗口内即便 HasLeaderLease() 因首轮心跳已为 true（self 接触即算多数派接触），组合守卫
//       仍为 false——快路径不会基于过时偏低的 commitIndex 读到尚未 apply 的陈旧状态机（即 I195
//       修复的「写已提交却读到空」窗口）。
//   (2) 一旦新 leader 的 committedCurrentTerm 转 true（no-op 已提交），其 commitIndex 必已
//       "拉动"覆盖旧 leader 已提交的所有条目：ReadIndex() 返回的 commitIndex 必须 >= 旧 leader
//       稳定期的 commitIndex。该"拉动"保证是快路径读到的快照真正线性一致的前提。
//
// 任一性质被破坏都会在此暴露，从而在 raft 层（而非仅在 ShardKV 层）兜住 I195 窗口被悄然 reopen：
//   - advanceCommit 误把 committedCurrentTerm 提前置 true；
//   - becomeLeader 漏重置 committedCurrentTerm=false；
//   - persist 误持久化 committedCurrentTerm，致重启后假 true、跳过 no-op 提交窗口。
//
// 性质(2)是确定性硬断言（代码正确时必然成立，不 flaky）；性质(1)的窗口属时序相关，仅作信息性
// 观测（抓到则佐证守卫必要性，抓不到不影响通过），避免测试偶发失败。
func TestReadIndexLeaseRequiresCommittedCurrentTerm(t *testing.T) {
	cfg := makeConfig(t, 3)
	defer cfg.cleanup()

	// 1) 等稳定 leader，并等到它进入「租约有效 且 已提交当前任期」的稳态。
	l1, _ := cfg.checkOneLeader()
	rf1 := cfg.rafts[l1]
	deadline := time.Now().Add(5 * time.Second)
	stable := false
	for time.Now().Before(deadline) {
		if rf1.HasLeaderLease() && rf1.HasCommittedCurrentTerm() {
			stable = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !stable {
		t.Fatalf("初始 leader 未能进入 租约有效且已提交当前任期 的稳态")
	}
	oldCommit, _ := rf1.ReadIndex()

	// 2) 杀掉稳定 leader，触发重新选举（其余 2 节点仍在线）。
	cfg.kill(l1)

	// 3) 观测新 leader 选举窗口 + 验证"拉动"安全性质。
	newLeader := -1
	sawLeaseWithoutCurrentTerm := false
	obsDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(obsDeadline) {
		if newLeader < 0 {
			if id, _ := cfg.checkOneLeader(); id != l1 && id >= 0 {
				newLeader = id
			}
		}
		if newLeader >= 0 {
			rf2 := cfg.rafts[newLeader]
			lease := rf2.HasLeaderLease()
			committed := rf2.HasCommittedCurrentTerm()
			if lease && !committed {
				// I195 守卫必须在此窗口拦住快路径：组合守卫为 false。
				sawLeaseWithoutCurrentTerm = true
			}
			if committed {
				// 新 leader 已提交本任期 no-op：验证 commitIndex 拉动保证。
				newCommit, _ := rf2.ReadIndex()
				if newCommit < oldCommit {
					t.Fatalf("新 leader 提交当前任期后 commitIndex=%d < 旧 leader 稳定 commitIndex=%d，"+
						"I195 拉动保证被破坏：快路径会基于过时 commitIndex 读陈旧状态机", newCommit, oldCommit)
				}
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLeader < 0 {
		t.Fatalf("杀主后未能在观察窗口内选出新 leader")
	}
	t.Logf("election-jitter window observed (HasLeaderLease=true && HasCommittedCurrentTerm=false): %v", sawLeaseWithoutCurrentTerm)
}
