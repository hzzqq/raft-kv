package diagnostics

import (
	"fmt"

	"raftkv/src/raft"
	"raftkv/src/shardkv"
	"raftkv/src/shardmaster"
)

// SelfCheck 对一份配置历史（configs[0..n]，通常即 ShardMaster.configs）做端到端自检：
// 首份编号须为 0 且结构合法；此后每份须满足 IsValidTransition（编号严格 +1 且结构合法）。
// 任何一步失败都扣分并标注，便于运维在「配置链损坏 / 跳号 / 孤儿分片」时快速定位。
// 空历史直接判 0 分。纯函数、零副作用，可直接单测。
func SelfCheck(configs []shardmaster.Config) Diagnosis {
	issues := make([]string, 0)
	score := 100
	if len(configs) == 0 {
		return Diagnosis{Score: 0, Issues: []string{"empty config history"}}
	}
	prev := &configs[0]
	if prev.Num != 0 {
		issues = append(issues, fmt.Sprintf("first config num should be 0, got %d", prev.Num))
		score -= 20
	}
	if !prev.Valid() {
		issues = append(issues, "first config structure invalid")
		score -= 20
	}
	for i := 1; i < len(configs); i++ {
		cur := &configs[i]
		ok, why := prev.IsValidTransition(cur)
		if !ok {
			issues = append(issues, fmt.Sprintf("transition %d->%d invalid: %s", prev.Num, cur.Num, why))
			score -= 20
		}
		prev = cur
	}
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	if len(issues) == 0 {
		issues = []string{"ok"}
	}
	return Diagnosis{Score: score, Issues: issues}
}

// RaftCheck 对一个 Raft 节点的只读状态快照做不变量自检（纯函数、零副作用，可直接单测）：
//   - commitIndex > lastLogIndex → 已提交条目不在日志/快照中（状态机可能丢写），重扣；
//   - lastApplied > commitIndex  → apply 越过已提交边界（违反 Raft 安全性），重扣；
//   - leader 无多数派租约        → 仅提示（新当选瞬间或未宽限心跳丢失属正常，不扣分）。
//
// 与 SelfCheck（配置链）互补：前者管「配置演进」，本函数管「单节点运行时健康」，
// 二者共同让诊断包覆盖「配置 + 共识」两条链路（R2 隐性：此前诊断完全看不到 raft 状态，
// 脑裂/任期翻滚/apply 落后于 commit 时无一手信号）。
func RaftCheck(st raft.RaftStatus) Diagnosis {
	issues := make([]string, 0)
	score := 100
	if st.CommitIndex > st.LastLogIndex {
		issues = append(issues, fmt.Sprintf("commitIndex(%d) 超出日志末尾 lastLogIndex(%d)：已提交条目不在内存/快照日志中，可能丢写", st.CommitIndex, st.LastLogIndex))
		score -= 30
	}
	if st.LastApplied > st.CommitIndex {
		issues = append(issues, fmt.Sprintf("lastApplied(%d) 超过 commitIndex(%d)：apply 越过已提交边界，违反 Raft 安全性", st.LastApplied, st.CommitIndex))
		score -= 30
	}
	if st.Role == raft.Leader && !st.HasLeaderLease {
		// 新当选瞬间尚未建立租约属正常，仅提示不扣分。
		issues = append(issues, "leader 尚未与多数派建立租约（刚当选或心跳丢失），线性一致读将走 ReadIndex 慢路径")
	}
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	if len(issues) == 0 {
		issues = []string{"ok"}
	}
	return Diagnosis{Score: score, Issues: issues}
}

// ShardCheck 对单个副本的数据面（分片归属与迁移态）做不变量自检（纯函数、零副作用，
// 可直接单测）。它弥补了 RaftCheck（共识层）与 SelfCheck（配置链）都看不到的「数据面
// 迁移健康」盲区：卡死迁移、自相矛盾的分片态、重复分片，此前只能人肉读 /debug/shards
// JSON，无量化信号。检查项：
//   - pendingIn ∩ pendingOut 非空 → 同一分片既要收又要发，逻辑自相矛盾（重扣）；
//   - Owned 含重复分片号 → 状态机分片索引重复，属实现 bug（中扣）；
//   - StallSeconds 超阈值（默认 60s）→ 迁移卡滞，运维盲点（中扣）；
//     未超阈值但 >0 仅提示，不扣分（迁移刚发起属正常）。
func ShardCheck(st shardkv.ShardDebug) Diagnosis {
	issues := make([]string, 0)
	score := 100

	inSet := make(map[int]bool, len(st.PendingIn))
	for _, s := range st.PendingIn {
		inSet[s] = true
	}
	for _, s := range st.PendingOut {
		if inSet[s] {
			issues = append(issues, fmt.Sprintf("分片 %d 同时处于 pendingIn(待收) 与 pendingOut(待发)，状态自相矛盾", s))
			score -= 30
			break
		}
	}

	seen := make(map[int]bool, len(st.Owned))
	for _, s := range st.Owned {
		if seen[s] {
			issues = append(issues, fmt.Sprintf("Owned 含重复分片号 %d，状态机分片索引重复", s))
			score -= 10
			break
		}
		seen[s] = true
	}

	if st.StallSeconds > 60 {
		issues = append(issues, fmt.Sprintf("迁移卡滞 StallSeconds=%.0f > 60s，pending 分片长期未推进（可能迁移冻结）", st.StallSeconds))
		score -= 20
	} else if st.StallSeconds > 0 {
		issues = append(issues, fmt.Sprintf("迁移进行中 StallSeconds=%.0f（未达 60s 阈值，属正常窗口）", st.StallSeconds))
	}

	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	if len(issues) == 0 {
		issues = []string{"ok"}
	}
	return Diagnosis{Score: score, Issues: issues}
}
