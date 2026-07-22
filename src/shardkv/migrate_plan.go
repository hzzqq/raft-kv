package shardkv

import "sort"

// MigrationStep 是单步迁移计划：把分片 Shard 从 From group 迁到 To group。
// From 为当前属主（可能为 0 表示此前未分配），To 为目标属主；From==To 不出现在步骤中。
type MigrationStep struct {
	Shard int
	From  int
	To    int
}

// MigrationPlan 是一组使分片分布趋向均衡（各 gid 负载差 ≤1）的迁移步骤。
// 纯函数：输入不变量、输出确定，不修改任何外部状态，可安全用于预览/审计/单测。
type MigrationPlan struct {
	Steps []MigrationStep
	// Target 是规划后的目标分布（与 Config.Shards 同构），可直接用于对比当前或落配置。
	Target [NShards]int
	Moved  int // 实际移动（From!=To）的分片数
}

// PlanRebalance 依据当前分片分布 current 与有效 gid 集合 gids，计算一组迁移步骤使
// 分布均衡（各 gid 负载差 ≤1，即前 extra 个 gid 比其余多 1）。
//   - gids 为空：所有分片标记为回收（Target 全 0，To=0 表示无属主），Moved=NShards。
//   - 仅保留当前仍有效的 gid 下、且未超出目标配额的碎片；超额碎片与被指向失效 gid
//     的碎片，按「当前负载最低且未达标」的贪心策略重分配给有效 gid。
//
// 该规划与 shardmaster.rebalance（就地变更 Config）互补：本函数只读、可预览，且给出
// 明确的 From/To 步骤，便于运维在真正提交配置前评估迁移代价、做 dry-run 审计。
// cluster-free：不触碰 raft / 集群，纯数组运算，确定性可单测。
func PlanRebalance(current [NShards]int, gids []int) MigrationPlan {
	plan := MigrationPlan{Steps: make([]MigrationStep, 0, NShards)}

	sorted := append([]int(nil), gids...)
	sort.Ints(sorted)
	ng := len(sorted)
	idxOf := make(map[int]int, ng)
	for i, g := range sorted {
		idxOf[g] = i
	}

	// 目标负载：前 extra 个各 base+1，其余 base（差 ≤1）。
	base := NShards / maxInt(ng, 1)
	extra := NShards % maxInt(ng, 1)
	target := make([]int, ng)
	for i := 0; i < ng; i++ {
		if i < extra {
			target[i] = base + 1
		} else {
			target[i] = base
		}
	}

	// kept[gi]：已决定保留在 gid 上的碎片数（用于判定超额）。
	kept := make([]int, ng)
	for s := 0; s < NShards; s++ {
		cur := current[s]
		gi, validCur := idxOf[cur]
		if ng > 0 && validCur && kept[gi] < target[gi] {
			kept[gi]++ // 该 gid 仍有配额，碎片原地保留，不移动
			plan.Target[s] = cur
			continue
		}
		if ng == 0 {
			// 无有效 group：回收所有碎片。
			plan.Steps = append(plan.Steps, MigrationStep{Shard: s, From: cur, To: 0})
			plan.Target[s] = 0
			plan.Moved++
			continue
		}
		// 需要为碎片 s 选一个负载最低且未达标的 gid（deficit 最大，并列取最小索引）。
		best := -1
		bestDeficit := -1
		for gi := 0; gi < ng; gi++ {
			deficit := target[gi] - kept[gi]
			if deficit <= 0 {
				continue
			}
			if deficit > bestDeficit {
				bestDeficit = deficit
				best = gi
			}
		}
		// best 必然存在：ng>0 时总目标负载=base*ng+extra=NShards，待重分配碎片数
		// （失效+超额）恰等于各 gid 累计 deficit，循环不变量保证。
		to := sorted[best]
		plan.Steps = append(plan.Steps, MigrationStep{Shard: s, From: cur, To: to})
		kept[best]++
		plan.Target[s] = to
		plan.Moved++
	}
	return plan
}

// maxInt 返回两整数较大者（避免依赖具体 Go 版本的 builtin max）。
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
