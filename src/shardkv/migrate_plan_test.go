package shardkv

import "testing"

// fillShards 构造 [NShards]int：前 n 个分片赋 g，其余赋 0。
func fillShards(g, n int) [NShards]int {
	var cur [NShards]int
	for i := 0; i < n && i < NShards; i++ {
		cur[i] = g
	}
	return cur
}

// countTargets 统计 Target 中各 gid 的出现次数。
func countTargets(t [NShards]int) map[int]int {
	m := map[int]int{}
	for _, g := range t {
		m[g]++
	}
	return m
}

// TestPlanRebalanceAllOnOne 验证：全部分片集中在一个 gid → 迁一半到另一 gid，达 5/5。
func TestPlanRebalanceAllOnOne(t *testing.T) {
	cur := fillShards(1, NShards) // 10 个全在 gid1
	plan := PlanRebalance(cur, []int{1, 2})
	if plan.Moved != NShards/2 {
		t.Fatalf("期望移动 %d 个，实际 %d", NShards/2, plan.Moved)
	}
	c := countTargets(plan.Target)
	if c[1] != 5 || c[2] != 5 {
		t.Fatalf("期望 5/5 均衡，实际 %v", c)
	}
}

// TestPlanRebalanceAlreadyBalanced 验证：已均衡 → 零移动、Target 与 current 一致。
func TestPlanRebalanceAlreadyBalanced(t *testing.T) {
	var cur [NShards]int
	for i := 0; i < 5; i++ {
		cur[i] = 1
	}
	for i := 5; i < 10; i++ {
		cur[i] = 2
	}
	plan := PlanRebalance(cur, []int{1, 2})
	if plan.Moved != 0 || len(plan.Steps) != 0 {
		t.Fatalf("已均衡应零移动，实际 Moved=%d Steps=%d", plan.Moved, len(plan.Steps))
	}
	if plan.Target != cur {
		t.Fatalf("Target 应与 current 一致，实际 %v", plan.Target)
	}
}

// TestPlanRebalanceUneven 验证：7/3 失衡 → 移动 2 个达 5/5。
func TestPlanRebalanceUneven(t *testing.T) {
	cur := fillShards(1, 7)
	for i := 7; i < 10; i++ {
		cur[i] = 2
	}
	plan := PlanRebalance(cur, []int{1, 2})
	if plan.Moved != 2 {
		t.Fatalf("期望移动 2 个，实际 %d", plan.Moved)
	}
	c := countTargets(plan.Target)
	if c[1] != 5 || c[2] != 5 {
		t.Fatalf("期望 5/5，实际 %v", c)
	}
}

// TestPlanRebalanceInvalidGid 验证：部分分片指向失效 gid（不在 gids 中）被回收重分配。
func TestPlanRebalanceInvalidGid(t *testing.T) {
	cur := fillShards(1, 8) // 8 个在 gid1
	cur[8], cur[9] = 99, 99 // 2 个指向失效 gid99
	plan := PlanRebalance(cur, []int{1, 2})
	// gid1 保留 5，超额 3 + 失效 2 = 5 个迁到 gid2 → 5/5。
	if plan.Moved != 5 {
		t.Fatalf("期望移动 5 个，实际 %d", plan.Moved)
	}
	c := countTargets(plan.Target)
	if c[1] != 5 || c[2] != 5 {
		t.Fatalf("期望 5/5，实际 %v", c)
	}
	// 所有迁移步骤的 To 必为有效 gid（1 或 2），不会指向失效 gid。
	for _, s := range plan.Steps {
		if s.To != 1 && s.To != 2 {
			t.Fatalf("迁移目标含失效 gid：%v", s)
		}
	}
}

// TestPlanRebalanceEmptyGids 验证：无有效 group → 全部回收（Target 全 0，Moved=NShards）。
func TestPlanRebalanceEmptyGids(t *testing.T) {
	cur := fillShards(1, NShards)
	plan := PlanRebalance(cur, nil)
	if plan.Moved != NShards {
		t.Fatalf("期望移动 %d 个（全回收），实际 %d", NShards, plan.Moved)
	}
	c := countTargets(plan.Target)
	if c[0] != NShards {
		t.Fatalf("期望 Target 全 0，实际 %v", c)
	}
	for _, s := range plan.Steps {
		if s.To != 0 {
			t.Fatalf("回收步骤 To 应为 0，实际 %d", s.To)
		}
	}
}

// TestPlanRebalanceSingleGid 验证：单 gid 且分片全在其上 → 零移动。
func TestPlanRebalanceSingleGid(t *testing.T) {
	cur := fillShards(1, NShards)
	plan := PlanRebalance(cur, []int{1})
	if plan.Moved != 0 {
		t.Fatalf("单 gid 全在其上应零移动，实际 %d", plan.Moved)
	}
	c := countTargets(plan.Target)
	if c[1] != NShards {
		t.Fatalf("期望全在 gid1，实际 %v", c)
	}
}

// TestPlanRebalanceSingleGidWithInvalid 验证：单 gid 但有失效碎片 → 仅回收失效碎片。
func TestPlanRebalanceSingleGidWithInvalid(t *testing.T) {
	cur := fillShards(1, 9)
	cur[9] = 7 // 失效，不在 gids{1} 中
	plan := PlanRebalance(cur, []int{1})
	if plan.Moved != 1 {
		t.Fatalf("期望仅移动 1 个失效碎片，实际 %d", plan.Moved)
	}
	c := countTargets(plan.Target)
	if c[1] != NShards {
		t.Fatalf("期望全部归 gid1，实际 %v", c)
	}
}

// TestPlanRebalanceYieldsBalance 验证：多种 current 下，规划后各有效 gid 负载差 ≤1。
func TestPlanRebalanceYieldsBalance(t *testing.T) {
	cases := []struct {
		cur  [NShards]int
		gids []int
	}{
		{fillShards(1, NShards), []int{1, 2, 3}},
		{fillShards(2, 6), []int{2, 5}},
		{fillShards(3, 4), []int{3, 4, 5, 6}},
	}
	for ci, tc := range cases {
		plan := PlanRebalance(tc.cur, tc.gids)
		// 统计有效 gid 的目标负载。
		loads := map[int]int{}
		for _, g := range plan.Target {
			if g == 0 {
				continue // 0 仅在 gids 为空时出现，本用例 gids 非空
			}
			loads[g]++
		}
		min, max := NShards, 0
		for _, l := range loads {
			if l < min {
				min = l
			}
			if l > max {
				max = l
			}
		}
		if max-min > 1 {
			t.Fatalf("case %d 规划后失衡：min=%d max=%d loads=%v", ci, min, max, loads)
		}
		// 移动的碎片数应等于「失效 + 超额」碎片数（最小迁移）。
		var invalidPlusOver int
		valid := map[int]bool{}
		for _, g := range tc.gids {
			valid[g] = true
		}
		base := NShards / len(tc.gids)
		extra := NShards % len(tc.gids)
		load := map[int]int{}
		for _, g := range tc.cur {
			if valid[g] {
				load[g]++
			} else {
				invalidPlusOver++
			}
		}
		for i, g := range tc.gids {
			quota := base
			if i < extra {
				quota = base + 1
			}
			if load[g] > quota {
				invalidPlusOver += load[g] - quota
			}
		}
		if plan.Moved != invalidPlusOver {
			t.Fatalf("case %d 移动数 %d 与最小迁移 %d 不符", ci, plan.Moved, invalidPlusOver)
		}
	}
}
