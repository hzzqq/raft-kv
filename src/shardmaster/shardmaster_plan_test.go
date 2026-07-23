package shardmaster

import (
	"testing"
)

// baseConfig2Groups 造一个 2 组、分片已在两组间铺开的合法配置（Num=1）。
func baseConfig2Groups() *Config {
	c := &Config{
		Num:    1,
		Groups: map[int][]string{1: {"s1"}, 2: {"s2"}},
	}
	for i := 0; i < NShards; i++ {
		c.Shards[i] = 1 + (i % 2)
	}
	return c
}

func allShardsInGroups(c *Config) bool {
	for i := 0; i < NShards; i++ {
		if _, ok := c.Groups[c.Shards[i]]; !ok {
			return false
		}
	}
	return true
}

// TestPlanJoinRebalance 验证 Join 新组后整体再平衡：目标配置合法、演进合法、均衡。
func TestPlanJoinRebalance(t *testing.T) {
	cur := baseConfig2Groups()
	res := Plan(cur, PlanOp{Join: map[int][]string{3: {"s3"}}})
	if res.Planned == nil {
		t.Fatal("nil planned")
	}
	if res.Planned.Num != 2 {
		t.Fatalf("planned num = %d, want 2", res.Planned.Num)
	}
	if !res.Planned.Valid() {
		t.Fatalf("planned config invalid: %v", res.Errors)
	}
	if res.TransitionErr != "" {
		t.Fatalf("unexpected transition error: %s", res.TransitionErr)
	}
	if !allShardsInGroups(res.Planned) {
		t.Fatalf("planned has orphan shard")
	}
	if !IsBalanced(res.Planned) {
		t.Fatalf("planned not balanced after join")
	}
	if _, exists := res.Planned.Groups[3]; !exists {
		t.Fatalf("joined group 3 missing")
	}
}

// TestPlanLeaveRebalance 验证 Leave 删除组后分片被安全重分配到其余组（无孤儿）。
func TestPlanLeaveRebalance(t *testing.T) {
	cur := baseConfig2Groups()
	// 再加一个组 3 让 leave 更有意义
	cur.Groups[3] = []string{"s3"}
	rebalance(cur)
	res := Plan(cur, PlanOp{Leave: []int{3}})
	if !res.Planned.Valid() {
		t.Fatalf("planned config invalid after leave: %v", res.Errors)
	}
	if _, exists := res.Planned.Groups[3]; exists {
		t.Fatalf("left group 3 still present")
	}
	if !allShardsInGroups(res.Planned) {
		t.Fatalf("orphan shard after leave rebalance")
	}
	if !IsBalanced(res.Planned) {
		t.Fatalf("planned not balanced after leave")
	}
}

// TestPlanMove 验证 Move 只改单个分片、不触发再平衡、目标配置仍合法。
func TestPlanMove(t *testing.T) {
	cur := baseConfig2Groups()
	// 把 shard 0 从当前属主迁到组 2
	from := cur.Shards[0]
	to := 2
	if from == to {
		to = 1
	}
	res := Plan(cur, PlanOp{Move: &PlanMove{Shard: 0, Gid: to}})
	if res.Planned.Shards[0] != to {
		t.Fatalf("shard 0 = %d, want %d", res.Planned.Shards[0], to)
	}
	if !res.Planned.Valid() {
		t.Fatalf("move target invalid: %v", res.Errors)
	}
	if res.TransitionErr != "" {
		t.Fatalf("unexpected transition error: %s", res.TransitionErr)
	}
	// 其余分片不应被再平衡改动
	for i := 1; i < NShards; i++ {
		if res.Planned.Shards[i] != cur.Shards[i] {
			t.Fatalf("shard %d changed by Move (should be untouched): %d != %d", i, res.Planned.Shards[i], cur.Shards[i])
		}
	}
}

// TestPlanInvalidMoveOrphan 验证 Move 到不存在的组会被校验出来（孤儿分片 + 演进非法）。
func TestPlanInvalidMoveOrphan(t *testing.T) {
	cur := baseConfig2Groups()
	res := Plan(cur, PlanOp{Move: &PlanMove{Shard: 0, Gid: 99}})
	if len(res.Errors) == 0 {
		t.Fatalf("expected structural errors for orphan shard")
	}
	if res.TransitionErr == "" {
		t.Fatalf("expected transition error for invalid move")
	}
	if res.Planned.Shards[0] != 99 {
		t.Fatalf("planned shard 0 = %d, want 99 (preview should show the bad state)", res.Planned.Shards[0])
	}
}

// TestPlanNoMutationOfCurrent 验证 Plan 是纯函数，不修改入参当前配置。
func TestPlanNoMutationOfCurrent(t *testing.T) {
	cur := baseConfig2Groups()
	snapshot := *cur
	snapshotGroups := copyGroups(cur.Groups)
	snapshotShards := cur.Shards
	_ = Plan(cur, PlanOp{Join: map[int][]string{3: {"s3"}}})
	if cur.Num != snapshot.Num {
		t.Fatalf("current.Num mutated: %d", cur.Num)
	}
	if len(cur.Groups) != len(snapshotGroups) {
		t.Fatalf("current.Groups mutated: %v", cur.Groups)
	}
	for i := 0; i < NShards; i++ {
		if cur.Shards[i] != snapshotShards[i] {
			t.Fatalf("current.Shards[%d] mutated: %d", i, cur.Shards[i])
		}
	}
}

// TestPlanTransitionNum 验证配置号严格 +1 演进。
func TestPlanTransitionNum(t *testing.T) {
	cur := baseConfig2Groups()
	cur.Num = 5
	res := Plan(cur, PlanOp{Join: map[int][]string{3: {"s3"}}})
	if res.Planned.Num != 6 {
		t.Fatalf("planned num = %d, want 6", res.Planned.Num)
	}
	if ok, _ := cur.IsValidTransition(res.Planned); !ok {
		t.Fatalf("expected valid transition")
	}
}
