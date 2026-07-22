package main

import "testing"

// 构造带 owned 数的 group 状态。
func owned(g, n int) groupStatus {
	return groupStatus{Group: g, OwnedCount: n}
}

// TestShardBalanceEven 验证：完全均衡 → 100 分。
func TestShardBalanceEven(t *testing.T) {
	st := clusterStatus{Groups: []groupStatus{owned(1, 5), owned(2, 5)}}
	score, _ := shardBalance(st)
	if score != 100 {
		t.Fatalf("期望 100（均衡），实际 %v", score)
	}
}

// TestShardBalanceSkew 验证：9/1 失衡 → 100-80=20 分。
func TestShardBalanceSkew(t *testing.T) {
	st := clusterStatus{Groups: []groupStatus{owned(1, 9), owned(2, 1)}}
	score, _ := shardBalance(st)
	if score != 20 {
		t.Fatalf("期望 20（9/1 失衡），实际 %v", score)
	}
}

// TestShardBalanceSingle 验证：单 group 无失衡可言 → 100 分。
func TestShardBalanceSingle(t *testing.T) {
	st := clusterStatus{Groups: []groupStatus{owned(1, 10)}}
	score, _ := shardBalance(st)
	if score != 100 {
		t.Fatalf("期望 100（单 group），实际 %v", score)
	}
}

// TestShardBalanceThree 验证：4/3/3 总 10，极差 1 → 100-10=90 分。
func TestShardBalanceThree(t *testing.T) {
	st := clusterStatus{Groups: []groupStatus{owned(1, 4), owned(2, 3), owned(3, 3)}}
	score, _ := shardBalance(st)
	if score != 90 {
		t.Fatalf("期望 90（极差 1），实际 %v", score)
	}
}

// TestShardBalanceNoGroups 验证：无 group → 0 分。
func TestShardBalanceNoGroups(t *testing.T) {
	score, detail := shardBalance(clusterStatus{})
	if score != 0 || detail != "no groups" {
		t.Fatalf("期望 0 + 'no groups'，实际 %v / %q", score, detail)
	}
}
