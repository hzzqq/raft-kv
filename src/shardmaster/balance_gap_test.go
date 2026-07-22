package shardmaster

import "testing"

func TestBalanceGap(t *testing.T) {
	bal := &Config{Num: 1, Shards: [NShards]int{1, 1, 1, 1, 1, 2, 2, 2, 2, 2}, Groups: map[int][]string{1: {"s1"}, 2: {"s2"}}}
	if g := BalanceGap(bal); g != 0 {
		t.Fatalf("均衡配置 gap 应为 0，实际 %d", g)
	}
	// gid1=7, gid2=3 → gap=4
	unbal := &Config{Num: 1, Shards: [NShards]int{1, 1, 1, 1, 1, 1, 1, 2, 2, 2}, Groups: map[int][]string{1: {"s1"}, 2: {"s2"}}}
	if g := BalanceGap(unbal); g != 4 {
		t.Fatalf("期望 gap=4，实际 %d", g)
	}
	// 3 个 gid：4/3/3 → gap=1
	three := &Config{Num: 1, Shards: [NShards]int{1, 1, 1, 1, 2, 2, 2, 3, 3, 3}, Groups: map[int][]string{1: {"s1"}, 2: {"s2"}, 3: {"s3"}}}
	if g := BalanceGap(three); g != 1 {
		t.Fatalf("期望 gap=1，实际 %d", g)
	}
	if BalanceGap(nil) != 0 || BalanceGap(&Config{}) != 0 {
		t.Fatal("nil / 空 Groups 应返回 0")
	}
}
