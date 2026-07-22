package shardmaster

import "testing"

func TestIsBalanced(t *testing.T) {
	// 均衡：2 个 gid 各 5 片
	bal := &Config{Num: 1, Shards: [NShards]int{1, 1, 1, 1, 1, 2, 2, 2, 2, 2}, Groups: map[int][]string{1: {"s1"}, 2: {"s2"}}}
	if !IsBalanced(bal) {
		t.Fatal("2 gid 各 5 片应判为均衡")
	}
	// 不均衡：差 >1
	unbal := &Config{Num: 1, Shards: [NShards]int{1, 1, 1, 1, 1, 1, 1, 2, 2, 2}, Groups: map[int][]string{1: {"s1"}, 2: {"s2"}}}
	if IsBalanced(unbal) {
		t.Fatal("gid1=7/gid2=3 差 4 应判为不均衡")
	}
	// 未分配分片 → 不均衡
	unassigned := &Config{Num: 1, Shards: [NShards]int{1, 1, 1, 1, 1, 0, 0, 0, 0, 0}, Groups: map[int][]string{1: {"s1"}}}
	if IsBalanced(unassigned) {
		t.Fatal("含未分配分片应判为不均衡")
	}
	// 指向失效 gid → 不均衡
	broken := &Config{Num: 1, Shards: [NShards]int{9, 9, 9, 9, 9, 9, 9, 9, 9, 9}, Groups: map[int][]string{1: {"s1"}}}
	if IsBalanced(broken) {
		t.Fatal("指向未知 gid 应判为不均衡")
	}
	// 无 group 且全未分配 → 均衡
	none := &Config{Num: 0, Shards: [NShards]int{}, Groups: map[int][]string{}}
	if !IsBalanced(none) {
		t.Fatal("无 group 且全未分配应判为均衡")
	}
	if IsBalanced(nil) {
		t.Fatal("nil 应返回 false")
	}
}
