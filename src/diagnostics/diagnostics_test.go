package diagnostics

import (
	"testing"

	"raftkv/src/shardmaster"
)

func TestDiagnoseConfig(t *testing.T) {
	// 健康配置：2 gid 各 5 片，全分配 → 满分、ok
	good := &shardmaster.Config{Num: 1, Shards: [shardmaster.NShards]int{1, 1, 1, 1, 1, 2, 2, 2, 2, 2}, Groups: map[int][]string{1: {"s1"}, 2: {"s2"}}}
	d := DiagnoseConfig(good)
	if d.Score != 100 {
		t.Fatalf("健康配置应满分 100，实际 %d issues=%v", d.Score, d.Issues)
	}

	// nil → 0 分
	if dn := DiagnoseConfig(nil); dn.Score != 0 {
		t.Fatalf("nil 应判 0 分，实际 %d", dn.Score)
	}

	// 不均衡：gid1=7/gid2=3 → 扣分
	unbal := &shardmaster.Config{Num: 1, Shards: [shardmaster.NShards]int{1, 1, 1, 1, 1, 1, 1, 2, 2, 2}, Groups: map[int][]string{1: {"s1"}, 2: {"s2"}}}
	du := DiagnoseConfig(unbal)
	if du.Score >= 100 {
		t.Fatalf("不均衡配置不应满分，实际 %d", du.Score)
	}
	if len(du.Issues) == 0 || du.Issues[0] == "ok" {
		t.Fatalf("不均衡应有问题项，实际 %v", du.Issues)
	}

	// 未覆盖：5 个未分配 → 扣分且含 uncovered 描述
	partial := &shardmaster.Config{Num: 1, Shards: [shardmaster.NShards]int{1, 1, 1, 1, 1, 0, 0, 0, 0, 0}, Groups: map[int][]string{1: {"s1"}}}
	dp := DiagnoseConfig(partial)
	found := false
	for _, s := range dp.Issues {
		if len(s) > 9 && s[:9] == "uncovered" {
			found = true
		}
	}
	if !found {
		t.Fatalf("未覆盖配置应含 uncovered 描述，实际 %v", dp.Issues)
	}
}
