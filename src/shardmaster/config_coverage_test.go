package shardmaster

import "testing"

func TestConfigShardCoverage(t *testing.T) {
	// 10 分片全有效分配
	full := &Config{Num: 1, Shards: [NShards]int{1, 1, 1, 1, 1, 2, 2, 2, 2, 2}, Groups: map[int][]string{1: {"s1"}, 2: {"s2"}}}
	cov, unc := ConfigShardCoverage(full)
	if cov != NShards || unc != 0 {
		t.Fatalf("期望全覆盖(10,0)，实际 (%d,%d)", cov, unc)
	}
	// 5 个未分配 + 1 个指向失效 gid
	partial := &Config{Num: 1, Shards: [NShards]int{1, 1, 1, 1, 1, 0, 0, 0, 0, 0}, Groups: map[int][]string{1: {"s1"}}}
	cov2, unc2 := ConfigShardCoverage(partial)
	if cov2 != 5 {
		t.Fatalf("期望 covered=5，实际 %d", cov2)
	}
	if unc2 != 5 {
		t.Fatalf("期望 uncovered=5（4 未分配+1 失效gid），实际 %d", unc2)
	}
	if cov2+unc2 != NShards {
		t.Fatalf("covered+uncovered 应恒等于 NShards")
	}
	if c, u := ConfigShardCoverage(nil); c != 0 || u != NShards {
		t.Fatalf("nil 应返回 (0,%d)，实际 (%d,%d)", NShards, c, u)
	}
}
