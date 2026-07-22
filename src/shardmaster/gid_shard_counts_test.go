package shardmaster

import "testing"

func TestGidShardCounts(t *testing.T) {
	c := &Config{
		Num:    1,
		Shards: [NShards]int{1, 1, 2, 2, 1, 2, 0, 0, 0, 0},
		Groups: map[int][]string{1: {"s1"}, 2: {"s2"}},
	}
	counts := GidShardCounts(c)
	if counts[1] != 3 {
		t.Fatalf("gid 1 应负责 3 个分片，实际 %d", counts[1])
	}
	if counts[2] != 3 {
		t.Fatalf("gid 2 应负责 3 个分片，实际 %d", counts[2])
	}
	// 指向不存在 gid 的分片（此处无）不计入有效 gid；空 Groups 返回空 map。
	empty := &Config{Num: 0}
	if m := GidShardCounts(empty); len(m) != 0 {
		t.Fatalf("空 Groups 应返回空 map，实际 %v", m)
	}
	if GidShardCounts(nil) != nil {
		t.Fatal("nil 输入应返回 nil")
	}
}
