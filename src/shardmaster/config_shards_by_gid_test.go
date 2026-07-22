package shardmaster

import (
	"reflect"
	"testing"
)

func TestConfigShardsByGid(t *testing.T) {
	c := &Config{
		Num:    1,
		Shards: [NShards]int{1, 1, 1, 2, 2, 2, 2, 0, 0, 0},
		Groups: map[int][]string{1: {"s1"}, 2: {"s2"}},
	}
	m := ConfigShardsByGid(c)
	want := map[int][]int{
		1: {0, 1, 2},
		2: {3, 4, 5, 6},
	}
	if !reflect.DeepEqual(m, want) {
		t.Fatalf("ConfigShardsByGid 不符：实际 %v 期望 %v", m, want)
	}
	// 指向失效 gid 的分片不计入
	broken := &Config{Num: 1, Shards: [NShards]int{9, 9, 9, 9, 9, 9, 9, 9, 9, 9}, Groups: map[int][]string{1: {"s1"}}}
	if mb := ConfigShardsByGid(broken); len(mb) != 1 || len(mb[1]) != 0 {
		t.Fatalf("失效 gid 分片不应计入，实际 %v", mb)
	}
	if ConfigShardsByGid(nil) != nil {
		t.Fatal("nil 应返回 nil")
	}
}
