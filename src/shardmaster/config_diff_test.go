package shardmaster

import (
	"reflect"
	"testing"
)

func TestConfigDiff(t *testing.T) {
	prev := &Config{
		Num:    1,
		Shards: [NShards]int{1, 1, 1, 1, 1, 2, 2, 2, 2, 2},
		Groups: map[int][]string{1: {"s1"}, 2: {"s2"}},
	}
	// next：移除 gid2、新增 gid3、把分片 0,5 迁到 gid3 → 配置号变化
	next := &Config{
		Num:    2,
		Shards: [NShards]int{3, 1, 1, 1, 1, 3, 2, 2, 2, 2},
		Groups: map[int][]string{1: {"s1"}, 3: {"s3"}},
	}
	d := ConfigDiff(prev, next)
	if !reflect.DeepEqual(d.AddedGids, []int{3}) {
		t.Fatalf("AddedGids 应为 [3]，实际 %v", d.AddedGids)
	}
	if !reflect.DeepEqual(d.RemovedGids, []int{2}) {
		t.Fatalf("RemovedGids 应为 [2]，实际 %v", d.RemovedGids)
	}
	if !d.NumChanged {
		t.Fatal("配置号应判定为变化")
	}
	// 迁移步骤应包含 shard0(1→3) 与 shard5(2→3)
	if len(d.Moved) != 2 {
		t.Fatalf("期望 2 次迁移，实际 %d: %v", len(d.Moved), d.Moved)
	}
	// nil 安全
	if dd := ConfigDiff(nil, next); dd.Moved != nil {
		t.Fatal("nil 输入应返回零值")
	}
}
