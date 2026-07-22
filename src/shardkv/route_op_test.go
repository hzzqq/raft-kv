package shardkv

import (
	"testing"

	"raftkv/src/shardmaster"
)

func TestRouteOp(t *testing.T) {
	c := &shardmaster.Config{
		Num:    1,
		Shards: [shardmaster.NShards]int{1, 1, 2, 2, 0, 0, 0, 0, 0, 0},
		Groups: map[int][]string{1: {"s1"}, 2: {"s2"}},
	}
	// key 落在 shard 0 → gid 1
	gid, ok := RouteOp(Op{Key: "a"}, c)
	if !ok || gid != 1 {
		t.Fatalf("期望路由到 gid 1，实际 ok=%v gid=%d", ok, gid)
	}
	// key 落在 shard 2 → gid 2
	g2, ok2 := RouteOp(Op{Key: "c"}, c)
	if !ok2 || g2 != 2 {
		t.Fatalf("期望路由到 gid 2，实际 ok=%v gid=%d", ok2, g2)
	}
	// 未分配分片（gid==0）→ ok=false
	_, ok3 := RouteOp(Op{Key: "shard4"}, c)
	if ok3 {
		t.Fatal("未分配分片应返回 ok=false")
	}
	// nil 配置 → ok=false
	if _, ok4 := RouteOp(Op{Key: "a"}, nil); ok4 {
		t.Fatal("nil 配置应返回 ok=false")
	}
	// 指向不存在的 gid → ok=false（配置损坏护栏）
	bad := &shardmaster.Config{Num: 2, Shards: [shardmaster.NShards]int{9, 9, 9, 9, 9, 9, 9, 9, 9, 9}, Groups: map[int][]string{1: {"s1"}}}
	if _, ok5 := RouteOp(Op{Key: "a"}, bad); ok5 {
		t.Fatal("指向未知 gid 的配置应返回 ok=false")
	}
}
