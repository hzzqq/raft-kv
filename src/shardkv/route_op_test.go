package shardkv

import (
	"testing"

	"raftkv/src/shardmaster"
)

func TestRouteOp(t *testing.T) {
	// 依据真实哈希把特定 key 钉到目标 gid，避免对 key→shard 映射硬编码。
	var shards [shardmaster.NShards]int
	shards[Key2Shard("a")] = 1
	shards[Key2Shard("c")] = 2
	c := &shardmaster.Config{
		Num:    1,
		Shards: shards,
		Groups: map[int][]string{1: {"s1"}, 2: {"s2"}},
	}
	// key "a" → gid 1
	gid, ok := RouteOp(Op{Key: "a"}, c)
	if !ok || gid != 1 {
		t.Fatalf("期望路由到 gid 1，实际 ok=%v gid=%d", ok, gid)
	}
	// key "c" → gid 2
	g2, ok2 := RouteOp(Op{Key: "c"}, c)
	if !ok2 || g2 != 2 {
		t.Fatalf("期望路由到 gid 2，实际 ok=%v gid=%d", ok2, g2)
	}
	// 未分配分片（gid==0）→ ok=false
	var unassigned [shardmaster.NShards]int
	unassigned[Key2Shard("zzz")] = 0
	cUn := &shardmaster.Config{Num: 1, Shards: unassigned, Groups: map[int][]string{1: {"s1"}}}
	if _, ok3 := RouteOp(Op{Key: "zzz"}, cUn); ok3 {
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
