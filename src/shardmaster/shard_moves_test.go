package shardmaster

import "testing"

func TestShardMoves(t *testing.T) {
	prev := &Config{Num: 1, Shards: [NShards]int{1, 1, 1, 1, 1, 2, 2, 2, 2, 2}, Groups: map[int][]string{1: {"s1"}, 2: {"s2"}}}
	// 把 shard 0 从 gid1 迁到 gid2，shard 5 从 gid2 迁到 gid1，其余不变。
	next := prev
	next = &Config{Num: 2, Shards: [NShards]int{2, 1, 1, 1, 1, 1, 2, 2, 2, 2}, Groups: map[int][]string{1: {"s1"}, 2: {"s2"}}}
	moves := ShardMoves(prev, next)
	if len(moves) != 2 {
		t.Fatalf("期望 2 次迁移，实际 %d: %v", len(moves), moves)
	}
	// 按分片升序：shard0 (1→2), shard5 (2→1)
	if moves[0] != (ShardMove{Shard: 0, From: 1, To: 2}) {
		t.Fatalf("shard0 迁移不符：%v", moves[0])
	}
	if moves[1] != (ShardMove{Shard: 5, From: 2, To: 1}) {
		t.Fatalf("shard5 迁移不符：%v", moves[1])
	}
	// 相同配置 → 无迁移
	if m := ShardMoves(prev, prev); len(m) != 0 {
		t.Fatalf("相同配置应无迁移，实际 %v", m)
	}
	// nil 安全
	if ShardMoves(nil, next) != nil || ShardMoves(prev, nil) != nil {
		t.Fatal("nil 输入应返回 nil")
	}
}
