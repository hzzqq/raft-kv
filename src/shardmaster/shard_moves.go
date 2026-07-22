package shardmaster

// ShardMove 是两份配置之间单个分片的属主变迁：Shard 从 From 迁到 To。
// From==To 表示无迁移（不出现在结果中）；From 为 0 表示此前未分配（首次分配）；
// To 为 0 表示被回收（无有效属主）。
type ShardMove struct {
	Shard int
	From  int
	To    int
}

// ShardMoves 对比 prev 与 next 两份配置，返回所有分片属主发生变化的迁移步骤
// （按分片号升序）。纯函数、零副作用，便于：① 再平衡前后审计"动了哪些分片、
// 迁移代价多大"；② 运维 dry-run 预览，避免直接提交后大范围抖动；③ 测试断言
// 迁移结果与预期完全一致。任一输入为 nil 返回 nil。
func ShardMoves(prev, next *Config) []ShardMove {
	if prev == nil || next == nil {
		return nil
	}
	var moves []ShardMove
	for i := 0; i < NShards; i++ {
		if prev.Shards[i] != next.Shards[i] {
			moves = append(moves, ShardMove{Shard: i, From: prev.Shards[i], To: next.Shards[i]})
		}
	}
	return moves
}
