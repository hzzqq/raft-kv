package shardkv

// OpShard 返回 op 所属的分片编号（基于 op.Key 的哈希）。纯函数，供路由判断
// 「本 op 应落在哪个分片 / 是否归本 group 负责」复用，与状态机 key2shard 一致。
func OpShard(op Op) int {
	return Key2Shard(op.Key)
}
