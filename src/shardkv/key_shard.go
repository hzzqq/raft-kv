package shardkv

// Key2Shard 把任意 key 映射到其所属分片编号（0..NShards-1），使用与状态机一致
// 的 fnv-1a 哈希取模。导出以便上层（gateway 路由、client 预判、测试断言）复用
// 同一映射，避免"客户端算错分片导致总是 ErrWrongGroup"这类隐性 bug。
func Key2Shard(key string) int {
	return key2shard(key)
}
