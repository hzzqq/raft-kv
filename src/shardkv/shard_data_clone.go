package shardkv

// Clone 返回分片数据的深拷贝：Data / LastSeq / LastResult 三个 map 均逐键复制，
// 与原实例完全隔离，修改任一方不影响另一方。迁移传输、快照序列化前调用，
// 避免"共享底层 map 导致的并发写竞态"与"回源后数据被后续写覆盖"两类隐性 bug。
// 接收者为 nil 时返回 nil（安全）。
func (sd *ShardData) Clone() *ShardData {
	if sd == nil {
		return nil
	}
	return sd.copy()
}
