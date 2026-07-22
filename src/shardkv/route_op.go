package shardkv

import "raftkv/src/shardmaster"

// RouteOp 根据当前配置 c 判定 op 应路由到的 replica group（gid）。
// 返回 (gid, ok)：gid 为 op.Key 所属分片的属主；ok=false 表示该分片尚未分配
// （gid==0，集群初始态）或被指向了不存在的 group（配置损坏），此刻应返
// ErrWrongGroup 让客户端等待配置就绪，而非带病写入。纯函数、零副作用，可直接单测。
func RouteOp(op Op, c *shardmaster.Config) (int, bool) {
	if c == nil {
		return 0, false
	}
	shard := OpShard(op)
	gid := c.Shards[shard]
	if gid == 0 {
		return 0, false
	}
	if _, ok := c.Groups[gid]; !ok {
		return 0, false
	}
	return gid, true
}
