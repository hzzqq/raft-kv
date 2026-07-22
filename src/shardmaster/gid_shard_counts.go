package shardmaster

// GidShardCounts 返回配置中每个有效 gid 当前负责的分片数量（gid→count）。
// 仅统计存在于 Groups 的 gid；若某分片指向了不存在的 gid（配置损坏），
// 该分片不被计入任何有效 gid 的计数（可用 ValidateConfig 另检）。
// nil 输入安全返回 nil。用于负载均衡评估、再平衡前快照、可观测性展示。
func GidShardCounts(c *Config) map[int]int {
	if c == nil {
		return nil
	}
	counts := make(map[int]int, len(c.Groups))
	for gid := range c.Groups {
		counts[gid] = 0
	}
	for i := 0; i < NShards; i++ {
		gid := c.Shards[i]
		if _, ok := counts[gid]; ok {
			counts[gid]++
		}
	}
	return counts
}
