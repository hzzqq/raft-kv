package shardmaster

// BalanceGap 返回配置中有效 gid 之间的最大分片数差（max(count)-min(count)）。
// 0 表示完全均衡；越大表示越倾斜。结合 IsBalanced（gap≤1 判均衡）使用：
// gap 可作为巡检阈值（如 gap>=3 才告警）、再平衡收益评估（迁移后 gap 应归零附近）、
// 以及 gate 资格判定（gap 过大时禁止新 Join/Leave 以免雪上加霜）。nil 或无 gid 返回 0。
func BalanceGap(c *Config) int {
	if c == nil || len(c.Groups) == 0 {
		return 0
	}
	counts := GidShardCounts(c)
	min, max := NShards, 0
	for _, n := range counts {
		if n < min {
			min = n
		}
		if n > max {
			max = n
		}
	}
	return max - min
}
