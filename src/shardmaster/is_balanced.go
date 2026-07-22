package shardmaster

// IsBalanced 判断配置的分片分布是否均衡：所有有效 gid 负责的分片数之差 ≤1，
// 且（存在 gid 时）没有未分配（gid==0）或指向失效 gid 的分片。这是对
// PlanRebalance 目标（"各 gid 负载差 ≤1"）的事后校验，可用于再平衡前后断言、
// 周期巡检、或网关 /status 的健康判定。nil 输入返回 false。
func IsBalanced(c *Config) bool {
	if c == nil {
		return false
	}
	ng := len(c.Groups)
	if ng == 0 {
		// 无 group：仅当所有分片均未分配才算"均衡"（否则是损坏态）。
		for i := 0; i < NShards; i++ {
			if c.Shards[i] != 0 {
				return false
			}
		}
		return true
	}
	counts := GidShardCounts(c)
	// 任一分片指向了不存在的 gid（counts 未含该 gid）→ 不均衡（配置损坏）。
	for i := 0; i < NShards; i++ {
		if _, ok := counts[c.Shards[i]]; !ok {
			return false
		}
	}
	min, max := NShards, 0
	for _, n := range counts {
		if n < min {
			min = n
		}
		if n > max {
			max = n
		}
	}
	return max-min <= 1
}
