package shardmaster

// ConfigShardCoverage 统计配置的"覆盖度"：返回 (covered, uncovered)。
//   - covered：分片指向一个存在于 Groups 的有效 gid（已正常分配）；
//   - uncovered：分片未分配（gid==0）或指向不存在的 gid（配置损坏）。
//
// 二者之和恒为 NShards。用于 /status 健康度（uncovered>0 即告警）、再平衡前
// 巡检、以及单元测试断言"再平衡后全覆盖"。nil 输入返回 (0, NShards)。
func ConfigShardCoverage(c *Config) (covered, uncovered int) {
	if c == nil {
		return 0, NShards
	}
	for i := 0; i < NShards; i++ {
		gid := c.Shards[i]
		if gid != 0 {
			if _, ok := c.Groups[gid]; ok {
				covered++
				continue
			}
		}
		uncovered++
	}
	return covered, uncovered
}
