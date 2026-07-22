package shardmaster

import "fmt"

// Valid 校验单份配置自身的结构合法性：每个组至少 1 个 server，且每个分片都归属一个
// 存在的组（无孤儿分片、无空组）。用于配置演进前后的一致性守门。
func (c *Config) Valid() bool {
	if c == nil {
		return false
	}
	for _, srvs := range c.Groups {
		if len(srvs) == 0 {
			return false
		}
	}
	for i := 0; i < NShards; i++ {
		if _, ok := c.Groups[c.Shards[i]]; !ok {
			return false
		}
	}
	return true
}

// IsValidTransition 校验 next 是否为 cur 的合法演进（Join/Leave/Move 应用后的配置守门）：
// 1) 配置号严格 +1；2) next 自身结构合法（无孤儿分片/空组）。
// 组可被 Join 新增或 Leave 删除——只要 next 中每个分片都落在存在的组上，即视为已正确重分配。
func (c *Config) IsValidTransition(next *Config) (bool, string) {
	if next == nil {
		return false, "next config is nil"
	}
	if next.Num != c.Num+1 {
		return false, fmt.Sprintf("config num must be %d, got %d", c.Num+1, next.Num)
	}
	if !next.Valid() {
		return false, "next config structure invalid (orphan shard or empty group)"
	}
	return true, ""
}
