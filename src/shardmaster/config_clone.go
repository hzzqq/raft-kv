package shardmaster

// CloneConfig 深拷贝一份 Config：Shards 数组按值拷贝，Groups map 逐键深拷贝其服务器切片。
// 供测试构造 fixture、配置快照/预览（如 PlanRebalance 前保存原状）使用，
// 避免调用方修改副本时意外污染原始配置。nil 输入安全返回 nil。
func CloneConfig(c *Config) *Config {
	if c == nil {
		return nil
	}
	clone := &Config{
		Num:    c.Num,
		Shards: c.Shards, // 数组按值拷贝
	}
	clone.Groups = make(map[int][]string, len(c.Groups))
	for gid, servers := range c.Groups {
		cp := make([]string, len(servers))
		copy(cp, servers)
		clone.Groups[gid] = cp
	}
	return clone
}
