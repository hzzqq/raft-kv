package shardmaster

import "sort"

// ConfigShardsByGid 返回配置中各 gid 负责的分片编号列表（按分片号升序）。
// 与 GidShardCounts（返回计数）互补：此处给出具体分片下标，便于迁移规划、
// 运维「某 group 负责哪些分片」的展示、以及单测断言「rebalance 后分片归位正确」。
// 仅包含存在于 Groups 的 gid；指向失效 gid 的分片不计入任何 gid 列表。
// nil 输入安全返回 nil。
func ConfigShardsByGid(c *Config) map[int][]int {
	if c == nil {
		return nil
	}
	m := make(map[int][]int, len(c.Groups))
	for gid := range c.Groups {
		m[gid] = nil
	}
	for i := 0; i < NShards; i++ {
		gid := c.Shards[i]
		if _, ok := m[gid]; ok {
			m[gid] = append(m[gid], i)
		}
	}
	for gid := range m {
		sort.Ints(m[gid])
	}
	return m
}
