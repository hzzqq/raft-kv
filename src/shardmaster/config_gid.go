package shardmaster

import "sort"

// GidList 返回配置中所有 gid（升序排列），便于稳定遍历、比对与分片再平衡计算。
// nil 输入安全返回 nil；空 Groups 返回空切片（非 nil）。
func GidList(c *Config) []int {
	if c == nil {
		return nil
	}
	gids := make([]int, 0, len(c.Groups))
	for gid := range c.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	return gids
}
