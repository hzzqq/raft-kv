package shardmaster

import "sort"

// ConfigDelta 是两份配置之间的差异摘要：新增/移除的 gid、分片迁移步骤（复用
// ShardMoves）、以及配置号是否变化。用于运维审计「这次 rebalance / Join / Leave
// 到底改了什么」、网关 /status 的变化提示、以及单测断言「Leave 后某 gid 被移除且
// 其分片被迁出」。纯函数、零副作用。任一输入为 nil 返回零值 ConfigDelta。
type ConfigDelta struct {
	AddedGids   []int
	RemovedGids []int
	Moved       []ShardMove
	NumChanged  bool
}

// ConfigDiff 对比 prev 与 next 两份配置，产出差异摘要。gid 列表按升序返回，稳定可读。
func ConfigDiff(prev, next *Config) ConfigDelta {
	if prev == nil || next == nil {
		return ConfigDelta{}
	}
	d := ConfigDelta{
		Moved:      ShardMoves(prev, next),
		NumChanged: prev.Num != next.Num,
	}
	prevGids := make(map[int]bool, len(prev.Groups))
	for g := range prev.Groups {
		prevGids[g] = true
	}
	nextGids := make(map[int]bool, len(next.Groups))
	for g := range next.Groups {
		nextGids[g] = true
	}
	for g := range next.Groups {
		if !prevGids[g] {
			d.AddedGids = append(d.AddedGids, g)
		}
	}
	for g := range prev.Groups {
		if !nextGids[g] {
			d.RemovedGids = append(d.RemovedGids, g)
		}
	}
	sort.Ints(d.AddedGids)
	sort.Ints(d.RemovedGids)
	return d
}
