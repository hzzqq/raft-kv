// config_cmp.go —— shardmaster 配置比较辅助（纯函数，零依赖、可单测）。
//
// 这些函数把"配置演进判断"从具体 RPC 路径里抽出来，统一语义，便于：
//   - 测试断言 rebalance 后配置正确推进（Num 单调、分片不重不漏）；
//   - 上层（gateway / 诊断端点）判断某段配置是否比另一段更新、属于哪个 group；
//   - 避免各调用点重复手写易错的逐字段比较。
package shardmaster

import (
	"fmt"
	"sort"
)

// slicesEqualSet 判断两个字符串切片作为集合是否相等（忽略顺序、允许重复计数一致）。
// 用于比较 Config.Groups 中 gid 对应的 server 列表（顺序在不同 copy 路径下可能不一致）。
func slicesEqualSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sa := append([]string(nil), a...)
	sb := append([]string(nil), b...)
	sort.Strings(sa)
	sort.Strings(sb)
	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}
	return true
}

// groupsEqual 判断两份 group→servers 映射是否等价（gid 集合一致、各自 server 列表集合一致）。
func groupsEqual(a, b map[int][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for gid, servers := range a {
		bs, ok := b[gid]
		if !ok || !slicesEqualSet(servers, bs) {
			return false
		}
	}
	return true
}

// ConfigsEqual 判断两份配置在语义上是否一致：Num、Shards 全量、Groups（集合语义）皆同。
// 用于断言 rebalance / Query 返回的配置与预期完全一致。
func ConfigsEqual(a, b *Config) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Num != b.Num {
		return false
	}
	if a.Shards != b.Shards {
		return false
	}
	return groupsEqual(a.Groups, b.Groups)
}

// IsNewer 判断配置 c 是否比 prev 更新（Num 严格更大）。prev 为 nil 时，任意非空 c 视为更新。
func IsNewer(c, prev *Config) bool {
	if prev == nil {
		return c != nil
	}
	if c == nil {
		return false
	}
	return c.Num > prev.Num
}

// NextConfigNum 返回配置 c 的下一段应使用的配置号（c.Num+1）。c 为 nil 时返回 0。
func NextConfigNum(c *Config) int {
	if c == nil {
		return 0
	}
	return c.Num + 1
}

// OwnedShards 返回配置 c 中归 gid 所有的分片编号（升序）。c 为 nil 返回 nil。
func OwnedShards(c *Config, gid int) []int {
	if c == nil {
		return nil
	}
	var out []int
	for i := 0; i < NShards; i++ {
		if c.Shards[i] == gid {
			out = append(out, i)
		}
	}
	return out
}

// ValidateConfig 校验单份配置的内部一致性，返回所有违规描述（空切片=合法）。
// 用于 Apply 前预检 / Query 结果自检，把"分片指向不存在的 group"这类
// 静默配置错误提前暴露为可读错误，而非带病上线后数据错乱。
func ValidateConfig(c *Config) []string {
	if c == nil {
		return []string{"config is nil"}
	}
	var problems []string
	if c.Num < 0 {
		problems = append(problems, fmt.Sprintf("negative config number %d", c.Num))
	}
	// 已编号（>0）的配置必须有 group，否则分片无主。
	if c.Num > 0 && len(c.Groups) == 0 {
		problems = append(problems, fmt.Sprintf("config numbered %d has no groups", c.Num))
	}
	for gid, servers := range c.Groups {
		if len(servers) == 0 {
			problems = append(problems, fmt.Sprintf("gid %d has no servers", gid))
			continue
		}
		for _, addr := range servers {
			if addr == "" {
				problems = append(problems, fmt.Sprintf("gid %d has empty server address", gid))
			}
		}
	}
	// 每个分片必须指向一个存在的 group（除非尚未分配、gid==0 且整体处于初始态）。
	for i := 0; i < NShards; i++ {
		gid := c.Shards[i]
		if gid == 0 {
			continue // 初始未分配：允许
		}
		if _, ok := c.Groups[gid]; !ok {
			problems = append(problems, fmt.Sprintf("shard %d assigned to unknown gid %d", i, gid))
		}
	}
	return problems
}
