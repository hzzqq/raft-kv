package shardmaster

import (
	"fmt"
	"sort"
	"strings"
)

// ConfigFingerprint 返回配置的稳定指纹串：综合 Num、Shards 全量与 Groups
// （gid 升序、各自 server 列表排序后拼接）。相同逻辑配置（即便 Groups 内 server
// 顺序不同、或指向同一份拷贝）必然得到相同指纹；任一维度变化必然改变指纹。
// 用途：① 检测配置是否真正变化（避免「Num 相同但分布变了」的漏判）；
// ② 去重/缓存 key；③ 日志中无脑打印整份配置（含 map）前先打指纹，便于比对变更。
// nil 输入返回 "<nil>"。
func ConfigFingerprint(c *Config) string {
	if c == nil {
		return "<nil>"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "num=%d shards=%v groups=", c.Num, c.Shards)
	gids := GidList(c)
	parts := make([]string, 0, len(gids))
	for _, g := range gids {
		servers := append([]string(nil), c.Groups[g]...)
		sort.Strings(servers)
		parts = append(parts, fmt.Sprintf("%d:[%s]", g, strings.Join(servers, ",")))
	}
	b.WriteString("{" + strings.Join(parts, " ") + "}")
	return b.String()
}
