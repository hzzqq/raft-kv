package diagnostics

import (
	"fmt"

	"raftkv/src/shardmaster"
)

// SelfCheck 对一份配置历史（configs[0..n]，通常即 ShardMaster.configs）做端到端自检：
// 首份编号须为 0 且结构合法；此后每份须满足 IsValidTransition（编号严格 +1 且结构合法）。
// 任何一步失败都扣分并标注，便于运维在「配置链损坏 / 跳号 / 孤儿分片」时快速定位。
// 空历史直接判 0 分。纯函数、零副作用，可直接单测。
func SelfCheck(configs []shardmaster.Config) Diagnosis {
	issues := make([]string, 0)
	score := 100
	if len(configs) == 0 {
		return Diagnosis{Score: 0, Issues: []string{"empty config history"}}
	}
	prev := &configs[0]
	if prev.Num != 0 {
		issues = append(issues, fmt.Sprintf("first config num should be 0, got %d", prev.Num))
		score -= 20
	}
	if !prev.Valid() {
		issues = append(issues, "first config structure invalid")
		score -= 20
	}
	for i := 1; i < len(configs); i++ {
		cur := &configs[i]
		ok, why := prev.IsValidTransition(cur)
		if !ok {
			issues = append(issues, fmt.Sprintf("transition %d->%d invalid: %s", prev.Num, cur.Num, why))
			score -= 20
		}
		prev = cur
	}
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	if len(issues) == 0 {
		issues = []string{"ok"}
	}
	return Diagnosis{Score: score, Issues: issues}
}
