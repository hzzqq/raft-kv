// diagnostics —— 把 shardmaster / shardkv 的若干纯函数判定聚合成一份人类可读的
// 配置健康诊断。不引入任何集群/raft 依赖，仅组合已存在的导出判定，便于
// 网关 /status、运维巡检、单元测试统一口径给出「配置是否可上线 / 是否均衡 / 是否损坏」。
package diagnostics

import (
	"encoding/json"
	"fmt"

	"raftkv/src/shardmaster"
)

// Diagnosis 是一份配置诊断结果：Score 为 0-100 健康分（越高越好）；
// Issues 为可读问题列表（空=健康）。
type Diagnosis struct {
	Score  int
	Issues []string
}

// DiagnoseConfig 综合校验一份配置的内部一致性、分片覆盖度与负载均衡度，
// 产出健康分与问题清单。扣分项：
//   - 每个内部一致性违规（ValidateConfig）：-20（封顶到 0）；
//   - 每个未覆盖分片（ConfigShardCoverage.uncovered）：-5（封顶 30）；
//   - 不均衡（IsBalanced=false）：再按 BalanceGap 额外 -gap*5（封顶 30）。
//
// nil 输入直接判 0 分并标注 "config is nil"。纯函数、零副作用，可直接单测。
func DiagnoseConfig(c *shardmaster.Config) Diagnosis {
	if c == nil {
		return Diagnosis{Score: 0, Issues: []string{"config is nil"}}
	}
	issues := make([]string, 0)
	score := 100

	if probs := shardmaster.ValidateConfig(c); len(probs) > 0 {
		for _, p := range probs {
			issues = append(issues, "invalid: "+p)
		}
		score -= 20 * len(probs)
	}

	covered, uncovered := shardmaster.ConfigShardCoverage(c)
	if uncovered > 0 {
		issues = append(issues, fmt.Sprintf("uncovered shards: %d/%d not assigned or orphaned", uncovered, shardmaster.NShards))
		score -= 5 * uncovered
	}
	_ = covered

	if !shardmaster.IsBalanced(c) {
		gap := shardmaster.BalanceGap(c)
		issues = append(issues, fmt.Sprintf("unbalanced: shard-count gap between gids is %d (want <=1)", gap))
		score -= 5 * gap
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

// Level 把 0-100 健康分映射为机器可读的严重度等级，供自动化按健康度门禁
//（如 CI/巡检脚本依等级决定是否阻断发布）：critical(<50) / warn(<80) / ok(>=80)。
func (d Diagnosis) Level() string {
	switch {
	case d.Score < 50:
		return "critical"
	case d.Score < 80:
		return "warn"
	default:
		return "ok"
	}
}

// JSON 以机器可读形态导出诊断结果（含 level 维度），便于被 Prometheus/CI/巡检脚本
// 直接消费做自动化门禁，而无需重新解析人类可读文本。
func (d Diagnosis) JSON() ([]byte, error) {
	return json.Marshal(struct {
		Score  int      `json:"score"`
		Level  string   `json:"level"`
		Issues []string `json:"issues"`
	}{
		Score:  d.Score,
		Level:  d.Level(),
		Issues: d.Issues,
	})
}
