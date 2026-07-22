// statusfmt —— 把网关 /status 的 JSON 渲染成人类可读的集群健康总览。
//
// 用法：curl -s localhost:8080/status | go run ./src/statusfmt
// 作为 `start.sh status` 的子命令后端，避免依赖 jq/python。
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

type groupStatus struct {
	Group         int     `json:"group"`
	HasLeader     bool    `json:"has_leader"`
	LeaderReplica int     `json:"leader_replica"`
	ConfigNum     int     `json:"config_num"`
	OwnedCount    int     `json:"owned_count"`
	PendingIn     []int   `json:"pending_in"`
	PendingOut    []int   `json:"pending_out"`
	Incoming      []int   `json:"incoming"`
	StallSeconds  float64 `json:"stall_seconds"`
}

type clusterStatus struct {
	Groups       []groupStatus `json:"groups"`
	Healthy      bool          `json:"healthy"`
	MaxConfigNum int           `json:"max_config_num"`
}

func main() {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read error:", err)
		os.Exit(1)
	}
	var st clusterStatus
	if err := json.Unmarshal(data, &st); err != nil {
		// 非 JSON（如网关未启动的错误文本）→ 原样透传。
		fmt.Print(string(data))
		return
	}
	fmt.Print(formatClusterStatus(st))
}

// formatClusterStatus 把集群状态渲染为人类可读总览字符串（末尾含换行）。
// 从 main 抽离以便单测；对 nil 切片做归一（显示 [] 而非 <nil>），保证输出稳定可读。
func formatClusterStatus(st clusterStatus) string {
	var b strings.Builder
	health := "HEALTHY"
	if !st.Healthy {
		health = "STALLED"
	}
	fmt.Fprintf(&b, "cluster: %s  latest_config=%d\n", health, st.MaxConfigNum)
	for _, g := range st.Groups {
		leader := "none"
		if g.HasLeader {
			leader = fmt.Sprintf("r%d", g.LeaderReplica)
		}
		pi, po, inc := g.PendingIn, g.PendingOut, g.Incoming
		if pi == nil {
			pi = []int{}
		}
		if po == nil {
			po = []int{}
		}
		if inc == nil {
			inc = []int{}
		}
		stall := ""
		if g.StallSeconds > 1.0 {
			stall = fmt.Sprintf("  <-- STALL %.1fs", g.StallSeconds)
		}
		fmt.Fprintf(&b, "  group %d leader=%-4s config=%-3d owned=%d pendingIn=%v pendingOut=%v incoming=%v%s\n",
			g.Group, leader, g.ConfigNum, g.OwnedCount, pi, po, inc, stall)
	}
	return b.String()
}

// clusterHealthScore 把集群状态折算为 0-100 健康分与一项摘要（纯函数，便于单测/阈值告警）。
// 维度：① 有 leader 的 group 占比（无主=严重，权重最高）；② 平均 stall 秒数
// （越高越差，每 10s 扣 10 分，封顶 50）；③ pending 积压（in/out/incoming
// 总条目，每 10 条扣 5 分，封顶 30）。三项取加权和并钳制到 [0,100]。
func clusterHealthScore(st clusterStatus) (score float64, summary string) {
	if len(st.Groups) == 0 {
		return 0, "no groups"
	}
	var leaderOK int
	var stallSum float64
	var backlog int
	for _, g := range st.Groups {
		if g.HasLeader {
			leaderOK++
		}
		stallSum += g.StallSeconds
		backlog += len(g.PendingIn) + len(g.PendingOut) + len(g.Incoming)
	}
	leaderRatio := float64(leaderOK) / float64(len(st.Groups))
	avgStall := stallSum / float64(len(st.Groups))

	stallPenalty := avgStall / 10.0 * 10.0
	if stallPenalty > 50 {
		stallPenalty = 50
	}
	backlogPenalty := float64(backlog) / 10.0 * 5.0
	if backlogPenalty > 30 {
		backlogPenalty = 30
	}
	score = 100*leaderRatio - stallPenalty - backlogPenalty
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	summary = fmt.Sprintf("%.0f%% groups healthy, avg_stall=%.1fs, backlog=%d",
		leaderRatio*100, avgStall, backlog)
	return score, summary
}

// shardBalance 量化分片在各 group 间的均衡度，返回 0-100 分（100=完全均衡）。
// 用「最大拥有量与最小拥有量之差」相对总分片的占比衡量失衡，便于单测
// 与 rebalance 前后对比（失衡下降即改善）。纯函数，无副作用。
func shardBalance(st clusterStatus) (score float64, detail string) {
	if len(st.Groups) == 0 {
		return 0, "no groups"
	}
	var counts []int
	total := 0
	for _, g := range st.Groups {
		counts = append(counts, g.OwnedCount)
		total += g.OwnedCount
	}
	if total == 0 {
		return 100, "no shards assigned"
	}
	min, max := counts[0], counts[0]
	for _, c := range counts {
		if c < min {
			min = c
		}
		if c > max {
			max = c
		}
	}
	dev := max - min
	score = 100 - float64(dev)*100.0/float64(total)
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}
	detail = fmt.Sprintf("min=%d max=%d total=%d", min, max, total)
	return score, detail
}
