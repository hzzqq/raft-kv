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
	health := "HEALTHY"
	if !st.Healthy {
		health = "STALLED"
	}
	fmt.Printf("cluster: %s  latest_config=%d\n", health, st.MaxConfigNum)
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
		fmt.Printf("  group %d leader=%-4s config=%-3d owned=%d pendingIn=%v pendingOut=%v incoming=%v%s\n",
			g.Group, leader, g.ConfigNum, g.OwnedCount, pi, po, inc, stall)
	}
}
