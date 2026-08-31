// metrics.go —— kvnode 的 Prometheus 指标端点（I152）。
//
// 为什么必须在**节点进程**里出这个端点：跨机/多容器部署时（gateway -connect），
// 各节点由独立的 kvnode 进程承载，gateway 只做 HTTP↔Clerk 翻译、其 s.c.KVs 为空，
// 因此 gateway 的 /metrics 只能给出请求面指标（QPS、延迟、状态码），拿不到任何
// 共识面状态。结果是「leader 切换了几次、哪个副本是 leader、apply 有没有落后」
// 这些最关键的运维信号，在真部署下完全观测不到（此前只有 kvnode 的 JSON /status，
// Prometheus 无法 scrape）。本文件把节点自身的诊断快照翻译成 Prometheus 文本格式，
// 使 docker-compose + Prometheus 可以逐节点 scrape，Grafana 直接出 leader 切换曲线。
//
// 指标一览（前缀 raftkv_）：
//
//	raftkv_node_up{node,kind,group}                   gauge   进程存活哨兵（恒 1）
//	raftkv_raft_term{...}                             gauge   当前任期
//	raftkv_raft_is_leader{...}                        gauge   1=本副本自认 Leader
//	raftkv_raft_has_leader_lease{...}                 gauge   1=近期与多数派有接触
//	raftkv_raft_leader_elections_total{...}           counter 累计赢得选举次数
//	raftkv_raft_commit_index / last_applied / apply_lag / last_log_index  gauge
//	raftkv_raft_health_score{...}                     gauge   0-100 不变量自检分
//	raftkv_shard_config_num{...}                      gauge   已生效配置版本
//	raftkv_shard_owned / pending_in / pending_out     gauge   分片持有与迁移未决（仅 shardkv）
//	raftkv_shard_stall_seconds{...}                   gauge   最长未决卡滞秒数
//	raftkv_shard_health_score{...}                    gauge   0-100 分片层自检分
//
// leader 切换次数的标准查法（每次切换必有且仅有一个副本赢得选举）：
//
//	sum(increase(raftkv_raft_leader_elections_total{group="0"}[5m]))
package main

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"raftkv/src/cluster"
	"raftkv/src/raft"
)

// nodeLabels 从节点名推导 Prometheus 标签集。
// shardkv 节点名形如 "g<gid>-<r>" → group="<gid>", replica="<r>"；
// shardmaster 节点名形如 "m<j>"    → group="sm",    replica="<j>"。
// 解析失败时退化为 group="?"，保证端点永不因命名意外而 500。
func nodeLabels(d cluster.NodeDiagnostics) string {
	group, replica := "?", "?"
	switch {
	case strings.HasPrefix(d.Name, "g"):
		var g, r int
		if _, err := fmt.Sscanf(d.Name, "g%d-%d", &g, &r); err == nil {
			group, replica = strconv.Itoa(g), strconv.Itoa(r)
		}
	case strings.HasPrefix(d.Name, "m"):
		var j int
		if _, err := fmt.Sscanf(d.Name, "m%d", &j); err == nil {
			group, replica = "sm", strconv.Itoa(j)
		}
	}
	return fmt.Sprintf("{node=%q,kind=%q,group=%q,replica=%q}", d.Name, d.Kind, group, replica)
}

// b01 把布尔转成 Prometheus 惯用的 0/1。
func b01(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// metricLine 是单条待输出的指标（同名指标共享 HELP/TYPE，故按 name 分组输出）。
type metricLine struct {
	name  string
	help  string
	typ   string // "gauge" | "counter"
	value float64
}

// nodeMetricLines 把节点诊断快照摊平成指标行。纯函数、零副作用，便于单测直接断言。
func nodeMetricLines(d cluster.NodeDiagnostics) []metricLine {
	out := []metricLine{
		{"raftkv_node_up", "节点进程存活哨兵（恒为 1；与 Prometheus 内建 up 配合可区分 scrape 失败与进程异常）", "gauge", 1},
	}
	if rs := d.Raft; rs != nil {
		lag := rs.CommitIndex - rs.LastApplied
		if lag < 0 {
			lag = 0
		}
		out = append(out,
			metricLine{"raftkv_raft_term", "本副本当前任期（任期快速翻滚意味着选举风暴）", "gauge", float64(rs.Term)},
			metricLine{"raftkv_raft_is_leader", "本副本是否自认 Leader（1=是）；同 group 内长期 sum>1 说明脑裂", "gauge", b01(rs.Role == raft.Leader)},
			metricLine{"raftkv_raft_has_leader_lease", "Leader 是否在选举超时内与多数派保持接触（1=是）；落入少数派时为 0", "gauge", b01(rs.HasLeaderLease)},
			metricLine{"raftkv_raft_leader_elections_total", "本副本累计赢得选举次数；同 group 求和的增量即 leader 切换次数", "counter", float64(rs.LeaderElections)},
			metricLine{"raftkv_raft_commit_index", "已提交日志索引", "gauge", float64(rs.CommitIndex)},
			metricLine{"raftkv_raft_last_applied", "已应用到状态机的日志索引", "gauge", float64(rs.LastApplied)},
			metricLine{"raftkv_raft_apply_lag", "commitIndex - lastApplied，状态机应用落后量（持续 >0 说明 apply 卡住）", "gauge", float64(lag)},
			metricLine{"raftkv_raft_last_log_index", "最后一条日志索引（含快照偏移）", "gauge", float64(rs.LastLogIndex)},
		)
	}
	if d.RaftCheck != nil {
		out = append(out, metricLine{"raftkv_raft_health_score",
			"Raft 层不变量自检分 0-100（diagnostics.RaftCheck），低于 100 即有不变量被破坏", "gauge", float64(d.RaftCheck.Score)})
	}
	if sd := d.Shard; sd != nil {
		out = append(out,
			metricLine{"raftkv_shard_config_num", "本节点已生效的分片配置版本号（各副本长期不一致说明配置推进卡住）", "gauge", float64(sd.ConfigNum)},
			metricLine{"raftkv_shard_owned", "本节点当前持有的分片数", "gauge", float64(len(sd.Owned))},
			metricLine{"raftkv_shard_pending_in", "待接收（迁入未完成）的分片数；残留即冻结配置推进", "gauge", float64(len(sd.PendingIn))},
			metricLine{"raftkv_shard_pending_out", "待迁出（对端未确认）的分片数", "gauge", float64(len(sd.PendingOut))},
			metricLine{"raftkv_shard_stall_seconds", "最长的分片未决卡滞时长（秒）；持续增长即迁移卡死", "gauge", sd.StallSeconds},
		)
	} else if d.Raft != nil {
		// shardmaster 无分片语义，仅补配置版本占位，便于看板统一按 node 聚合。
		out = append(out, metricLine{"raftkv_shard_config_num",
			"本节点已生效的分片配置版本号（各副本长期不一致说明配置推进卡住）", "gauge", float64(d.ConfigNum)})
	}
	if d.ShardCheck != nil {
		out = append(out, metricLine{"raftkv_shard_health_score",
			"分片层不变量自检分 0-100（diagnostics.ShardCheck）", "gauge", float64(d.ShardCheck.Score)})
	}
	return out
}

// writeNodeMetrics 以 Prometheus 文本 exposition 格式输出本节点指标。
// 同名指标只写一次 # HELP/# TYPE（Prometheus 规范要求），行按指标名稳定排序，
// 便于 diff 与人工 curl 阅读。
func writeNodeMetrics(w io.Writer, d cluster.NodeDiagnostics) error {
	labels := nodeLabels(d)
	lines := nodeMetricLines(d)
	sort.SliceStable(lines, func(i, j int) bool { return lines[i].name < lines[j].name })

	var sb strings.Builder
	seen := make(map[string]bool, len(lines))
	for _, m := range lines {
		if !seen[m.name] {
			seen[m.name] = true
			fmt.Fprintf(&sb, "# HELP %s %s\n# TYPE %s %s\n", m.name, m.help, m.name, m.typ)
		}
		// 整数值不带小数点输出，避免看板上出现 3.000000 这类噪音。
		if m.value == float64(int64(m.value)) {
			fmt.Fprintf(&sb, "%s%s %d\n", m.name, labels, int64(m.value))
		} else {
			fmt.Fprintf(&sb, "%s%s %g\n", m.name, labels, m.value)
		}
	}
	_, err := io.WriteString(w, sb.String())
	return err
}
