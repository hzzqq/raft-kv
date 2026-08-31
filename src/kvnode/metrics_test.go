package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"raftkv/src/cluster"
	"raftkv/src/diagnostics"
	"raftkv/src/raft"
	"raftkv/src/shardkv"
)

// metricsTestConfig 端口独立（19300 段），避免与 main_test/diag_test 并行时抢端口。
const metricsTestConfig = `{
  "n_groups": 1, "n_replicas": 1, "n_sm": 3, "max_raft_state": 0,
  "data_dir": "",
  "nodes": [
    {"name": "m0",   "addr": "127.0.0.1:19300"},
    {"name": "m1",   "addr": "127.0.0.1:19301"},
    {"name": "m2",   "addr": "127.0.0.1:19302"},
    {"name": "g0-0", "addr": "127.0.0.1:19310"}
  ]
}`

func writeMetricsConfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "deploy.json")
	if err := os.WriteFile(p, []byte(metricsTestConfig), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return p
}

// metricValue 从 Prometheus 文本里取出指定指标名的第一个样本值（原样字符串）。
// 找不到返回 ""，便于断言"某指标不应出现"。
func metricValue(body, name string) string {
	for _, ln := range strings.Split(body, "\n") {
		if strings.HasPrefix(ln, "#") || !strings.HasPrefix(ln, name) {
			continue
		}
		// 必须是完整指标名（避免 raftkv_shard_owned 命中 raftkv_shard_owned_xxx）
		rest := ln[len(name):]
		if rest == "" || (rest[0] != '{' && rest[0] != ' ') {
			continue
		}
		if i := strings.LastIndex(ln, " "); i >= 0 {
			return ln[i+1:]
		}
	}
	return ""
}

// TestNodeLabelsFromName 验证标签推导：shardkv 名映射到 group/replica 数字，
// shardmaster 映射到 group="sm"，非法名不 panic 而是退化为 "?"。
// 标签值错了看板会按错误维度聚合（例如把两个 group 的 leader 计数混算），
// 这类错误运行时不报错、只出错误结论，故必须单测钉住。
func TestNodeLabelsFromName(t *testing.T) {
	cases := []struct {
		name, kind string
		wantParts  []string
	}{
		{"g0-2", "shardkv", []string{`node="g0-2"`, `kind="shardkv"`, `group="0"`, `replica="2"`}},
		{"g3-0", "shardkv", []string{`group="3"`, `replica="0"`}},
		{"m1", "shardmaster", []string{`node="m1"`, `group="sm"`, `replica="1"`}},
		{"weird", "unknown", []string{`group="?"`, `replica="?"`}},
	}
	for _, c := range cases {
		got := nodeLabels(cluster.NodeDiagnostics{Name: c.name, Kind: c.kind})
		for _, want := range c.wantParts {
			if !strings.Contains(got, want) {
				t.Errorf("nodeLabels(%q) = %s, 缺少 %s", c.name, got, want)
			}
		}
	}
}

// TestNodeMetricLinesShardKV 验证 ShardKV 快照被完整摊平，且 apply_lag 由
// commit-applied 算出（这是"apply 卡住"的唯一可观测信号）。
func TestNodeMetricLinesShardKV(t *testing.T) {
	rs := raft.RaftStatus{
		Role: raft.Leader, Term: 7, CommitIndex: 120, LastApplied: 100,
		LastLogIndex: 130, HasLeaderLease: true, LeaderElections: 3,
	}
	sd := shardkv.ShardDebug{
		GID: 1, Leader: true, ConfigNum: 5,
		Owned: []int{0, 1, 2}, PendingIn: []int{7}, PendingOut: []int{8, 9},
		StallSeconds: 2.5,
	}
	rc := diagnostics.Diagnosis{Score: 100}
	sc := diagnostics.Diagnosis{Score: 80}
	d := cluster.NodeDiagnostics{
		Name: "g0-1", Kind: "shardkv", ConfigNum: 5,
		Raft: &rs, Shard: &sd, RaftCheck: &rc, ShardCheck: &sc,
	}

	want := map[string]float64{
		"raftkv_node_up":                     1,
		"raftkv_raft_term":                   7,
		"raftkv_raft_is_leader":              1,
		"raftkv_raft_has_leader_lease":       1,
		"raftkv_raft_leader_elections_total": 3,
		"raftkv_raft_commit_index":           120,
		"raftkv_raft_last_applied":           100,
		"raftkv_raft_apply_lag":              20,
		"raftkv_raft_last_log_index":         130,
		"raftkv_raft_health_score":           100,
		"raftkv_shard_config_num":            5,
		"raftkv_shard_owned":                 3,
		"raftkv_shard_pending_in":            1,
		"raftkv_shard_pending_out":           2,
		"raftkv_shard_stall_seconds":         2.5,
		"raftkv_shard_health_score":          80,
	}
	got := make(map[string]float64)
	for _, m := range nodeMetricLines(d) {
		if _, dup := got[m.name]; dup {
			t.Errorf("指标 %s 重复出现（同名多样本会让 Prometheus 拒绝整次 scrape）", m.name)
		}
		got[m.name] = m.value
	}
	for name, wv := range want {
		gv, ok := got[name]
		if !ok {
			t.Errorf("缺少指标 %s", name)
			continue
		}
		if gv != wv {
			t.Errorf("%s = %v, want %v", name, gv, wv)
		}
	}
	// leader_elections 必须声明为 counter，否则 Grafana 里 increase() 语义不成立
	for _, m := range nodeMetricLines(d) {
		if m.name == "raftkv_raft_leader_elections_total" && m.typ != "counter" {
			t.Errorf("raftkv_raft_leader_elections_total 类型 = %q, want counter", m.typ)
		}
	}
}

// TestNodeMetricLinesFollowerNoLease 验证非 leader 副本的布尔位为 0，
// 且 applied 超过 commit 的瞬时快照不会算出负的 lag（负值会污染看板 Y 轴）。
func TestNodeMetricLinesFollowerNoLease(t *testing.T) {
	rs := raft.RaftStatus{Role: raft.Follower, Term: 9, CommitIndex: 50, LastApplied: 51}
	d := cluster.NodeDiagnostics{Name: "g0-2", Kind: "shardkv", Raft: &rs}
	got := make(map[string]float64)
	for _, m := range nodeMetricLines(d) {
		got[m.name] = m.value
	}
	if got["raftkv_raft_is_leader"] != 0 {
		t.Errorf("follower 的 is_leader = %v, want 0", got["raftkv_raft_is_leader"])
	}
	if got["raftkv_raft_has_leader_lease"] != 0 {
		t.Errorf("follower 的 has_leader_lease = %v, want 0", got["raftkv_raft_has_leader_lease"])
	}
	if got["raftkv_raft_apply_lag"] != 0 {
		t.Errorf("apply_lag = %v, want 0（不得为负）", got["raftkv_raft_apply_lag"])
	}
}

// TestWriteNodeMetricsFormat 验证输出符合 Prometheus 文本规范：每个指标名恰好
// 一行 # HELP 与一行 # TYPE，样本行带完整标签。规范违例会让 Prometheus 直接
// 丢弃整次 scrape（表现为看板无数据却无明显报错），故必须钉住。
func TestWriteNodeMetricsFormat(t *testing.T) {
	rs := raft.RaftStatus{Role: raft.Leader, Term: 2, CommitIndex: 10, LastApplied: 10}
	rc := diagnostics.Diagnosis{Score: 100}
	d := cluster.NodeDiagnostics{Name: "m0", Kind: "shardmaster", ConfigNum: 1, Raft: &rs, RaftCheck: &rc}

	var sb strings.Builder
	if err := writeNodeMetrics(&sb, d); err != nil {
		t.Fatalf("writeNodeMetrics: %v", err)
	}
	body := sb.String()

	helpCount, typeCount := map[string]int{}, map[string]int{}
	for _, ln := range strings.Split(body, "\n") {
		f := strings.Fields(ln)
		if len(f) < 3 {
			continue
		}
		if strings.HasPrefix(ln, "# HELP ") {
			helpCount[f[2]]++
		}
		if strings.HasPrefix(ln, "# TYPE ") {
			typeCount[f[2]]++
		}
	}
	if len(helpCount) == 0 {
		t.Fatalf("没有任何 # HELP 行:\n%s", body)
	}
	for name, n := range helpCount {
		if n != 1 {
			t.Errorf("%s 有 %d 行 # HELP, want 1", name, n)
		}
		if typeCount[name] != 1 {
			t.Errorf("%s 有 %d 行 # TYPE, want 1", name, typeCount[name])
		}
	}
	// shardmaster 无分片语义：不应出现分片持有/迁移指标
	for _, absent := range []string{"raftkv_shard_owned", "raftkv_shard_pending_in", "raftkv_shard_health_score"} {
		if v := metricValue(body, absent); v != "" {
			t.Errorf("shardmaster 不应暴露 %s（得到 %s）", absent, v)
		}
	}
	// 样本行必须带 group="sm" 标签
	if !strings.Contains(body, `raftkv_raft_term{node="m0",kind="shardmaster",group="sm",replica="0"}`) {
		t.Errorf("样本行标签不正确:\n%s", body)
	}
	// 整数不带小数点（看板可读性）
	if strings.Contains(body, "raftkv_raft_term") && strings.Contains(body, "term} 2.0") {
		t.Errorf("整数值被写成浮点:\n%s", body)
	}
}

// TestMetricsEndpointLive 端到端验证：真起一个节点，GET /metrics 返回
// Prometheus Content-Type 与真实采集值。这条覆盖 diagHandler 的注册与
// node.Diagnostics() → 文本 的完整链路（跨机部署时 Prometheus 就是这么拉的）。
func TestMetricsEndpointLive(t *testing.T) {
	_, node, err := startNode(writeMetricsConfig(t), "g0-0")
	if err != nil {
		t.Fatalf("startNode g0-0: %v", err)
	}
	defer node.Stop()

	ts := httptest.NewServer(diagHandler(node))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want 含 text/plain（否则 Prometheus 不解析）", ct)
	}
	code, body := httpGet(t, ts, "/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics = %d", code)
	}
	// 单副本 group 选不出 leader（这正是最需要观测的异常态）：端点必须仍能出数
	for _, name := range []string{
		"raftkv_node_up", "raftkv_raft_term", "raftkv_raft_is_leader",
		"raftkv_raft_leader_elections_total", "raftkv_raft_apply_lag",
		"raftkv_shard_owned", "raftkv_raft_health_score",
	} {
		if v := metricValue(body, name); v == "" {
			t.Errorf("缺少指标 %s:\n%s", name, body)
		}
	}
	if v := metricValue(body, "raftkv_node_up"); v != "1" {
		t.Errorf("raftkv_node_up = %q, want 1", v)
	}
}
