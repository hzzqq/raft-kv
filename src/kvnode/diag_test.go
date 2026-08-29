package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// diagTestConfig 与 main_test.go 的配置同形，但端口独立（19200 段），避免两份测试
// 并行时抢占同一监听端口。
const diagTestConfig = `{
  "n_groups": 1, "n_replicas": 1, "n_sm": 3, "max_raft_state": 0,
  "data_dir": "",
  "nodes": [
    {"name": "m0",   "addr": "127.0.0.1:19200"},
    {"name": "m1",   "addr": "127.0.0.1:19201"},
    {"name": "m2",   "addr": "127.0.0.1:19202"},
    {"name": "g0-0", "addr": "127.0.0.1:19210"}
  ]
}`

func writeDiagConfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "deploy.json")
	if err := os.WriteFile(p, []byte(diagTestConfig), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return p
}

// httpGet 发一次请求并返回 (状态码, body)。
func httpGet(t *testing.T, ts *httptest.Server, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(ts.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return resp.StatusCode, string(b)
}

// diagView 按 NodeDiagnostics 的实际 JSON 形状解析（PascalCase，沿用 ShardDebugView 惯例）。
type diagView struct {
	Name      string
	Kind      string
	ConfigNum int
	RaftRole  string
	Raft      *struct {
		Term           int
		CommitIndex    int
		LastApplied    int
		HasLeaderLease bool
	}
	Shard *struct {
		GID          int
		Leader       bool
		Owned        []int
		PendingIn    []int
		PendingOut   []int
		StallSeconds float64
	}
	RaftCheck  *struct{ Score int }
	ShardCheck *struct{ Score int }
}

func parseStatus(t *testing.T, ts *httptest.Server) diagView {
	t.Helper()
	code, body := httpGet(t, ts, "/status")
	if code != http.StatusOK {
		t.Fatalf("/status = %d, want 200", code)
	}
	var d diagView
	if err := json.Unmarshal([]byte(body), &d); err != nil {
		t.Fatalf("/status 不是合法 JSON: %v\nbody=%s", err, body)
	}
	return d
}

// TestDiagHandlerShardMaster 验证 ShardMaster 节点的诊断端点：存活探针可用、
// /status 是合法 JSON 且 kind 正确、Raft 状态被真实采集。
//
// 本用例只起 3 个 ShardMaster 中的 1 个，故该副本选不出 leader —— 这正是要覆盖的
// 场景：运维最需要诊断的恰恰是「无法达成共识」的异常节点，此时端点必须仍能响应，
// 而不是随 Raft 一起卡死。
func TestDiagHandlerShardMaster(t *testing.T) {
	_, node, err := startNode(writeDiagConfig(t), "m0")
	if err != nil {
		t.Fatalf("startNode m0: %v", err)
	}
	defer node.Stop()

	ts := httptest.NewServer(diagHandler(node))
	defer ts.Close()

	if code, body := httpGet(t, ts, "/healthz"); code != http.StatusOK || !strings.Contains(body, "ok") {
		t.Fatalf("/healthz = %d %q, want 200 + \"ok\"", code, body)
	}

	d := parseStatus(t, ts)
	if d.Name != "m0" {
		t.Errorf("Name = %q, want m0", d.Name)
	}
	if d.Kind != "shardmaster" {
		t.Errorf("Kind = %q, want shardmaster", d.Kind)
	}
	if d.Raft == nil {
		t.Fatal("Raft 字段缺失：ShardMaster 的 Raft 状态未被采集")
	}
	// 角色必须是可读文字（raft.Role 是 int，若忘了转换这里会是空串）
	switch d.RaftRole {
	case "Follower", "Candidate", "Leader":
	default:
		t.Errorf("RaftRole = %q, want Follower/Candidate/Leader（int 未转文字？）", d.RaftRole)
	}
	// ShardMaster 不持有分片，Shard / ShardCheck 应被 omitempty 省略
	if d.Shard != nil {
		t.Errorf("shardmaster 不应有 Shard 字段")
	}
	if d.ShardCheck != nil {
		t.Errorf("shardmaster 不应有 ShardCheck 字段")
	}
	if d.RaftCheck == nil {
		t.Error("RaftCheck 缺失：diagnostics 不变量自检未接入")
	}

	// 一行式摘要（人工 curl 巡检用）
	if code, body := httpGet(t, ts, "/"); code != http.StatusOK ||
		!strings.Contains(body, "name=m0") || !strings.Contains(body, "kind=shardmaster") {
		t.Errorf("GET / = %d %q, want 含 name=m0 kind=shardmaster", code, body)
	}
}

// TestDiagHandlerShardKV 验证 group 副本节点会额外暴露分片持有/迁移状态
// （Shard + ShardCheck），这是判断「数据卡在迁移中」的关键信息。
func TestDiagHandlerShardKV(t *testing.T) {
	_, node, err := startNode(writeDiagConfig(t), "g0-0")
	if err != nil {
		t.Fatalf("startNode g0-0: %v", err)
	}
	defer node.Stop()

	ts := httptest.NewServer(diagHandler(node))
	defer ts.Close()

	d := parseStatus(t, ts)
	if d.Kind != "shardkv" {
		t.Errorf("Kind = %q, want shardkv", d.Kind)
	}
	if d.Raft == nil {
		t.Error("Raft 字段缺失")
	}
	if d.Shard == nil {
		t.Fatal("Shard 字段缺失：ShardKV 的分片状态未被采集")
	}
	// StartNodeTCP 以 g+1 作为 gid（见 MakeShardKV(g+1, ...)），故 g0-0 → gid 1
	if d.Shard.GID != 1 {
		t.Errorf("Shard.GID = %d, want 1（StartNodeTCP 的 gid=g+1）", d.Shard.GID)
	}
	if d.ShardCheck == nil {
		t.Error("ShardCheck 缺失：分片层不变量自检未接入")
	}

	if code, body := httpGet(t, ts, "/"); code != http.StatusOK || !strings.Contains(body, "gid=1") {
		t.Errorf("GET / = %d %q, want 含 gid=1", code, body)
	}
}

// TestDiagHandlerUnknownPath 验证未注册路径不会打挂节点：巡检脚本敲错路径时
// 绝不能返回 5xx 或让进程崩溃。
func TestDiagHandlerUnknownPath(t *testing.T) {
	_, node, err := startNode(writeDiagConfig(t), "m1")
	if err != nil {
		t.Fatalf("startNode m1: %v", err)
	}
	defer node.Stop()

	ts := httptest.NewServer(diagHandler(node))
	defer ts.Close()

	if code, _ := httpGet(t, ts, "/no-such-endpoint"); code >= 500 {
		t.Errorf("GET /no-such-endpoint = %d, 不应 5xx", code)
	}
}
