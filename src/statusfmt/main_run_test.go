// main_run_test.go —— run()/渲染模式/退出码的 cluster-free 单测。
package main

import (
	"encoding/json"
	"strings"
	"testing"
)

const healthyJSON = `{"healthy":true,"max_config_num":4,"groups":[
  {"group":1,"has_leader":true,"leader_replica":0,"config_num":4,"owned_count":5},
  {"group":2,"has_leader":true,"leader_replica":1,"config_num":4,"owned_count":5}]}`

const stalledJSON = `{"healthy":false,"max_config_num":2,"groups":[
  {"group":1,"has_leader":false,"config_num":2,"owned_count":10,"stall_seconds":12.5}]}`

// TestRunHumanHealthy 验证：健康集群 → 退出码 0，输出含评分摘要两行。
func TestRunHumanHealthy(t *testing.T) {
	var out strings.Builder
	code, err := run(strings.NewReader(healthyJSON), &out, false)
	if err != nil || code != 0 {
		t.Fatalf("expected (0,nil), got (%d,%v)", code, err)
	}
	s := out.String()
	if !strings.Contains(s, "health_score=") || !strings.Contains(s, "balance_score=") {
		t.Fatalf("expected score summary lines:\n%s", s)
	}
	if !strings.Contains(s, "HEALTHY") {
		t.Fatalf("expected HEALTHY:\n%s", s)
	}
}

// TestRunHumanStalledExitCode 验证：非健康集群 → 退出码 2（供 -check/CI 探活）。
func TestRunHumanStalledExitCode(t *testing.T) {
	var out strings.Builder
	code, err := run(strings.NewReader(stalledJSON), &out, false)
	if err != nil || code != 2 {
		t.Fatalf("expected (2,nil), got (%d,%v)", code, err)
	}
}

// TestRunJSONMode 验证：-json 输出可解析报告，字段齐全且数值合理。
func TestRunJSONMode(t *testing.T) {
	var out strings.Builder
	code, err := run(strings.NewReader(healthyJSON), &out, true)
	if err != nil || code != 0 {
		t.Fatalf("expected (0,nil), got (%d,%v)", code, err)
	}
	var r map[string]interface{}
	if err := json.Unmarshal([]byte(out.String()), &r); err != nil {
		t.Fatalf("json output not parseable: %v\n%s", err, out.String())
	}
	for _, k := range []string{"healthy", "max_config_num", "health_score", "balance_score", "health_summary", "balance_detail", "group_count"} {
		if _, ok := r[k]; !ok {
			t.Fatalf("missing key %q in report: %v", k, r)
		}
	}
	if r["health_score"].(float64) != 100 {
		t.Fatalf("expected health_score=100, got %v", r["health_score"])
	}
	if r["group_count"].(float64) != 2 {
		t.Fatalf("expected group_count=2, got %v", r["group_count"])
	}
}

// TestRunJSONModeStalled 验证：JSON 模式下非健康同样返回退出码 2。
func TestRunJSONModeStalled(t *testing.T) {
	var out strings.Builder
	code, _ := run(strings.NewReader(stalledJSON), &out, true)
	if code != 2 {
		t.Fatalf("expected exit 2 for stalled cluster, got %d", code)
	}
}

// TestRunPassthroughNonJSON 验证：非 JSON 输入原样透传且退出码 0（不判健康）。
func TestRunPassthroughNonJSON(t *testing.T) {
	var out strings.Builder
	code, err := run(strings.NewReader("gateway not running\n"), &out, false)
	if err != nil || code != 0 {
		t.Fatalf("expected (0,nil), got (%d,%v)", code, err)
	}
	if out.String() != "gateway not running\n" {
		t.Fatalf("expected passthrough, got %q", out.String())
	}
}
