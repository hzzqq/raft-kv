package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"raftkv/src/cluster"
)

// TestGatewayDebugLinearizability 验证 GET /debug/linearizability 的契约：200 + 合法 JSON，
// 含 ok/checked/violations/detail 四字段，且 detail 非空、checked>=0、violations>=0；
// 金标准正例 + 集成自检通过 ⇒ ok=true、violations=0。
func TestGatewayDebugLinearizability(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)

	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/debug/linearizability")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/linearizability = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		OK         bool   `json:"ok"`
		Checked    int    `json:"checked"`
		Violations int    `json:"violations"`
		Detail     string `json:"detail"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("GET /debug/linearizability body is not valid JSON: %v (body=%s)", err, string(body))
	}
	if out.Detail == "" {
		t.Fatalf("detail must not be empty")
	}
	if out.Checked < 0 || out.Violations < 0 {
		t.Fatalf("checked/violations must be >=0, got checked=%d violations=%d", out.Checked, out.Violations)
	}
	// 金标准正例 + 集成自检通过 ⇒ 应报告 ok=true、violations=0。
	if !out.OK {
		t.Fatalf("expected ok=true for golden-correct history, got false (detail=%s)", out.Detail)
	}
	if out.Violations != 0 {
		t.Fatalf("expected 0 violations for golden-correct history, got %d", out.Violations)
	}
}
