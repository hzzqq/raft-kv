package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"raftkv/src/cluster"
)

// TestGatewayRaftHealthMetric 验证 /metrics 暴露 raft_min_health_score（来自各副本
// diagnostics.RaftCheck 最低分），使共识健康可经 Prometheus scrape 与阈值告警（I220 延伸）。
// 同时验证 Prometheus 文本与 JSON 两种格式均含该 gauge。
func TestGatewayRaftHealthMetric(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)

	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// Prometheus 文本格式：Accept 含 text/plain。
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/metrics", nil)
	req.Header.Set("Accept", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", resp.StatusCode)
	}
	text := string(body)
	if !strings.Contains(text, "raft_min_health_score") {
		t.Fatalf("/metrics (prometheus) 缺少 raft_min_health_score；body 前 500 字:\n%s", text[:min(500, len(text))])
	}
	// 解析值，断言在 [0,100]。
	var val float64
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "raft_min_health_score ") {
			f, err := strconv.ParseFloat(strings.TrimSpace(line[len("raft_min_health_score "):]), 64)
			if err != nil {
				t.Fatalf("解析 raft_min_health_score 失败: %v (line=%q)", err, line)
			}
			val = f
		}
	}
	if val < 0 || val > 100 {
		t.Fatalf("raft_min_health_score=%v 不在 [0,100]", val)
	}

	// JSON 格式：缺省 Accept，snap["gateway"] 子键应包含该 gauge。
	resp2, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	b2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if !strings.Contains(string(b2), "raft_min_health_score") {
		t.Fatalf("/metrics (json) 的 gateway 子键缺少 raft_min_health_score")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
