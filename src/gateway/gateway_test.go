// gateway_test.go —— 用 httptest 覆盖网关的三种操作
package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"raftkv/src/cluster"
)

func TestGatewayHTTP(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)

	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// PUT /kv/foo = bar
	putReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/foo", strings.NewReader("bar"))
	resp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	// GET /kv/foo -> bar
	resp2, err := http.Get(ts.URL + "/kv/foo")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if string(b) != "bar" {
		t.Fatalf("GET /kv/foo = %q, want \"bar\"", string(b))
	}

	// POST /kv/foo/append = -baz
	resp3, err := http.Post(ts.URL+"/kv/foo/append", "text/plain", strings.NewReader("-baz"))
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()

	resp4, _ := http.Get(ts.URL + "/kv/foo")
	b4, _ := io.ReadAll(resp4.Body)
	resp4.Body.Close()
	if string(b4) != "bar-baz" {
		t.Fatalf("GET /kv/foo after append = %q, want \"bar-baz\"", string(b4))
	}

	// GET /healthz -> 200
	h, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	if h.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", h.StatusCode)
	}
	h.Body.Close()

	// GET /metrics -> 200 + valid JSON containing "counters"
	m, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	if m.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", m.StatusCode)
	}
	mb, _ := io.ReadAll(m.Body)
	m.Body.Close()
	var parsed map[string]interface{}
	if err := json.Unmarshal(mb, &parsed); err != nil {
		t.Fatalf("GET /metrics body is not valid JSON: %v (body=%s)", err, string(mb))
	}
	if _, ok := parsed["counters"]; !ok {
		t.Fatalf("GET /metrics JSON missing \"counters\" key: %s", string(mb))
	}
	if _, ok := parsed["histograms"]; !ok {
		t.Fatalf("GET /metrics JSON missing \"histograms\" key: %s", string(mb))
	}

	// GET /debug/shards -> 200 + valid JSON array of per-replica shard state.
	ds, err := http.Get(ts.URL + "/debug/shards")
	if err != nil {
		t.Fatal(err)
	}
	if ds.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/shards = %d, want 200", ds.StatusCode)
	}
	dsb, _ := io.ReadAll(ds.Body)
	ds.Body.Close()
	var views []ShardDebugView
	if err := json.Unmarshal(dsb, &views); err != nil {
		t.Fatalf("GET /debug/shards body is not valid JSON: %v (body=%s)", err, string(dsb))
	}
	if len(views) == 0 {
		t.Fatalf("GET /debug/shards returned empty array")
	}
	// Init 后配置应已应用：至少一个副本 ConfigNum >= 1。
	foundApplied := false
	for _, v := range views {
		if v.ConfigNum >= 1 {
			foundApplied = true
		}
	}
	if !foundApplied {
		t.Fatalf("GET /debug/shards: no replica has applied a config: %s", string(dsb))
	}
}

// TestGatewayObservability：验证新增的 /status（集群健康 JSON）与 /debug/migrate
// （迁移进度文本）端点可用且返回预期结构。
func TestGatewayObservability(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// GET /status -> 200 + valid JSON，Healthy 应为 true（刚 Init 完，无卡滞）。
	st, err := http.Get(ts.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	if st.StatusCode != http.StatusOK {
		t.Fatalf("GET /status = %d, want 200", st.StatusCode)
	}
	stb, _ := io.ReadAll(st.Body)
	st.Body.Close()
	var cs ClusterStatus
	if err := json.Unmarshal(stb, &cs); err != nil {
		t.Fatalf("GET /status body is not valid JSON: %v (body=%s)", err, string(stb))
	}
	if !cs.Healthy {
		t.Fatalf("GET /status Healthy = false, want true (body=%s)", string(stb))
	}
	if len(cs.Groups) != 2 {
		t.Fatalf("GET /status groups = %d, want 2", len(cs.Groups))
	}

	// GET /debug/migrate -> 200 + 文本含 "latest config="。
	mg, err := http.Get(ts.URL + "/debug/migrate")
	if err != nil {
		t.Fatal(err)
	}
	if mg.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/migrate = %d, want 200", mg.StatusCode)
	}
	mgb, _ := io.ReadAll(mg.Body)
	mg.Body.Close()
	if !strings.Contains(string(mgb), "latest config=") {
		t.Fatalf("GET /debug/migrate missing 'latest config=' line: %s", string(mgb))
	}
}

// TestGatewayFailFast：杀掉集群所有副本后，网关应在有界重试后快速失败（返回 5xx）
// 而非无限挂起。验证 Clerk 的 GetE 有界重试 + 网关的错误->HTTP 状态码映射在集群
// 不可达时生效（否则遇到 3-group 再平衡冻结会让 HTTP 请求永久挂死）。
func TestGatewayFailFast(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// 先写入一个值，确保配置已应用、集群健康。
	putReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/fast", strings.NewReader("v"))
	if pr, err := http.DefaultClient.Do(putReq); err != nil {
		t.Fatal(err)
	} else {
		pr.Body.Close()
	}

	// 通过 labrpc 网络把每个 KV 副本标记为不可达：调用在 callWithTimeout 内超时，
	// Clerk 有界重试 5s 后返回 ErrTimeout -> 网关 504，快速失败而非挂起。
	for g := range c.KVs {
		for r := range c.KVs[g] {
			c.Net.Enable(1000+g*100+r, false)
		}
	}

	start := time.Now()
	g, err := http.Get(ts.URL + "/kv/fast")
	if err != nil {
		t.Fatal(err)
	}
	code := g.StatusCode
	g.Body.Close()
	elapsed := time.Since(start)

	if code != http.StatusServiceUnavailable && code != http.StatusGatewayTimeout {
		t.Fatalf("GET after killing cluster = %d, want 503 or 504", code)
	}
	if elapsed > 8*time.Second {
		t.Fatalf("gateway did not fail fast: took %v (want < 8s)", elapsed)
	}
}

// TestGatewayConfigs：GET /debug/configs 返回 shardmaster 完整配置历史，且最新配置号
// 与集群已应用配置一致、每段配置含分片归属。用于复盘 rebalance 轨迹（排查冻结时确认
// 分片在哪些 group 间迁移）。
func TestGatewayConfigs(t *testing.T) {
	c := cluster.StartCluster(3, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(3)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// 触发一次 rebalance，制造多段配置历史。
	c.Churn(2, 30*time.Millisecond, 1)
	c.WaitAllConfigs(c.Configs()[len(c.Configs())-1].Num)

	resp, err := http.Get(ts.URL + "/debug/configs")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/configs = %d, want 200", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var out struct {
		LatestNum int         `json:"latest_num"`
		Configs   []ConfigView `json:"configs"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("decode /debug/configs: %v\nbody=%s", err, string(b))
	}
	if out.LatestNum < 1 {
		t.Fatalf("latest_num = %d, want >= 1", out.LatestNum)
	}
	if len(out.Configs) != out.LatestNum+1 {
		t.Fatalf("configs count = %d, want %d", len(out.Configs), out.LatestNum+1)
	}
	// 历史应含初始空配置（num=0, 无 group）与若干次 rebalance（group 数递增）。
	seenGroups := map[int]bool{}
	for _, cfg := range out.Configs {
		for g := range cfg.Groups {
			seenGroups[g] = true
		}
	}
	if len(seenGroups) == 0 {
		t.Fatalf("/debug/configs 未包含任何 group 配置")
	}
}
