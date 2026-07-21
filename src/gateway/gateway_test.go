// gateway_test.go —— 用 httptest 覆盖网关的三种操作
package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"raftkv/src/cluster"
	"raftkv/src/shardmaster"
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

// TestGatewayMetricsPrometheus：验证 /metrics 按 Accept 协商输出 Prometheus 文本
// 格式（Accept 含 text/plain 或 prometheus）。该格式便于被 Prometheus / scrape 客户端
// 采集，是 cycle 14 新增的监控集成能力。
func TestGatewayMetricsPrometheus(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// 先产生一些指标：一次写入 + 一次读取。
	putReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/m", strings.NewReader("v"))
	if pr, err := http.DefaultClient.Do(putReq); err != nil {
		t.Fatal(err)
	} else {
		pr.Body.Close()
	}
	if _, err := http.Get(ts.URL + "/kv/m"); err != nil {
		t.Fatal(err)
	}

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/metrics", nil)
	req.Header.Set("Accept", "text/plain; version=0.0.4")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics (prometheus) = %d, want 200", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Fatalf("GET /metrics Content-Type = %q, want text/plain", ct)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	body := string(b)

	// Prometheus 输出须含 TYPE 声明与已知 counter（ops_total 由每次操作累计）。
	if !strings.Contains(body, "# TYPE ops_total counter") {
		t.Fatalf("GET /metrics (prometheus) missing '# TYPE ops_total counter':\n%s", body)
	}
	if !strings.Contains(body, "\nops_total ") {
		t.Fatalf("GET /metrics (prometheus) missing ops_total series:\n%s", body)
	}
	// 默认（JSON）格式仍可用：不带 Accept 时应返回 JSON。
	j, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	if j.Header.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("GET /metrics (default) Content-Type = %q, want application/json", j.Header.Get("Content-Type"))
	}
	jb, _ := io.ReadAll(j.Body)
	j.Body.Close()
	var parsed map[string]interface{}
	if err := json.Unmarshal(jb, &parsed); err != nil {
		t.Fatalf("GET /metrics (default) not valid JSON: %v\nbody=%s", err, string(jb))
	}
	if _, ok := parsed["counters"]; !ok {
		t.Fatalf("GET /metrics (default) missing counters key: %s", string(jb))
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

// TestGatewayConcurrencyLimit：并发洪泛远超信号量上限（64）的 /kv 请求（I12 的核心
// 担忧场景——慢速 Raft 读会在信号量上真实重叠），验证网关不会无限排队：超出的请求
// 立即得到 429，而整体保持稳定（无 panic、所有请求都能收到响应、洪泛后仍可用）。
func TestGatewayConcurrencyLimit(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)

	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// 内存集群后端经线性读快路径优化后极快，正常请求难以让在途数打满 64 槽，导致
	// 429 路径偶发不被触发。注入 10ms 人为延迟拉长在途窗口，使 90 并发洪泛能确定性
	// 打满信号量、稳定复现 429（仅单测生效，生产 testDelay 恒为 0）。
	s.SetTestDelay(10 * time.Millisecond)

	// 先写入一个值，使后续 GET 在未被限流时返回 200。
	putReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/flood", strings.NewReader("v"))
	if pr, err := http.DefaultClient.Do(putReq); err != nil {
		t.Fatal(err)
	} else {
		pr.Body.Close()
	}

	const n = 240
	// 客户端并发上限（90 个连接），既压垮测试监听器 accept 队列，又仍远高于
	// 网关 64 个信号量槽——因此 handler 会真实重叠打满信号量，触发 429；同时
	// 不至于把内存 Raft 集群压到不可恢复。
	const clientConc = 90
	var wg sync.WaitGroup
	var mu sync.Mutex
	got200, got429, gotOther := 0, 0, 0
	var firstErr error
	gate := make(chan struct{}, clientConc)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gate <- struct{}{}
			defer func() { <-gate }()
			resp, err := http.Get(ts.URL + "/kv/flood")
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			code := resp.StatusCode
			resp.Body.Close()
			mu.Lock()
			switch code {
			case http.StatusOK:
				got200++
			case http.StatusTooManyRequests:
				got429++
			default:
				gotOther++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if firstErr != nil {
		t.Fatalf("unexpected request error: %v", firstErr)
	}
	total := got200 + got429 + gotOther
	if total != n {
		t.Fatalf("processed %d requests, want %d (200=%d, 429=%d, other=%d)", total, n, got200, got429, gotOther)
	}
	if got200 == 0 {
		t.Fatalf("expected some 200 responses, got none (429=%d, other=%d)", got429, gotOther)
	}
	if got429 == 0 {
		t.Fatalf("expected some 429 responses under flood, got none (200=%d, other=%d)", got200, gotOther)
	}

	// 洪泛后网关仍可用：/kv 请求应在短暂收敛后再次稳定返回 200（容忍洪泛造成的
	// 内存集群瞬时不可达——网关已正确以 429/504 快速失败而非挂死，这里只验证恢复）。
	var recovered bool
	for attempt := 0; attempt < 20; attempt++ {
		resp, err := http.Get(ts.URL + "/kv/flood")
		if err == nil {
			code := resp.StatusCode
			resp.Body.Close()
			if code == http.StatusOK {
				recovered = true
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !recovered {
		t.Fatalf("gateway did not recover to 200 after flood (last seen non-200/err)")
	}
}

// TestGatewayGroups：GET /debug/groups 返回当前 group 成员与分片归属的 JSON，
// 结构正确、gid 齐全、且 [0..NShards) 正好被各 group 不重不漏地切分（I14）。
func TestGatewayGroups(t *testing.T) {
	const nGroups = 3
	c := cluster.StartCluster(nGroups, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(nGroups)

	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/debug/groups")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/groups = %d, want 200", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var out struct {
		Num    int `json:"num"`
		Groups []struct {
			GID     int      `json:"gid"`
			Servers []string `json:"servers"`
			Shards  []int    `json:"shards"`
		} `json:"groups"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("decode /debug/groups: %v\nbody=%s", err, string(b))
	}
	if len(out.Groups) != nGroups {
		t.Fatalf("/debug/groups groups = %d, want %d (body=%s)", len(out.Groups), nGroups, string(b))
	}

	// gids 应为 1..nGroups，且每个 group 都有 server 成员。
	seen := map[int]bool{}
	allShards := map[int]bool{}
	for _, g := range out.Groups {
		seen[g.GID] = true
		if len(g.Servers) == 0 {
			t.Fatalf("gid %d has no servers: %s", g.GID, string(b))
		}
		for _, sh := range g.Shards {
			if allShards[sh] {
				t.Fatalf("shard %d owned by more than one group: %s", sh, string(b))
			}
			allShards[sh] = true
		}
	}
	for i := 1; i <= nGroups; i++ {
		if !seen[i] {
			t.Fatalf("gid %d missing from /debug/groups: %s", i, string(b))
		}
	}

	// [0..NShards) 必须被恰好覆盖一次（不重不漏）。
	for i := 0; i < shardmaster.NShards; i++ {
		if !allShards[i] {
			t.Fatalf("shard %d not owned by any group: %s", i, string(b))
		}
	}
}
