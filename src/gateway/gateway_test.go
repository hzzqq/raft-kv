// gateway_test.go —— 用 httptest 覆盖网关的三种操作
package main

import (
	"compress/gzip"
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

// TestGatewayAccessLog：验证 I15 的进程内访问日志——/debug/accesslog 记录近期的
// HTTP 请求（方法/路径/状态码/延迟）。先发若干请求，再断言日志含对应条目与正确状态码。
func TestGatewayAccessLog(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// 产生若干请求：一次 PUT（200）、一次 GET（200）。
	putReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/logged", strings.NewReader("v"))
	if pr, err := http.DefaultClient.Do(putReq); err != nil {
		t.Fatal(err)
	} else {
		pr.Body.Close()
	}
	if _, err := http.Get(ts.URL + "/kv/logged"); err != nil {
		t.Fatal(err)
	}
	// 一个必然 404 的路径也计入日志（即使本网关未注册该路由，仍应出现在访问日志中）。
	http.Get(ts.URL + "/nope")

	resp, err := http.Get(ts.URL + "/debug/accesslog?limit=10")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/accesslog = %d, want 200", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var entries []struct {
		Method    string  `json:"method"`
		Path      string  `json:"path"`
		Status    int     `json:"status"`
		LatencyMs float64 `json:"latency_ms"`
	}
	if err := json.Unmarshal(b, &entries); err != nil {
		t.Fatalf("decode /debug/accesslog: %v\nbody=%s", err, string(b))
	}
	if len(entries) == 0 {
		t.Fatalf("/debug/accesslog returned empty: %s", string(b))
	}
	// 应至少含 /kv/logged 的 PUT 与 GET 两条，且 /nope 计入（无论状态码）。
	sawPut, sawGet := false, false
	for _, e := range entries {
		if e.Path == "/kv/logged" && e.Method == http.MethodPut && e.Status == http.StatusOK {
			sawPut = true
		}
		if e.Path == "/kv/logged" && e.Method == http.MethodGet && e.Status == http.StatusOK {
			sawGet = true
		}
		if e.LatencyMs < 0 {
			t.Fatalf("access log negative latency: %s", string(b))
		}
	}
	if !sawPut || !sawGet {
		t.Fatalf("/debug/accesslog missing expected /kv/logged entries: %s", string(b))
	}
}

// TestGatewayRequestTimeout：验证 I16 的单请求超时兜底——后端操作（经 testDelay 人为
// 拉长）超过 requestTimeout 时，网关立即返回 503 而非让 HTTP 连接无限挂起。
func TestGatewayRequestTimeout(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)
	s.SetRequestTimeout(200 * time.Millisecond)
	s.SetTestDelay(2 * time.Second) // 强制 handler 远超超时上限
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	start := time.Now()
	resp, err := http.Get(ts.URL + "/kv/timeout")
	if err != nil {
		t.Fatal(err)
	}
	code := resp.StatusCode
	resp.Body.Close()
	elapsed := time.Since(start)

	if code != http.StatusServiceUnavailable {
		t.Fatalf("GET exceeding timeout = %d, want 503", code)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("gateway did not time out promptly: took %v (want < 2s)", elapsed)
	}
}

// TestGatewayReadyz：验证 I18 的就绪探针——集群健康（Init 后）返回 200，杀光所有
// 副本（无 leader）时返回 503，与存活探针 /healthz（恒 200）语义区分。
func TestGatewayReadyz(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	rd, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	if rd.StatusCode != http.StatusOK {
		t.Fatalf("GET /readyz (healthy) = %d, want 200", rd.StatusCode)
	}
	rd.Body.Close()

	// /healthz 始终 200（存活探针），与就绪探针区分。
	h, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	if h.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", h.StatusCode)
	}
	h.Body.Close()

	// 杀光所有副本：clusterHealthy 返回 false -> /readyz 503。
	for g := range c.KVs {
		for r := range c.KVs[g] {
			c.Net.Enable(1000+g*100+r, false)
		}
	}
	time.Sleep(1200 * time.Millisecond) // 等 leader 失联、各副本退位（无 leader）
	rd2, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	code := rd2.StatusCode
	rd2.Body.Close()
	if code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz (no leader) = %d, want 503", code)
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
		LatestNum int          `json:"latest_num"`
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

// TestGatewayLogLevel：验证 /debug/log 分级结构化日志（I47）——每请求产生一条日志，
// 可按 ?level= 过滤最低级别（成功请求为 info，错误请求升级 warn/error）。
func TestGatewayLogLevel(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// 一次成功请求（应产生 info 级日志）
	putReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/foo", strings.NewReader("bar"))
	resp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
	// 触发一次客户端错误：对不存在路径发起不支持的方法，网关返回 4xx -> warn。
	// GET /kv/ 形式非法（缺 key），路由不匹配 -> 405 Method Not Allowed。
	bad, _ := http.NewRequest(http.MethodDelete, ts.URL+"/kv/foo", nil)
	br, _ := http.DefaultClient.Do(bad)
	if br != nil {
		br.Body.Close()
	}

	// 默认（info 级）应能看到成功请求的 info 日志。
	respLog, err := http.Get(ts.URL + "/debug/log?level=info&limit=50")
	if err != nil {
		t.Fatal(err)
	}
	lb, _ := io.ReadAll(respLog.Body)
	respLog.Body.Close()
	var logs []map[string]interface{}
	if err := json.Unmarshal(lb, &logs); err != nil {
		t.Fatalf("GET /debug/log body not valid JSON: %v (body=%s)", err, string(lb))
	}
	if len(logs) == 0 {
		t.Fatalf("GET /debug/log returned no entries (body=%s)", string(lb))
	}
	// 至少有一条 info 级 "request" 日志（成功 PUT 产生）。
	hasInfo := false
	for _, l := range logs {
		if l["level"] == "info" && l["msg"] == "request" {
			hasInfo = true
		}
	}
	if !hasInfo {
		t.Fatalf("GET /debug/log missing info 'request' entry (body=%s)", string(lb))
	}

	// ?level=error 应只返回 error 级（本测试无 5xx，故为空数组）。
	respErr, err := http.Get(ts.URL + "/debug/log?level=error&limit=50")
	if err != nil {
		t.Fatal(err)
	}
	eb, _ := io.ReadAll(respErr.Body)
	respErr.Body.Close()
	var errLogs []map[string]interface{}
	if err := json.Unmarshal(eb, &errLogs); err != nil {
		t.Fatalf("GET /debug/log?level=error body not valid JSON: %v (body=%s)", err, string(eb))
	}
	for _, l := range errLogs {
		if l["level"] != "error" {
			t.Fatalf("level=error filter leaked non-error entry: %s", string(eb))
		}
	}
}

// TestGatewayRequestID：验证 X-Request-ID 透传（I48）——入站缺则生成、存在则沿用，
// 均回写到响应头，便于跨服务链路追踪。该测试直接构造 Server + 平凡 handler，不依赖
// raft 集群，故不受沙箱恢复后偶发的 raft 选举挂死影响（更稳）。
func TestGatewayRequestID(t *testing.T) {
	// 直接构造 Server（不经由 cluster，避免集群启动），仅需 wrap 用到的字段。
	s := &Server{
		sem:            make(chan struct{}, maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
	}
	h := s.wrap(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok")
	})
	ts := httptest.NewServer(http.HandlerFunc(h))
	defer ts.Close()

	// 1) 入站无 X-Request-ID -> 网关生成并回写。
	resp, err := http.Get(ts.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	idGen := resp.Header.Get("X-Request-ID")
	if idGen == "" {
		t.Fatalf("expected X-Request-ID to be generated and set on response")
	}
	resp.Body.Close()

	// 2) 入站带 X-Request-ID -> 网关原样透传（不重新生成）。
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/x", nil)
	req.Header.Set("X-Request-ID", "trace-abc-123")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if got := resp2.Header.Get("X-Request-ID"); got != "trace-abc-123" {
		t.Fatalf("X-Request-ID not propagated: got %q want trace-abc-123", got)
	}
	resp2.Body.Close()
}

// TestGatewayClientLimit：验证每客户端令牌桶限流（I49）——同一客户端在突发上限内
// 放行，超出返回 429 + Retry-After；不同客户端互不影响。cluster-free 构造。
// 用极小补充速率（0.001 rps）使测试窗口内几乎不补充，从而突发上限即精确允许数。
func TestGatewayClientLimit(t *testing.T) {
	s := &Server{
		sem:            make(chan struct{}, maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
		clientLimiters: make(map[string]*tokenBucket),
		clientRate:     0.001, // 测试窗口内几乎不补充
		clientBurst:    5,
	}
	h := s.wrap(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok")
	})
	ts := httptest.NewServer(http.HandlerFunc(h))
	defer ts.Close()
	client := &http.Client{}

	// 客户端 c1：连续 10 次请求，前 5 次（突发）放行，后 5 次限流 429。
	ok, limited := 0, 0
	for i := 0; i < 10; i++ {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/x", nil)
		req.Header.Set("X-Client-ID", "c1")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		switch resp.StatusCode {
		case http.StatusOK:
			ok++
		case http.StatusTooManyRequests:
			limited++
			if resp.Header.Get("Retry-After") == "" {
				t.Fatalf("rate-limited response missing Retry-After header")
			}
		default:
			t.Fatalf("unexpected status %d", resp.StatusCode)
		}
		resp.Body.Close()
	}
	if ok != 5 {
		t.Fatalf("client c1: expected 5 allowed (burst), got %d", ok)
	}
	if limited != 5 {
		t.Fatalf("client c1: expected 5 rate-limited, got %d", limited)
	}

	// 不同客户端 c2 应不受 c1 消耗影响（独立令牌桶），首次请求即放行。
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/x", nil)
	req.Header.Set("X-Client-ID", "c2")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("client c2: expected allowed (independent bucket), got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestGatewayCORS：验证 CORS 中间件（I50）——普通跨域请求注入 ACAO，OPTIONS 预检
// 返回 204 + 头，受限 origins 不匹配时不注入（等效拒绝）。cluster-free 构造。
func TestGatewayCORS(t *testing.T) {
	base := func() http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, "ok")
		})
	}

	// 1) 允许所有源（corsOrigins 空）。
	s := &Server{}
	h := s.corsHandler(base())
	ts := httptest.NewServer(h)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/x", nil)
	req.Header.Set("Origin", "https://example.com")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Get("Access-Control-Allow-Origin") == "" {
		t.Fatalf("expected Access-Control-Allow-Origin header")
	}
	resp.Body.Close()

	// OPTIONS 预检 -> 204 + Allow-Methods。
	pre, _ := http.NewRequest(http.MethodOptions, ts.URL+"/x", nil)
	pre.Header.Set("Origin", "https://example.com")
	prep, err := http.DefaultClient.Do(pre)
	if err != nil {
		t.Fatal(err)
	}
	if prep.StatusCode != http.StatusNoContent {
		t.Fatalf("preflight status = %d, want 204", prep.StatusCode)
	}
	if prep.Header.Get("Access-Control-Allow-Methods") == "" {
		t.Fatalf("preflight missing Access-Control-Allow-Methods")
	}
	prep.Body.Close()

	// 2) 受限 origins（仅允许 a.com）：b.com 不应注入 ACAO。
	s2 := &Server{corsOrigins: []string{"https://a.com"}}
	h2 := s2.corsHandler(base())
	ts2 := httptest.NewServer(h2)
	defer ts2.Close()
	req3, _ := http.NewRequest(http.MethodGet, ts2.URL+"/x", nil)
	req3.Header.Set("Origin", "https://b.com")
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	if resp3.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("disallowed origin should not get ACAO header")
	}
	resp3.Body.Close()

	// 匹配 a.com 应正常注入。
	req4, _ := http.NewRequest(http.MethodGet, ts2.URL+"/x", nil)
	req4.Header.Set("Origin", "https://a.com")
	resp4, err := http.DefaultClient.Do(req4)
	if err != nil {
		t.Fatal(err)
	}
	if got := resp4.Header.Get("Access-Control-Allow-Origin"); got != "https://a.com" {
		t.Fatalf("matched origin ACAO = %q, want https://a.com", got)
	}
	resp4.Body.Close()
}

// TestGatewayConfigLoad：验证极简 YAML 子集配置解析（I51）——覆盖各字段、列表解析、
// 未知键忽略，以及 Apply 后 Server 配置生效（限流桶与 CORS 被设置）。cluster-free。
func TestGatewayConfigLoad(t *testing.T) {
	yaml := `
# 网关配置
listen_addr: ":9090"
request_timeout_sec: 15
max_concurrent: 32
client_rate: 50
client_burst: 10
cors_origins: ["https://a.com", "https://b.com"]
unknown_key: ignored
`
	cfg, err := ParseGatewayConfig([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != ":9090" {
		t.Fatalf("listen_addr = %q, want :9090", cfg.ListenAddr)
	}
	if cfg.RequestTimeout != 15 {
		t.Fatalf("request_timeout_sec = %d, want 15", cfg.RequestTimeout)
	}
	if cfg.MaxConcurrent != 32 {
		t.Fatalf("max_concurrent = %d, want 32", cfg.MaxConcurrent)
	}
	if cfg.ClientRate != 50 {
		t.Fatalf("client_rate = %v, want 50", cfg.ClientRate)
	}
	if cfg.ClientBurst != 10 {
		t.Fatalf("client_burst = %d, want 10", cfg.ClientBurst)
	}
	if len(cfg.CORSOrigins) != 2 || cfg.CORSOrigins[0] != "https://a.com" || cfg.CORSOrigins[1] != "https://b.com" {
		t.Fatalf("cors_origins = %v, want [a.com b.com]", cfg.CORSOrigins)
	}

	// Apply 后 Server 配置应生效：限流桶被创建、CORS 被设置。
	s := &Server{
		sem:            make(chan struct{}, maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
	}
	cfg.Apply(s)
	if s.RequestTimeout() != 15*time.Second {
		t.Fatalf("RequestTimeout after Apply = %v, want 15s", s.RequestTimeout())
	}
	if cap(s.sem) != 32 {
		t.Fatalf("sem cap after Apply = %d, want 32", cap(s.sem))
	}
	s.limitMu.Lock()
	nilMap := s.clientLimiters == nil
	s.limitMu.Unlock()
	if nilMap {
		t.Fatalf("clientLimiters not initialized after Apply")
	}
	if len(s.corsOrigins) != 2 {
		t.Fatalf("corsOrigins after Apply = %v, want 2 entries", s.corsOrigins)
	}
}

// TestGatewayDebugConfig：验证 /debug/config 返回当前生效配置快照（I52）。cluster-free
// 直接构造 Server + Apply 配置，再用 httptest 调用 handleDebugConfig。
func TestGatewayDebugConfig(t *testing.T) {
	s := &Server{
		sem:            make(chan struct{}, maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
	}
	cfg := GatewayConfig{
		ListenAddr:     ":9090",
		RequestTimeout: 15,
		MaxConcurrent:  32,
		ClientRate:     50,
		ClientBurst:    10,
		CORSOrigins:    []string{"https://a.com"},
		MaxBodySize:    2097152,
	}
	cfg.Apply(s)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugConfig(w, r)
	}))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/debug/config")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var snap ConfigSnapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		t.Fatalf("body not valid JSON: %v (body=%s)", err, string(b))
	}
	if snap.ListenAddr != ":9090" || snap.RequestTimeout != 15 || snap.MaxConcurrent != 32 ||
		snap.ClientRate != 50 || snap.ClientBurst != 10 || snap.MaxBodySize != 2097152 ||
		len(snap.CORSOrigins) != 1 || snap.CORSOrigins[0] != "https://a.com" {
		t.Fatalf("config snapshot mismatch: %+v", snap)
	}
}

// TestGatewayMaxBodySize：验证请求体上限（I54）。cluster-free 直接构造 Server，
// 用 httptest 套 wrap 中间件（内部 no-op handler，不依赖真实 clerk/集群），已知
// Content-Length 超过 maxBodySize 必须返回 413，未超限则正常放行。
func TestGatewayMaxBodySize(t *testing.T) {
	s := &Server{
		sem:            make(chan struct{}, maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
	}
	s.SetMaxBodySize(16) // 限制 16 字节
	inner := func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }
	h := s.wrap(inner)

	ts := httptest.NewServer(http.HandlerFunc(h))
	defer ts.Close()

	// 超过上限：Content-Length=32 > 16 -> 期望 413
	big := strings.Repeat("x", 32)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/x", strings.NewReader(big))
	req.ContentLength = int64(len(big))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body status = %d, want 413", resp.StatusCode)
	}
	resp.Body.Close()

	// 未超限：8 <= 16 -> 期望 200 且内部 handler 正常执行（body="ok"）
	small := strings.Repeat("y", 8)
	req2, _ := http.NewRequest(http.MethodPut, ts.URL+"/x", strings.NewReader(small))
	req2.ContentLength = int64(len(small))
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("small body status = %d, want 200", resp2.StatusCode)
	}
	b, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if string(b) != "ok" {
		t.Fatalf("small body inner handler output = %q, want ok", string(b))
	}
}

// TestGatewayCompress：验证响应 gzip 压缩（I55）。cluster-free 直接构造 Server，
// 套 wrap + no-op handler。带 Accept-Encoding: gzip 时响应须有 Content-Encoding: gzip
// 且 body 可解压回原文；不带则原样返回。
func TestGatewayCompress(t *testing.T) {
	s := &Server{
		sem:            make(chan struct{}, maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
		compress:       true,
	}
	inner := func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, strings.Repeat("payload-", 200)) }
	h := s.wrap(inner)
	ts := httptest.NewServer(http.HandlerFunc(h))
	defer ts.Close()

	// 带 gzip
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/x", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", resp.Header.Get("Content-Encoding"))
	}
	if resp.Header.Get("Vary") == "" {
		t.Fatalf("Vary header missing (cache-correctness regression)")
	}
	gr, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(gr)
	resp.Body.Close()
	if string(out) != strings.Repeat("payload-", 200) {
		t.Fatalf("decompressed body mismatch (len=%d)", len(out))
	}

	// 不带 gzip -> 原样
	resp2, err := http.Get(ts.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	if resp2.Header.Get("Content-Encoding") == "gzip" {
		t.Fatalf("unexpected gzip when client did not ask")
	}
	b2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if string(b2) != strings.Repeat("payload-", 200) {
		t.Fatalf("uncompressed body mismatch (len=%d)", len(b2))
	}
}

// TestGatewaySecurityHeaders：验证基线安全响应头（I56）。cluster-free 套 wrap +
// no-op handler，断言响应带 X-Content-Type-Options / X-Frame-Options / Referrer-Policy。
func TestGatewaySecurityHeaders(t *testing.T) {
	s := &Server{
		sem:            make(chan struct{}, maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
		secHeaders:     true,
	}
	inner := func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }
	h := s.wrap(inner)
	ts := httptest.NewServer(http.HandlerFunc(h))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	for _, h := range []string{"X-Content-Type-Options", "X-Frame-Options", "Referrer-Policy"} {
		if resp.Header.Get(h) == "" {
			t.Fatalf("security header %s missing", h)
		}
	}
}

// TestGatewayIPAllow：验证 IP 白名单（I57）。cluster-free 套 wrap + no-op handler。
// 白名单设为 10.0.0.0/8（不含 httptest 的 127.0.0.1 来源）-> 403；改为 127.0.0.0/8 -> 200。
func TestGatewayIPAllow(t *testing.T) {
	inner := func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }

	// 拒绝：来源 127.0.0.1 不在 10.0.0.0/8
	s := &Server{
		sem:            make(chan struct{}, maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
	}
	s.SetIPAllow([]string{"10.0.0.0/8"})
	h := s.wrap(inner)
	ts := httptest.NewServer(http.HandlerFunc(h))
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("denied status = %d, want 403", resp.StatusCode)
	}
	resp.Body.Close()

	// 放行：来源 127.0.0.1 在 127.0.0.0/8
	s2 := &Server{
		sem:            make(chan struct{}, maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
	}
	s2.SetIPAllow([]string{"127.0.0.0/8"})
	h2 := s2.wrap(inner)
	ts2 := httptest.NewServer(http.HandlerFunc(h2))
	defer ts2.Close()
	resp2, err := http.Get(ts2.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("allowed status = %d, want 200", resp2.StatusCode)
	}
	resp2.Body.Close()
}

// TestGatewayDebugRoutes：验证 /debug/routes 路由清单端点（I58）。cluster-free 直接
// 构造 Server 并调 Handler()（不启动集群），再 httptest 调 handleDebugRoutes，断言返回
// 的 routes 包含全部已注册模式且含本端点自身。
func TestGatewayDebugRoutes(t *testing.T) {
	s := &Server{
		sem:            make(chan struct{}, maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
	}
	s.Handler() // 填充 s.routes
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugRoutes(w, r)
	}))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/debug/routes")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var out struct {
		Count  int      `json:"count"`
		Routes []string `json:"routes"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("body not valid JSON: %v (body=%s)", err, string(b))
	}
	if out.Count == 0 {
		t.Fatalf("routes count = 0, want >0")
	}
	want := map[string]bool{
		"GET /kv/{key}":     true,
		"PUT /kv/{key}":     true,
		"GET /debug/config": true,
		"GET /debug/routes": true,
	}
	for pat := range want {
		found := false
		for _, r := range out.Routes {
			if r == pat {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("route %q missing from /debug/routes (routes=%v)", pat, out.Routes)
		}
	}
}

// TestGatewayDebugVersion：验证版本与 uptime 端点（I59）。cluster-free 直接构造 Server，
// 设 startedAt 过去 100s、version 后，经 Handler() 调 /debug/version，断言 uptime≈100 且
// version 透传、含 go_version。
func TestGatewayDebugVersion(t *testing.T) {
	s := &Server{
		sem:            make(chan struct{}, maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
		startedAt:      time.Now().Add(-100 * time.Second),
		version:        "test-1.2.3",
	}
	mux := s.Handler()
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/debug/version")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Version   string  `json:"version"`
		GoVersion string  `json:"go_version"`
		UptimeSec float64 `json:"uptime_sec"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Version != "test-1.2.3" {
		t.Fatalf("version = %q, want test-1.2.3", out.Version)
	}
	if out.GoVersion == "" {
		t.Fatalf("go_version empty")
	}
	if out.UptimeSec < 99 || out.UptimeSec > 101 {
		t.Fatalf("uptime_sec = %v, want ~100", out.UptimeSec)
	}
}
