// gateway_metrics_test.go —— 验证网关 per-route 请求指标埋点（#80），全程 cluster-free：
// 直接构造 Server + wrap（no-op handler），经 httptest 发若干请求后断言 Metrics 计数；
// 并直接调 handleMetrics 验证 /metrics 合并暴露网关指标（Prometheus 文本 + JSON 子键）。
package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"raftkv/src/util"
)

func TestGatewayRequestMetrics(t *testing.T) {
	s := &Server{
		sem:            util.NewSemaphore(maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
	}
	Metrics.Reset()

	inner := func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }
	h := s.wrap(inner)
	ts := httptest.NewServer(http.HandlerFunc(h))
	defer ts.Close()

	// 发两次 GET 请求 -> 应记录 2 次请求总数、2 次 200 响应。
	http.Get(ts.URL + "/x")
	http.Get(ts.URL + "/x")
	// 再发一次 POST -> 验证 method 维度（cycle #118 新增能力）独立成序列。
	if _, err := http.Post(ts.URL+"/x", "text/plain", nil); err != nil {
		t.Fatalf("POST /x: %v", err)
	}
	if got := Metrics.CounterVec("http_requests_total", "method").WithLabelValues("POST").Value(); got < 1 {
		t.Fatalf("http_requests_total{method=POST} = %d, want >= 1 (method dimension missing)", got)
	}

	if got := Metrics.CounterVec("http_requests_total", "method").WithLabelValues("GET").Value(); got < 2 {
		t.Fatalf("http_requests_total{method=GET} = %d, want >= 2", got)
	}
	if got := Metrics.CounterVec("http_responses_total", "code", "method").WithLabelValues("200", "GET").Value(); got < 2 {
		t.Fatalf("http_responses_total{code=200,method=GET} = %d, want >= 2", got)
	}
	if got := Metrics.Histogram("http_request_latency_ms").Snapshot().Count; got < 2 {
		t.Fatalf("http_request_latency_ms count = %d, want >= 2", got)
	}

	// Prometheus 分支：应含网关指标序列名（带标签维度）。
	prec := httptest.NewRecorder()
	preq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	preq.Header.Set("Accept", "text/plain; version=0.0.4")
	s.handleMetrics(prec, preq)
	if prec.Code != http.StatusOK {
		t.Fatalf("handleMetrics(prom) status = %d, want 200", prec.Code)
	}
	pbody := prec.Body.String()
	if !strings.Contains(pbody, `http_requests_total{method="GET"}`) {
		t.Fatalf("prometheus output missing labeled http_requests_total:\n%s", pbody)
	}
	if !strings.Contains(pbody, `http_responses_total{code="200",method="GET"}`) {
		t.Fatalf("prometheus output missing labeled http_responses_total:\n%s", pbody)
	}

	// JSON 分支：顶层仍含 counters/histograms（KV 层），并新增 gateway 子键。
	jrec := httptest.NewRecorder()
	jreq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	s.handleMetrics(jrec, jreq)
	if jrec.Code != http.StatusOK {
		t.Fatalf("handleMetrics(json) status = %d, want 200", jrec.Code)
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal(jrec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("handleMetrics(json) not valid JSON: %v", err)
	}
	if _, ok := parsed["counters"]; !ok {
		t.Fatalf("JSON lost top-level KV counters (regression)")
	}
	if _, ok := parsed["histograms"]; !ok {
		t.Fatalf("JSON lost top-level KV histograms (regression)")
	}
	if _, ok := parsed["gateway"]; !ok {
		t.Fatalf("JSON missing gateway sub-key")
	}
}
