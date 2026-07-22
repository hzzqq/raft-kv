package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestGatewayLimiterAllowsNormalTraffic 是限流栈的 e2e 守卫：在宽松的 per-client 与
// route 限流下，正常流量必须畅通返回 200，证明限流中间件「只拦超额、不误伤正常请求」，
// 且各限流层可在同一 Wrap 上共存而不相互破坏（cluster-free：用 stub handler）。
func TestGatewayLimiterAllowsNormalTraffic(t *testing.T) {
	resetCounter("gateway_ratelimit_client_total")
	resetCounter("gateway_ratelimit_route_total")
	resetCounter("gateway_ratelimit_concurrent_total")

	cfg, _ := ParseGatewayConfig([]byte("route_limits: [GET /kv/*=50/50]"))
	s := NewServer(nil)
	cfg.Apply(s)
	s.SetClientRateLimit(100, 100) // 宽松 per-client 限流
	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })

	for i := 0; i < 10; i++ {
		req := httptest.NewRequest("GET", "/kv/x", nil)
		req.Header.Set("X-Client-ID", "normal")
		rec := httptest.NewRecorder()
		h(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("normal request %d got status %d, want 200", i, rec.Code)
		}
	}
	// 宽松限流下不应产生任何 429 计数。
	if got := Metrics.Counter("gateway_ratelimit_client_total").Value(); got != 0 {
		t.Fatalf("client 429 counter = %d under generous limit, want 0", got)
	}
	if got := Metrics.Counter("gateway_ratelimit_route_total").Value(); got != 0 {
		t.Fatalf("route 429 counter = %d under generous limit, want 0", got)
	}
}

// TestGatewayLimiterStackOrdering 验证限流栈的拒绝优先级：熔断打开态优先于
// 并发/限流（最先拒），随后是 per-client、route、最后并发信号量。此处构造熔断打开态，
// 即便并发与限流都宽裕，也应得到 503 而非 429（证明优先级正确，cluster-free）。
func TestGatewayLimiterStackOrdering(t *testing.T) {
	resetCounter("gateway_breaker_rejects_total")
	resetCounter("gateway_ratelimit_client_total")
	resetCounter("gateway_ratelimit_concurrent_total")

	s := NewServer(nil)
	s.SetClientRateLimit(1000, 1000) // 宽裕
	s.SetBreaker(100, 10*time.Second)
	// 先触发熔断打开
	breakerH := s.Wrap(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) })
	for i := 0; i < 100; i++ {
		breakerH(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
	}
	if Metrics.Gauge("gateway_breaker_open").Value() != 1 {
		t.Fatal("breaker should be open before ordering test")
	}
	// 熔断打开态下，正常 handler 也应被快速失败（503），且不计入 client/concurrent 429
	normalH := s.Wrap(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	rec := httptest.NewRecorder()
	normalH(rec, httptest.NewRequest("GET", "/kv/x", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 (breaker open), got %d", rec.Code)
	}
	if Metrics.Counter("gateway_ratelimit_client_total").Value() != 0 {
		t.Fatal("breaker-open rejection must not be counted as client 429")
	}
	if Metrics.Counter("gateway_ratelimit_concurrent_total").Value() != 0 {
		t.Fatal("breaker-open rejection must not be counted as concurrent 429")
	}
}
