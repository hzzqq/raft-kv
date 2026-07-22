package main

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// resetCounter 把某个计数器的当前值清零（测试间隔离用，避免累加到断言上）。
func resetCounter(name string) {
	c := Metrics.Counter(name)
	c.Sub(c.Value())
}

func TestRatelimitClientMetric(t *testing.T) {
	resetCounter("gateway_ratelimit_client_total")
	s := NewServer(nil)
	s.SetClientRateLimit(1, 1) // 1 rps, burst 1 → 第 2 个同客户端请求即 429
	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest("GET", "/kv/x", nil)
		req.Header.Set("X-Client-ID", "c1")
		h(recorder(), req)
	}
	if got := Metrics.Counter("gateway_ratelimit_client_total").Value(); got <= 0 {
		t.Fatalf("client ratelimit counter = %d, want > 0", got)
	}
}

func TestRatelimitConcurrentMetric(t *testing.T) {
	resetCounter("gateway_ratelimit_concurrent_total")
	s := NewServer(nil)
	s.SetClientRateLimit(0, 0)           // 关闭 per-client 限流，单独验证并发信号量
	s.SetTestDelay(200 * time.Millisecond) // 拉长在途窗口，使并发信号量可被打满
	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })

	n := maxConcurrent + 5
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // 同时放行，确保并发信号量被瞬间打满
			req := httptest.NewRequest("GET", "/kv/x", nil)
			h(recorder(), req)
		}()
	}
	close(start)
	wg.Wait()
	// 至少应有 (n - maxConcurrent) 个请求因并发超限被 429。
	if got := Metrics.Counter("gateway_ratelimit_concurrent_total").Value(); got < int64(n-maxConcurrent) {
		t.Fatalf("concurrent ratelimit counter = %d, want >= %d", got, n-maxConcurrent)
	}
}

func TestBreakerMetricOnTrip(t *testing.T) {
	resetCounter("gateway_breaker_trips_total")
	resetCounter("gateway_breaker_rejects_total")
	Metrics.Gauge("gateway_breaker_open").Set(0)
	s := NewServer(nil)
	s.SetBreaker(3, 10*time.Second) // 连续 3 个 5xx 即熔断
	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest("GET", "/kv/x", nil)
		h(recorder(), req)
	}
	if got := Metrics.Counter("gateway_breaker_trips_total").Value(); got <= 0 {
		t.Fatalf("breaker trips counter = %d, want > 0", got)
	}
	if got := Metrics.Gauge("gateway_breaker_open").Value(); got != 1 {
		t.Fatalf("breaker open gauge = %v, want 1", got)
	}
	// 熔断打开后，后续请求被快速失败，且计入 breaker rejects。
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/kv/x", nil)
		h(recorder(), req)
	}
	if got := Metrics.Counter("gateway_breaker_rejects_total").Value(); got <= 0 {
		t.Fatalf("breaker rejects counter = %d, want > 0", got)
	}
}

// recorder 返回新的 httptest.ResponseRecorder（测试只关心状态码/指标，不读 body）。
func recorder() *httptest.ResponseRecorder { return httptest.NewRecorder() }
