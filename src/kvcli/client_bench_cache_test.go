package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestClientBenchCacheStats 验证：Bench 窗口内缓存指标覆盖全部 GET 操作，且命中率合法。
func TestClientBenchCacheStats(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("v"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.EnableCache(30*time.Second, 10000)
	c.EnableSingleFlight()

	res := c.Bench(200, 8, "get", 8)
	if res.Errors != 0 {
		t.Fatalf("bench errors=%d", res.Errors)
	}
	got := res.CacheHits + res.CacheMisses
	want := int64(res.Ops) // op=get -> 每次 op 一次 GET
	if got != want {
		t.Fatalf("缓存指标应覆盖全部 GET 操作: hits+misses=%d, want %d", got, want)
	}
	if res.CacheHitRate < 0 || res.CacheHitRate > 1 {
		t.Fatalf("命中率越界: %f", res.CacheHitRate)
	}
}
