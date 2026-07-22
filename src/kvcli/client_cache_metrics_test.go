package main

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestClientCacheStats 验证：缓存命中/未命中/单飞合并/负向空值计数器正确。
func TestClientCacheStats(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("v"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.EnableCache(30*time.Second, 100)
	c.EnableSingleFlight()

	// 第一次 Get 回源（miss=1, hit=0）。
	if _, err := c.Get("k"); err != nil {
		t.Fatalf("Get err: %v", err)
	}
	st := c.CacheStats()
	if st.Misses != 1 || st.Hits != 0 {
		t.Fatalf("after 1st Get: misses=%d hits=%d, want 1/0", st.Misses, st.Hits)
	}

	// 第二次 Get 命中缓存（hit=1, miss 仍为 1）。
	if _, err := c.Get("k"); err != nil {
		t.Fatalf("Get err: %v", err)
	}
	st = c.CacheStats()
	if st.Hits != 1 {
		t.Fatalf("after 2nd Get: hits=%d, want 1", st.Hits)
	}
	if st.Misses != 1 {
		t.Fatalf("misses should stay 1, got %d", st.Misses)
	}

	// 并发同 key：单飞合并 -> coalesced>0（回源次数已在 TestClientSingleFlight 验证）。
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Get("same")
		}()
	}
	wg.Wait()
	st = c.CacheStats()
	if st.Coalesced == 0 {
		t.Fatalf("期望并发同 key 产生单飞合并（coalesced>0），实际 %d", st.Coalesced)
	}
}

// TestClientCacheStatsNegative 验证：缓存命中的空值计入 Negative 计数器（穿透缓解信号）。
func TestClientCacheStatsNegative(t *testing.T) {
	// 后端对缺失 key 返回空串（模拟 KV 未设置）。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(""))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.EnableCache(30*time.Second, 100)

	if _, err := c.Get("missing"); err != nil {
		t.Fatalf("Get err: %v", err)
	}
	if _, err := c.Get("missing"); err != nil { // 第二次命中缓存的空值
		t.Fatalf("Get err: %v", err)
	}
	st := c.CacheStats()
	if st.Negative != 1 {
		t.Fatalf("期望命中 1 次空值（negative=1），实际 %d", st.Negative)
	}
	if st.Hits != 1 {
		t.Fatalf("期望命中 1 次，实际 %d", st.Hits)
	}
}
