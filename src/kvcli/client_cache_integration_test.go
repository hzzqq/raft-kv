package main

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestClientCacheDeepIntegration 综合验证 #83-#86：并发同 key 的 Get 在
// 单飞+缓存加持下，后端仅回源 1 次，其余全部命中，且指标自洽。
func TestClientCacheDeepIntegration(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		time.Sleep(5 * time.Millisecond) // 拉长回源，确保并发重叠
		_, _ = w.Write([]byte("v"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.EnableCache(30*time.Second, 1000)
	c.EnableSingleFlight()

	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Get("hot")
		}()
	}
	wg.Wait()

	st := c.CacheStats()
	// 击穿防护的真指标：单飞把 100 并发同 key 合并为仅 1 次真实回源。
	if atomic.LoadInt64(&hits) != 1 {
		t.Fatalf("期望单飞+缓存使回源仅 1 次，实际 %d", atomic.LoadInt64(&hits))
	}
	// 每 goroutine 在进入单飞前各记一次未命中；其中 1 个真正回源、其余 n-1 个被合并。
	if st.Misses != int64(n) {
		t.Fatalf("期望未命中=%d（每 goroutine 各记一次），实际 %d", n, st.Misses)
	}
	if st.Coalesced != int64(n-1) {
		t.Fatalf("期望单飞合并数 coalesced=%d，实际 %d", n-1, st.Coalesced)
	}
	// 全部发生在并发窗口内：回源者写缓存、其余复用单飞结果，窗口内无缓存命中。
	if st.Hits != 0 {
		t.Fatalf("期望窗口内缓存命中=0（由单飞合并覆盖），实际 %d", st.Hits)
	}
	if st.Hits+st.Misses != int64(n) {
		t.Fatalf("期望命中+未命中=%d，实际 %d", n, st.Hits+st.Misses)
	}
}
