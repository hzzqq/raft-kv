package main

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestClientSingleFlight 验证：并发同 key 的 Get（缓存+单飞开启）只回源一次。
func TestClientSingleFlight(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		time.Sleep(10 * time.Millisecond) // 拉长回源，确保并发重叠
		_, _ = w.Write([]byte("v"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.EnableCache(30*time.Second, 100)
	c.EnableSingleFlight()

	var wg sync.WaitGroup
	const n = 50
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = c.Get("same-key")
		}(i)
	}
	wg.Wait()

	if atomic.LoadInt64(&hits) != 1 {
		t.Fatalf("期望后端仅被回源 1 次（击穿保护），实际 %d", atomic.LoadInt64(&hits))
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("Get[%d] err=%v", i, e)
		}
	}
	// 单飞+缓存已生效：再 Get 一次应命中缓存，后端计数不变。
	if _, e := c.Get("same-key"); e != nil {
		t.Fatalf("二次 Get err=%v", e)
	}
	if atomic.LoadInt64(&hits) != 1 {
		t.Fatalf("二次 Get 应命中缓存，后端计数应为 1，实际 %d", atomic.LoadInt64(&hits))
	}
}

// TestClientSingleFlightDisabled 验证：未开启单飞时，并发同 key 各自回源（行为透明、无保护）。
func TestClientSingleFlightDisabled(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		_, _ = w.Write([]byte("v"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.EnableCache(30*time.Second, 100) // 不开启单飞

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Get("k")
		}()
	}
	wg.Wait()
	// 未开启单飞：并发各自回源（缓存尚未预热时），hits 应 >1。
	if atomic.LoadInt64(&hits) <= 1 {
		t.Fatalf("未开启单飞时，并发回源应 >1，实际 %d", atomic.LoadInt64(&hits))
	}
}
