// client_cache_test.go —— 验证 kvcli 读穿缓存（#73）：Get 命中未过期直接返回、
// 后端仅被调用一次；Put/Append 成功使缓存失效，下次 GET 回源。全程 cluster-free
// （httptest 平凡 handler + 原子计数后端命中次数）。
package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientCache(t *testing.T) {
	var mu sync.Mutex
	store := map[string]string{}
	var hits int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			store["k"] = string(b)
		case http.MethodGet:
			io.WriteString(w, store["k"])
		}
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	c.EnableCache(5*time.Second, 16)

	// 第一次 GET 回源（后端命中 1 次）。此时服务端尚无值，返回空串。
	v1, err := c.Get("k")
	if err != nil {
		t.Fatalf("Get#1 err: %v", err)
	}
	if v1 != "" {
		t.Fatalf("Get#1 = %q, want empty (server has no value yet)", v1)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("backend hits after Get#1 = %d, want 1", got)
	}

	// 第二次 GET 命中缓存，后端不应再被调用（命中仍为 1）。
	v2, err := c.Get("k")
	if err != nil {
		t.Fatalf("Get#2 err: %v", err)
	}
	if v2 != "" {
		t.Fatalf("Get#2 = %q, want empty", v2)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("backend hits after Get#2 (cached) = %d, want 1 (cache should serve)", got)
	}

	// Put 成功 -> 缓存失效。
	if err := c.Put("k", "w"); err != nil {
		t.Fatalf("Put err: %v", err)
	}
	if got := atomic.LoadInt32(&hits); got != 2 {
		t.Fatalf("backend hits after Put = %d, want 2", got)
	}

	// 失效后 GET 回源，应读到 Put 写入的 "w"（命中 = 3）。
	v3, err := c.Get("k")
	if err != nil {
		t.Fatalf("Get#3 err: %v", err)
	}
	if v3 != "w" {
		t.Fatalf("Get#3 = %q, want w", v3)
	}
	if got := atomic.LoadInt32(&hits); got != 3 {
		t.Fatalf("backend hits after Get#3 (re-fetch) = %d, want 3", got)
	}
}
