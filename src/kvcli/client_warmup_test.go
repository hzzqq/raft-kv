package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestClientWarmUp 验证：缓存启用后预热成功，随后读取命中缓存（hits 增长）。
func TestClientWarmUp(t *testing.T) {
	var backend atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend.Add(1)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("v"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.EnableCache(60*time.Second, 100)
	if err := c.WarmUp([]string{"a", "b", "c"}); err != nil {
		t.Fatalf("WarmUp 应成功，实际 %v", err)
	}
	if backend.Load() != 3 {
		t.Fatalf("期望后端回源 3 次，实际 %d", backend.Load())
	}
	// 预热后读取应命中缓存，不再回源。
	before := c.CacheStats().Hits
	for _, k := range []string{"a", "b", "c"} {
		if v, err := c.Get(k); err != nil || v != "v" {
			t.Fatalf("读取 %s 应命中缓存，实际 v=%q err=%v", k, v, err)
		}
	}
	if c.CacheStats().Hits-before != 3 {
		t.Fatalf("期望命中 3 次缓存，实际增量 %d", c.CacheStats().Hits-before)
	}
	if backend.Load() != 3 {
		t.Fatalf("预热后读取不应再回源，实际后端调用 %d", backend.Load())
	}
}

// TestClientWarmUpPartialFailure 验证：部分 key 回源失败，WarmUp 返回聚合错误。
func TestClientWarmUpPartialFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kv/bad" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	c.EnableCache(60*time.Second, 100)
	err := c.WarmUp([]string{"ok", "bad"})
	if err == nil {
		t.Fatalf("部分失败应返回错误")
	}
}

// TestClientWarmUpNoCache 验证：未启用缓存时 WarmUp 返回错误。
func TestClientWarmUpNoCache(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	if err := c.WarmUp([]string{"a"}); err == nil {
		t.Fatalf("未启用缓存应返回错误")
	}
}
