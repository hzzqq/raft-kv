package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestMGetBoundedConcurrency 验证 SetMaxConcurrent(n) 真实限制 MGet 在途请求数：
// 用带 sleep 的 mock 网关统计并发在途数，断言其不超过设定上限（R2 隐性健壮性——
// 默认无上限，超大批量会一次性拉起大量 goroutine）。仅运行本测试，不触发 cluster 测试。
func TestMGetBoundedConcurrency(t *testing.T) {
	var inflight int64
	var maxInflight int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&inflight, 1)
		for {
			m := atomic.LoadInt64(&maxInflight)
			if n <= m || atomic.CompareAndSwapInt64(&maxInflight, m, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt64(&inflight, -1)
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, r.URL.Path)
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	c.SetMaxConcurrent(2)

	keys := make([]string, 20)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	res := c.MGet(keys)
	if len(res.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", res.Errors)
	}
	if len(res.Results) != len(keys) {
		t.Fatalf("got %d results, want %d", len(res.Results), len(keys))
	}
	if got := atomic.LoadInt64(&maxInflight); got > 2 {
		t.Fatalf("max in-flight = %d, want <= 2 (SetMaxConcurrent(2) violated)", got)
	}
}

// TestMGetUnboundedByDefault 验证默认（未 SetMaxConcurrent）不限制并发：20 个 key
// 应允许超过 2 个同时在途（确认限流是显式 opt-in，向后兼容历史语义）。
func TestMGetUnboundedByDefault(t *testing.T) {
	var inflight int64
	var maxInflight int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&inflight, 1)
		for {
			m := atomic.LoadInt64(&maxInflight)
			if n <= m || atomic.CompareAndSwapInt64(&maxInflight, m, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt64(&inflight, -1)
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, r.URL.Path)
	}))
	defer ts.Close()

	c := NewClient(ts.URL) // 不设 maxConcurrent
	keys := make([]string, 20)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	res := c.MGet(keys)
	if len(res.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", res.Errors)
	}
	if atomic.LoadInt64(&maxInflight) <= 2 {
		t.Fatalf("default should allow >2 in-flight, got %d (limit unexpectedly active)", atomic.LoadInt64(&maxInflight))
	}
}
