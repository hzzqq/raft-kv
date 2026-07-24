package main

import (
	"context"
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

// TestMGetBoundedContextCancel 验证：限流信号量支持 ctx 取消——当 ctx 已取消时，
// sem.Acquire(ctx) 立即返回 ctx.Err()（而非阻塞在满信号量上），MGet 不应挂死，
// 且被阻塞的 key 被记为 ctx 错误。这是 #207 用 util.Semaphore 替换裸 channel 后
// 新增的取消语义（cluster-free，mock 网关即可触发）。
func TestMGetBoundedContextCancel(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// ctx 已取消时 handler 不应被调用（信号量在派发 goroutine 前就拦截）。
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "x")
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	c.SetMaxConcurrent(1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 预先取消

	keys := []string{"a", "b", "c", "d"}
	done := make(chan struct{})
	var res MGetResult
	go func() {
		res = c.MGetCtx(ctx, keys)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("MGetCtx hung despite cancelled ctx + bounded concurrency")
	}
	if len(res.Errors) != len(keys) {
		t.Fatalf("want all %d keys errored (ctx), got %d: %v", len(keys), len(res.Errors), res.Errors)
	}
}
