package main

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestClientGzip 验证开启 gzip 后：请求带 Accept-Encoding、服务端压缩、客户端透明解压得明文。
func TestClientGzip(t *testing.T) {
	body := "hello-gzip-world"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Set("Vary", "Accept-Encoding")
			gz := gzip.NewWriter(w)
			gz.Write([]byte(body))
			gz.Close()
			return
		}
		io.WriteString(w, body)
	}))
	defer srv.Close()

	// 未开启 gzip：服务端明文返回，客户端读到原文
	cPlain := NewClient(srv.URL)
	if v, err := cPlain.Get("k"); err != nil || v != body {
		t.Fatalf("plain: got %q err %v", v, err)
	}

	// 开启 gzip：服务端压缩，客户端透明解压
	cGz := NewClient(srv.URL)
	cGz.EnableGzip()
	if v, err := cGz.Get("k"); err != nil || v != body {
		t.Fatalf("gzip: got %q err %v", v, err)
	}
}

// TestClientPingHealthyReady 验证存活/就绪探针与网关契约一致。
func TestClientPingHealthyReady(t *testing.T) {
	var ready int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(200)
		case "/readyz":
			if atomic.LoadInt32(&ready) == 1 {
				w.WriteHeader(200)
			} else {
				w.WriteHeader(503)
			}
		default:
			io.WriteString(w, "ok")
		}
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("Ping should succeed: %v", err)
	}
	if !c.Healthy(context.Background()) {
		t.Fatalf("Healthy should be true")
	}
	if c.Ready(context.Background()) {
		t.Fatalf("Ready should be false when not marked ready")
	}
	atomic.StoreInt32(&ready, 1)
	if !c.Ready(context.Background()) {
		t.Fatalf("Ready should be true after marked ready")
	}
}

// TestRetryAfterParse 是 retryAfterSeconds 的纯函数单测。
func TestRetryAfterParse(t *testing.T) {
	resp := &http.Response{Header: http.Header{}}
	if _, ok := retryAfterSeconds(resp); ok {
		t.Fatalf("empty header should be false")
	}
	resp.Header.Set("Retry-After", "3")
	if d, ok := retryAfterSeconds(resp); !ok || d != 3*time.Second {
		t.Fatalf("expected 3s, got %v ok=%v", d, ok)
	}
	resp.Header.Set("Retry-After", "9999")
	if d, ok := retryAfterSeconds(resp); !ok || d != 5*time.Second {
		t.Fatalf("expected capped 5s, got %v", d)
	}
}

// TestClientRetryAfter 验证 503+Retry-After 被兑现（等待约 1s 而非纯毫秒级退避）。
func TestClientRetryAfter(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(503)
			return
		}
		io.WriteString(w, "ok")
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	c.SetRetry(3, time.Millisecond)
	start := time.Now()
	v, err := c.Get("k")
	if err != nil {
		t.Fatalf("retry-after should lead to success: %v", err)
	}
	if v != "ok" {
		t.Fatalf("got %q", v)
	}
	if time.Since(start) < 900*time.Millisecond {
		t.Fatalf("expected to honor Retry-After ~1s, got %v", time.Since(start))
	}
}

// TestClientBreaker 验证连续失败触发熔断后，后续请求快速失败且不再打后端。
func TestClientBreaker(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(500)
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	c.EnableBreaker(2, 1, time.Second)
	if err := c.Put("k", "v"); err == nil {
		t.Fatalf("expected error on 500")
	}
	if err := c.Put("k", "v"); err == nil {
		t.Fatalf("expected error on 500")
	}
	if atomic.LoadInt32(&hits) != 2 {
		t.Fatalf("expected 2 server hits, got %d", hits)
	}
	// 第三次应被熔断快速失败，不再打后端
	err := c.Put("k", "v")
	if err == nil || !strings.Contains(err.Error(), "circuit breaker open") {
		t.Fatalf("expected breaker open fast-fail, got %v", err)
	}
	if atomic.LoadInt32(&hits) != 2 {
		t.Fatalf("breaker should block 3rd call (no extra hit), got %d", hits)
	}
}

// TestClientMetrics 验证客户端指标被记录。
func TestClientMetrics(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kv/fail" {
			w.WriteHeader(500)
			return
		}
		io.WriteString(w, "ok")
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	if _, err := c.Get("k"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if _, err := c.Get("fail"); err == nil {
		t.Fatalf("expected error")
	}
	m := c.Metrics()
	if m.Requests < 2 {
		t.Fatalf("expected at least 2 requests, got %d", m.Requests)
	}
	if m.Errors < 1 {
		t.Fatalf("expected at least 1 error, got %d", m.Errors)
	}
	if m.AvgLatency() <= 0 {
		t.Fatalf("expected positive avg latency")
	}
}

// TestClientClose 验证 Close 回收空闲连接后仍能新建连接继续工作。
func TestClientClose(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	if v, err := c.Get("k"); err != nil || v != "ok" {
		t.Fatalf("before close: got %q err %v", v, err)
	}
	c.Close()
	if v, err := c.Get("k"); err != nil || v != "ok" {
		t.Fatalf("after close should still work: got %q err %v", v, err)
	}
}
