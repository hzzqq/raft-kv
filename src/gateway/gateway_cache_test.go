package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// newCacheServer 构造一个 cluster-free 网关（nil 集群）并关闭压缩/安全头以简化断言。
func newCacheServer() *Server {
	s := NewServer(nil)
	s.SetCompress(false)
	s.SetSecurityHeaders(false)
	return s
}

func getBody(t *testing.T, ts *httptest.Server, method, path string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestResponseCacheHit 验证 GET 命中缓存后不回源（后端调用计数不增）。
func TestResponseCacheHit(t *testing.T) {
	s := newCacheServer()
	s.SetCache(2*time.Second, 8)
	var mu sync.Mutex
	var calls int
	stub := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		io.WriteString(w, fmt.Sprintf(`{"n":%d}`, n))
	}
	ts := httptest.NewServer(s.Wrap(stub))
	defer ts.Close()

	code1, b1 := getBody(t, ts, "GET", "/x")
	code2, b2 := getBody(t, ts, "GET", "/x")
	if code1 != 200 || code2 != 200 {
		t.Fatalf("status: %d / %d", code1, code2)
	}
	if b1 != b2 {
		t.Fatalf("body changed between calls: %q vs %q (cache should freeze response)", b1, b2)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected 1 backend call (cached), got %d", calls)
	}
}

// TestResponseCacheNegative 验证 5xx 负缓存生效（抖动期不反复回源）。
func TestResponseCacheNegative(t *testing.T) {
	s := newCacheServer()
	s.SetCache(2*time.Second, 8)
	var mu sync.Mutex
	var calls int
	stub := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		w.WriteHeader(503)
		io.WriteString(w, `{"error":"backend down"}`)
	}
	ts := httptest.NewServer(s.Wrap(stub))
	defer ts.Close()

	c1, _ := getBody(t, ts, "GET", "/y")
	c2, _ := getBody(t, ts, "GET", "/y")
	if c1 != 503 || c2 != 503 {
		t.Fatalf("expected 503/503, got %d/%d", c1, c2)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected 1 backend call (negative cache), got %d", calls)
	}
}

// TestResponseCacheExpiry 验证 TTL 过期后回源重新执行。
func TestResponseCacheExpiry(t *testing.T) {
	s := newCacheServer()
	s.SetCache(50*time.Millisecond, 8)
	var mu sync.Mutex
	var calls int
	stub := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		w.WriteHeader(200)
		io.WriteString(w, "ok")
	}
	ts := httptest.NewServer(s.Wrap(stub))
	defer ts.Close()

	getBody(t, ts, "GET", "/z")
	time.Sleep(120 * time.Millisecond) // 超过 TTL
	getBody(t, ts, "GET", "/z")
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Fatalf("expected 2 backend calls after expiry, got %d", calls)
	}
}

// TestResponseCacheFIFO 验证容量满时 FIFO 淘汰最旧条目。
func TestResponseCacheFIFO(t *testing.T) {
	s := newCacheServer()
	s.SetCache(10*time.Second, 2) // 仅容纳 2 条
	var mu sync.Mutex
	var calls int
	stub := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		w.WriteHeader(200)
		io.WriteString(w, "ok")
	}
	ts := httptest.NewServer(s.Wrap(stub))
	defer ts.Close()

	getBody(t, ts, "GET", "/a")
	getBody(t, ts, "GET", "/b")
	getBody(t, ts, "GET", "/c") // 触发淘汰 /a
	getBody(t, ts, "GET", "/a") // /a 已淘汰，应回源
	mu.Lock()
	defer mu.Unlock()
	if calls != 4 {
		t.Fatalf("expected 4 backend calls (FIFO eviction of /a), got %d", calls)
	}
}

// TestResponseCacheSkipNonGet 验证非 GET（如 PUT）不被缓存。
func TestResponseCacheSkipNonGet(t *testing.T) {
	s := newCacheServer()
	s.SetCache(10*time.Second, 8)
	var mu sync.Mutex
	var calls int
	stub := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		w.WriteHeader(200)
		io.WriteString(w, "ok")
	}
	ts := httptest.NewServer(s.Wrap(stub))
	defer ts.Close()

	getBody(t, ts, "PUT", "/w")
	getBody(t, ts, "PUT", "/w")
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Fatalf("expected 2 backend calls (PUT not cached), got %d", calls)
	}
}
