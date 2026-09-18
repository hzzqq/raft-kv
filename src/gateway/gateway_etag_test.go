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

// TestETagConditionalGet 验证 ETag 回写与 If-None-Match 命中时 304 不回源。
func TestETagConditionalGet(t *testing.T) {
	s := newCacheServer()
	s.SetETag(true)
	var calls atomic.Int32
	stub := func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		io.WriteString(w, `{"hello":"world"}`)
	}
	ts := httptest.NewServer(s.Wrap(stub))
	defer ts.Close()

	// 第一次 GET：200 + ETag
	req1, _ := http.NewRequest("GET", ts.URL+"/kv/e", nil)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	if resp1.StatusCode != 200 {
		t.Fatalf("first GET status=%d", resp1.StatusCode)
	}
	etag := resp1.Header.Get("ETag")
	if etag == "" {
		t.Fatalf("expected ETag header on first GET")
	}

	// 带 If-None-Match 的第二次 GET：应 304 且不回源
	req2, _ := http.NewRequest("GET", ts.URL+"/kv/e", nil)
	req2.Header.Set("If-None-Match", etag)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotModified {
		t.Fatalf("expected 304, got %d", resp2.StatusCode)
	}
	if c := calls.Load(); c != 1 {
		t.Fatalf("expected 1 backend call (304 short-circuits), got %d", c)
	}

	// 带错误 If-None-Match：应 200 且再次回源
	req3, _ := http.NewRequest("GET", ts.URL+"/kv/e", nil)
	req3.Header.Set("If-None-Match", `"wrong"`)
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != 200 {
		t.Fatalf("expected 200 for mismatched etag, got %d", resp3.StatusCode)
	}
	if c := calls.Load(); c != 2 {
		t.Fatalf("expected 2 backend calls after mismatched etag, got %d", c)
	}
	_ = b1
}

// TestETagConditionalGetOnCacheHit 证明 I209：SetCache+SetETag 双开时，缓存命中路径
// 也必须服务条件 GET。修复前 wrap 中缓存命中检查先于 If-None-Match 检查，命中即全量
// 回放 200——生产配置（main.go SetCache+SetETag）下条件 GET 永远得不到 304，退化为
// 整包传输，浪费带宽。既有 TestETagConditionalGet 因 newCacheServer 不开缓存而
// 全程走 miss 路径，覆盖不到该缺口。
//
// 复现路径（修复前必失败）：
//  1. GET /kv/k -> 200 + ETag（回源并落缓存/落 ETag）
//  2. 带 If-None-Match 的第二次 GET：命中缓存 → 修复前直接 200 全量回放（应 304）
//  3. PUT 失效后带旧 ETag：不得再 304，回源得新值 + 新 ETag（与 I208 护栏联动回归）
func TestETagConditionalGetOnCacheHit(t *testing.T) {
	s := newCacheServer() // compress/security headers 关：缓存键固定为 "GET /kv/k plain"
	s.SetCache(time.Hour, 8)
	s.SetETag(true)

	var mu sync.Mutex
	val := "v1"
	var calls int32
	// 经 Wrap 注入 stub handler（cluster-free），GET/PUT 与 handleGet/handlePut 的
	// 缓存交互同口径：GET 读当前值，PUT 写新值并失效缓存。
	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			mu.Lock()
			cur := val
			mu.Unlock()
			atomic.AddInt32(&calls, 1)
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			io.WriteString(w, cur)
			return
		}
		mu.Lock()
		val = "v2"
		mu.Unlock()
		s.invalidateKeyCache("/kv/k")
		w.WriteHeader(http.StatusOK)
	})

	do := func(method string, hdr map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/kv/k", nil)
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec
	}

	// 1. 首次 GET：200 + ETag（回源并落缓存/落 ETag）
	r1 := do(http.MethodGet, nil)
	if r1.Code != http.StatusOK {
		t.Fatalf("first GET = %d, want 200", r1.Code)
	}
	etag := r1.Header().Get("ETag")
	if etag == "" {
		t.Fatal("first GET should carry ETag")
	}

	// 2. 命中缓存的条件 GET：应 304 不回源、无 body（修复前：命中即 200 全量回放）
	r2 := do(http.MethodGet, map[string]string{"If-None-Match": etag})
	if r2.Code != http.StatusNotModified {
		t.Fatalf("conditional GET on cache hit = %d, want 304 (I209：缓存命中短路先于 If-None-Match 检查)", r2.Code)
	}
	if r2.Body.Len() != 0 {
		t.Fatalf("304 must carry no body, got %d bytes", r2.Body.Len())
	}

	// 3. If-None-Match 不匹配：仍命中缓存回放 200，不回源
	r3 := do(http.MethodGet, map[string]string{"If-None-Match": `"wrong"`})
	if r3.Code != http.StatusOK || r3.Body.String() != "v1" {
		t.Fatalf("mismatched INM on cache hit = %d body=%q, want 200 v1", r3.Code, r3.Body.String())
	}

	// 4. 无 If-None-Match 的普通 GET：命中缓存回放 200
	r4 := do(http.MethodGet, nil)
	if r4.Code != http.StatusOK || r4.Body.String() != "v1" {
		t.Fatalf("plain GET on cache hit = %d body=%q, want 200 v1", r4.Code, r4.Body.String())
	}
	if c := atomic.LoadInt32(&calls); c != 1 {
		t.Fatalf("populated cache should serve all subsequent GETs (calls=1), got %d", c)
	}

	// 5. PUT 失效后携带旧 ETag：不得再 304，回源得新值 + 新 ETag
	if rec := do(http.MethodPut, nil); rec.Code != http.StatusOK {
		t.Fatalf("PUT failed: %d", rec.Code)
	}
	r5 := do(http.MethodGet, map[string]string{"If-None-Match": etag})
	if r5.Code != http.StatusOK || r5.Body.String() != "v2" {
		t.Fatalf("conditional GET after invalidation = %d body=%q, want 200 v2", r5.Code, r5.Body.String())
	}
	freshETag := r5.Header().Get("ETag")
	if freshETag == "" || freshETag == etag {
		t.Fatalf("GET after invalidation should carry a new ETag, got %q (old %q)", freshETag, etag)
	}

	// 6. 新值已落缓存：带新 ETag 的条件 GET 再次 304（此次走缓存命中路径）
	r6 := do(http.MethodGet, map[string]string{"If-None-Match": freshETag})
	if r6.Code != http.StatusNotModified {
		t.Fatalf("conditional GET with fresh etag on cache hit = %d, want 304", r6.Code)
	}
	if c := atomic.LoadInt32(&calls); c != 2 {
		t.Fatalf("expected exactly 2 backend calls, got %d", c)
	}
}

// TestETagConditionalGetOnCacheHitGzip 证明 I209 在压缩变体下同样成立：
// gzip 与明文是两个独立缓存键（cacheKey 含 Accept-Encoding 维度），各自的
// ETag 独立匹配——gzip 变体命中缓存时带 If-None-Match 也必须 304（修复前
// 同样退化为 200 整包回放压缩体，带宽浪费加倍）。
func TestETagConditionalGetOnCacheHitGzip(t *testing.T) {
	s := NewServer(nil)
	s.SetSecurityHeaders(false) // compress 保持默认开启
	s.SetCache(time.Hour, 8)
	s.SetETag(true)

	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		io.WriteString(w, "v1")
	})

	do := func(acceptGzip bool, inm string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/kv/k", nil)
		if acceptGzip {
			req.Header.Set("Accept-Encoding", "gzip")
		}
		if inm != "" {
			req.Header.Set("If-None-Match", inm)
		}
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec
	}

	// 首次 GET（gzip 变体）：回源落 gzip 缓存（键含 gzip 维度）
	r1 := do(true, "")
	if r1.Code != http.StatusOK {
		t.Fatalf("first GET = %d, want 200", r1.Code)
	}
	if ce := r1.Header().Get("Content-Encoding"); ce != "gzip" {
		t.Fatalf("first GET Content-Encoding = %q, want gzip", ce)
	}
	etag := r1.Header().Get("ETag")
	if etag == "" {
		t.Fatal("first GET should carry ETag")
	}

	// 命中 gzip 变体缓存的条件 GET：应 304
	r2 := do(true, etag)
	if r2.Code != http.StatusNotModified {
		t.Fatalf("conditional GET on gzip cache hit = %d, want 304 (I209)", r2.Code)
	}

	// 未落缓存的 plain 变体（无对应 ETag 记录）：不得误 304，回源 200
	r3 := do(false, etag)
	if r3.Code != http.StatusOK {
		t.Fatalf("conditional GET on uncached plain variant = %d, want 200 (per-variant ETag isolation)", r3.Code)
	}
}

// TestCacheHitReplayCarriesETag 证明 cycle 209：缓存命中回放的 200 必须带 ETag 头，
// 与回源路径观测口径一致。修复前 cacheSet（克隆响应头）先于 w.Header().Set("ETag")
// 执行，缓存快照头不含 ETag，replayCache 原样回放——生产 SetCache+SetETag 双开时
// 客户端自第二次 GET 起拿到的 200 全部没有 ETag，无从发起条件 GET，I209 的 304
// 短路在真实流量中永远等不到 If-None-Match（条件 GET 链路静默退化）。
// 既有覆盖缺口：TestETagConditionalGetOnCacheHit 断言 ETag 的两次响应（第 1/5 步）
// 与 scope 测试里「cached GET 带 ETag」实为 miss 路径（PUT 失效后/首次 GET），
// 纯命中回放的 200 从未被断言过带 ETag。
//
// 复现路径（修复前必失败）：
//  1. GET /kv/k -> 200 + ETag（回源落缓存，快照头缺 ETag）
//  2. 普通 GET -> 缓存命中回放 200：必须带同一 ETag（修复前静默丢失）
//  3. 用回放得到的 ETag 发条件 GET -> 304（端到端闭环，依赖第 2 步补齐）
func TestCacheHitReplayCarriesETag(t *testing.T) {
	s := newCacheServer() // compress/security headers 关：缓存键固定为 "GET /kv/k plain"
	s.SetCache(time.Hour, 8)
	s.SetETag(true)

	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		io.WriteString(w, "v1")
	})
	do := func(inm string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/kv/k", nil)
		if inm != "" {
			req.Header.Set("If-None-Match", inm)
		}
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec
	}

	// 1. 首次 GET（miss 回源）：200 + ETag
	r1 := do("")
	if r1.Code != http.StatusOK || r1.Body.String() != "v1" {
		t.Fatalf("first GET = %d body=%q, want 200 v1", r1.Code, r1.Body.String())
	}
	etag := r1.Header().Get("ETag")
	if etag == "" {
		t.Fatal("first GET should carry ETag")
	}

	// 2. 纯缓存命中回放：200 必须带同一 ETag（修复前：快照头缺 ETag，静默丢失）
	r2 := do("")
	if r2.Code != http.StatusOK || r2.Body.String() != "v1" {
		t.Fatalf("cache-hit GET = %d body=%q, want 200 v1", r2.Code, r2.Body.String())
	}
	if got := r2.Header().Get("ETag"); got != etag {
		t.Fatalf("cache-hit replay ETag = %q, want %q (回放路径 ETag 观测口径必须与回源一致)", got, etag)
	}

	// 3. 端到端闭环：客户端用回放得到的 ETag 发条件 GET 应 304 不回放
	r3 := do(r2.Header().Get("ETag"))
	if r3.Code != http.StatusNotModified {
		t.Fatalf("conditional GET with replayed ETag = %d, want 304", r3.Code)
	}
}
