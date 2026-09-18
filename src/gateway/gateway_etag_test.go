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

// TestETagIfNoneMatchStarWildcard 证明 I211：If-None-Match: * 按 RFC 7232 §3.2 语义
// 匹配"存在任意当前表示"，而非字面量字符串比对。修复前网关对 * 做强比较精确匹配，
// * 永不等于已存强 ETag——携带 * 的条件 GET 全部退化为全量 200。
// 保守边界：该维度无已存 ETag（从未回源/已被写失效清除）时 * 不命中，回退 200
// （多传数据不会错发 304，与 I209 既有保守口径一致）。
//
// 复现路径（修复前必失败）：
//  1. 首次 GET 落 ETag 后，带 If-None-Match: * 的 GET：修复前 200 全量回源（应 304）
//  2. invalidateKeyCache 清除已存 ETag 后，带 * 的 GET：200（无当前表示可证，保守不命中）
func TestETagIfNoneMatchStarWildcard(t *testing.T) {
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

	// 0. 未回源过的 key：无已存 ETag 可证当前表示存在 → * 不命中，保守 200 回源
	req0, _ := http.NewRequest("GET", ts.URL+"/kv/star", nil)
	req0.Header.Set("If-None-Match", "*")
	resp0, err := http.DefaultClient.Do(req0)
	if err != nil {
		t.Fatal(err)
	}
	resp0.Body.Close()
	if resp0.StatusCode != 200 {
		t.Fatalf("star on unknown key = %d, want 200 (conservative)", resp0.StatusCode)
	}
	if c := calls.Load(); c != 1 {
		t.Fatalf("star on unknown key should hit backend once, got %d", c)
	}

	// 1. 首次普通 GET：落 ETag
	req1, _ := http.NewRequest("GET", ts.URL+"/kv/star", nil)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	resp1.Body.Close()
	if resp1.StatusCode != 200 || resp1.Header.Get("ETag") == "" {
		t.Fatalf("first GET = %d etag=%q, want 200 with ETag", resp1.StatusCode, resp1.Header.Get("ETag"))
	}

	// 2. If-None-Match: *：命中当前表示 → 304 不回源（修复前：* 与强 ETag 字面不等，200 回源）
	req2, _ := http.NewRequest("GET", ts.URL+"/kv/star", nil)
	req2.Header.Set("If-None-Match", "*")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotModified {
		t.Fatalf("If-None-Match: * = %d, want 304 (I211：* 应按存在性语义匹配)", resp2.StatusCode)
	}
	if c := calls.Load(); c != 2 {
		t.Fatalf("star hit must short-circuit (calls=2), got %d", c)
	}

	// 3. 写失效清除已存 ETag 后：无当前表示可证 → * 不命中，回源 200
	s.invalidateKeyCache("/kv/star")
	req3, _ := http.NewRequest("GET", ts.URL+"/kv/star", nil)
	req3.Header.Set("If-None-Match", "*")
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != 200 {
		t.Fatalf("star after invalidation = %d, want 200 (no known representation)", resp3.StatusCode)
	}
}

// TestETagIfNoneMatchWeakComparison 证明 I211：RFC 7232 §2.3.2 要求 If-None-Match
// 一律用弱比较（opaque 串一致即等价，忽略 W/ 弱前缀）。修复前网关对整个头值做强
// 比较精确匹配，W/"<opaque>" 永不命中强 ETag——客户端/中间层以弱标签形态回传的
// 条件 GET 全部退化为全量 200；多标签列表（"a", W/"b"）同样整串比对永不命中。
//
// 复现路径（修复前必失败）：
//  1. INM: W/"<已存强 ETag 的 opaque>" → 修复前 200（应 304）
//  2. INM: "other", W/"<opaque>" 列表任一命中 → 修复前 200（应 304）
//  3. INM: W/"nope" → 200（不命中，正确行为对照）
//  4. INM: "unterminated 非法片段 → 200（忽略不 panic，不误命中）
func TestETagIfNoneMatchWeakComparison(t *testing.T) {
	s := newCacheServer()
	s.SetETag(true)
	stub := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		io.WriteString(w, `{"hello":"world"}`)
	}
	ts := httptest.NewServer(s.Wrap(stub))
	defer ts.Close()

	req1, _ := http.NewRequest("GET", ts.URL+"/kv/w", nil)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	resp1.Body.Close()
	etag := resp1.Header.Get("ETag")
	if etag == "" || etag[0] != '"' {
		t.Fatalf("first GET should carry strong ETag, got %q", etag)
	}
	opaque := etag[1 : len(etag)-1]

	cases := []struct {
		name string
		inm  string
		want int
	}{
		{"weak tag of stored strong etag", `W/` + etag, http.StatusNotModified},
		{"list any-match", `"other", W/"` + opaque + `"`, http.StatusNotModified},
		{"weak mismatch", `W/"nope"`, 200},
		{"malformed fragment ignored", `"unterminated`, 200},
		{"strong mismatch", `"deadbeef"`, 200},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest("GET", ts.URL+"/kv/w", nil)
		req.Header.Set("If-None-Match", tc.inm)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("INM %q = %d, want %d (I211 弱比较)", tc.inm, resp.StatusCode, tc.want)
		}
	}
}

// TestETagStarAndWeakOnCacheHit 证明 I211 在缓存命中短路路径（I209）同样成立：
// * 与 W/ 标签在命中路径也得 304，而非退化为全量回放 200——两处条件请求比对
// 共用 etagMatches，语义必须同口径。
func TestETagStarAndWeakOnCacheHit(t *testing.T) {
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

	// 首次 GET（miss 回源）：200 + 强 ETag，值落缓存
	r1 := do("")
	if r1.Code != http.StatusOK {
		t.Fatalf("first GET = %d, want 200", r1.Code)
	}
	etag := r1.Header().Get("ETag")
	if etag == "" {
		t.Fatal("first GET should carry ETag")
	}

	// 命中缓存 + If-None-Match: *：304（修复前：* 精确匹配失败 → 200 整包回放）
	if r2 := do("*"); r2.Code != http.StatusNotModified {
		t.Fatalf("star on cache hit = %d, want 304 (I211)", r2.Code)
	}

	// 命中缓存 + W/ 弱标签：304（修复前：同上退化为 200 回放）
	if r3 := do(`W/` + etag); r3.Code != http.StatusNotModified {
		t.Fatalf("weak tag on cache hit = %d, want 304 (I211)", r3.Code)
	}

	// 不匹配的弱标签：仍 200 回放
	if r4 := do(`W/"wrong"`); r4.Code != http.StatusOK || r4.Body.String() != "v1" {
		t.Fatalf("weak mismatch on cache hit = %d body=%q, want 200 v1", r4.Code, r4.Body.String())
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

// TestETagGzipVariantMatchesTransferredRepresentation 证明 I213：gzip 变体的 ETag
// 必须对"实际传输表示"（压缩后字节）计算，而非压缩前明文。RFC 9110 §8.8.1：强 ETag
// 标识特定表示（含内容编码）——同一资源的 gzip 与 plain 是两个表示，修复前两者
// ETag 同为 SHA256(明文) 完全相同，且与线上传输的压缩字节不对应：压缩参数一旦变化
// （压缩字节变、明文不变），条件 GET 凭旧 ETag 错误命中 304，客户端继续复用已失效
// 的压缩表示。
//
// 复现路径（修复前必失败）：
//  1. 同值分别以 gzip / plain 变体 GET：两变体 ETag 相同（修复前均为明文哈希）
//  2. gzip 变体 ETag ≠ SHA256(实际收到的压缩字节)
//  对照：plain 变体 ETag == SHA256(明文字节)，修复前后均过，证明测试抓的是
//  gzip 变体口径而非哈希方向写反。
func TestETagGzipVariantMatchesTransferredRepresentation(t *testing.T) {
	s := NewServer(nil)
	s.SetSecurityHeaders(false) // compress 保持默认开启
	s.SetETag(true)

	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		io.WriteString(w, "v1")
	})

	do := func(acceptGzip bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/kv/k", nil)
		if acceptGzip {
			req.Header.Set("Accept-Encoding", "gzip")
		}
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec
	}

	rg := do(true)
	rp := do(false)
	if rg.Code != http.StatusOK || rp.Code != http.StatusOK {
		t.Fatalf("GET status gzip=%d plain=%d, want 200/200", rg.Code, rp.Code)
	}
	if ce := rg.Header().Get("Content-Encoding"); ce != "gzip" {
		t.Fatalf("gzip variant Content-Encoding = %q, want gzip", ce)
	}
	if body := rp.Body.String(); body != "v1" {
		t.Fatalf("plain variant body = %q, want plaintext v1", body)
	}
	etagG := rg.Header().Get("ETag")
	etagP := rp.Header().Get("ETag")
	if etagG == "" || etagP == "" {
		t.Fatalf("both variants should carry ETag, got gzip=%q plain=%q", etagG, etagP)
	}

	// 1. 两个编码变体是两个表示，强 ETag 必须互不相同（修复前同为明文哈希）
	if etagG == etagP {
		t.Fatalf("gzip and plain variants must carry distinct ETags (RFC 9110 §8.8.1), both %q", etagG)
	}

	// 2. gzip 变体 ETag 必须等于实际传输字节（压缩体）的哈希
	if want := computeETag(rg.Body.Bytes()); etagG != want {
		t.Fatalf("gzip variant ETag = %q, want hash of transferred (compressed) bytes %q", etagG, want)
	}

	// 3. 对照：plain 变体 ETag 等于实际传输字节（明文）的哈希（既有行为不变）
	if want := computeETag(rp.Body.Bytes()); etagP != want {
		t.Fatalf("plain variant ETag = %q, want hash of transferred (plain) bytes %q", etagP, want)
	}
}
