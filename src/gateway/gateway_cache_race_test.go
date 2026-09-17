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

// TestGatewayCacheNoStaleAcrossConcurrentWrite 证明 I208：GET 回源期间发生写失效时，
// 该 GET 完成后不得把写前旧值落缓存、也不得把陈旧 ETag 写回 etagStore。
//
// 复现路径（修复前必失败）：
//  1. GET /kv/k 开始回源，读到 v1 后被人为挂起（模拟慢后端：迁移抖动下的重试窗口）
//  2. PUT /kv/k = v2 成功 → invalidateKeyCache（此刻缓存为空，删除是 no-op，拦不住步骤 3）
//  3. GET#1 完成返回 v1 —— 修复前 cacheSet/etagSet 无条件执行，写前旧值复活进缓存
//  4. 后续普通 GET 命中缓存返回陈旧 v1（应回源得 v2）；
//     携带旧 ETag 的条件 GET 误得 304（应无法匹配）
//
// 这是 I193 的并发缺口：invalidateKeyCache 的「即时删除」只能拦「写后开始的回源」，
// 拦不住「写前开始、写后完成」的在途回源（I193 回归测试为顺序场景，覆盖不到）。
func TestGatewayCacheNoStaleAcrossConcurrentWrite(t *testing.T) {
	s := NewServer(nil)
	s.SetCache(time.Hour, 16) // 长 TTL：确保陈旧值若被写入，测试窗口内必然命中
	s.SetETag(true)

	var mu sync.Mutex
	val := "v1"
	var calls int32
	entered := make(chan struct{}) // GET#1 已读到旧值并挂起
	release := make(chan struct{}) // 放行 GET#1 完成
	getDone := make(chan *httptest.ResponseRecorder, 1)

	storedETag := func() string {
		s.etagMu.Lock()
		defer s.etagMu.Unlock()
		return s.etagStore["GET /kv/k plain"]
	}

	// 经 Wrap 注入 stub handler（cluster-free），GET/PUT 语义与 handleGet/handlePut
	// 的缓存交互同口径：GET 读当前值，PUT 写新值并失效缓存。
	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			mu.Lock()
			cur := val
			mu.Unlock()
			// 首次 GET：读到旧值后挂起，等 PUT 完成后再返回，制造
			// 「回源读取发生在写之前、响应完成发生在写之后」的竞态窗口。
			if atomic.AddInt32(&calls, 1) == 1 {
				close(entered)
				<-release
			}
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			io.WriteString(w, cur)
			return
		}
		// PUT：写新值并失效缓存（与 handlePut 成功路径一致）
		mu.Lock()
		val = "v2"
		mu.Unlock()
		s.invalidateKeyCache("/kv/k")
		w.WriteHeader(http.StatusOK)
	})

	do := func(method, path string, hdr map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec
	}

	// 1. GET#1 异步回源并挂起在竞态窗口内
	go func() {
		req := httptest.NewRequest(http.MethodGet, "/kv/k", nil)
		rec := httptest.NewRecorder()
		h(rec, req)
		getDone <- rec
	}()
	<-entered

	// 2. PUT 写 v2 并失效（此刻缓存为空，删除 no-op——修复前拦不住步骤 3）
	if rec := do(http.MethodPut, "/kv/k", nil); rec.Code != http.StatusOK {
		t.Fatalf("PUT failed: code=%d", rec.Code)
	}

	// 3. 放行 GET#1 完成（同步等待其 wrap 收尾，保证落缓存时序确定）
	close(release)
	rec1 := <-getDone
	if got := rec1.Body.String(); got != "v1" {
		t.Fatalf("GET#1 应回源时读到的旧值 v1，got %q", got)
	}
	staleETag := storedETag() // 修复前：此处已存陈旧 ETag(v1)

	// 4a. 后续普通 GET：不得命中陈旧 v1，必须回源得 v2
	rec2 := do(http.MethodGet, "/kv/k", nil)
	if got := rec2.Body.String(); got != "v2" {
		t.Fatalf("并发写后的后续 GET 返回陈旧缓存 %q，want 回源 v2（I208：慢 GET 把写前旧值写回缓存）", got)
	}
	// 4b. 陈旧 ETag 不得被慢 GET 写回 etagStore（这是「条件 GET 误 304」的根因：
	// wrap 中缓存命中检查先于 If-None-Match 检查，故直接断言存储态而非 HTTP 态）。
	if staleETag != "" {
		t.Fatalf("陈旧 ETag %s 被慢 GET 写回 etagStore（I208：写失效后完成的回源不得落 ETag）", staleETag)
	}

	// 回归确认：干净回源后缓存/ETag 功能恢复——后续 GET 应命中刚落的 v2 缓存，
	// handler 不再被调用（GET#1 与 4a 各占一次，calls 维持 2）。
	rec4 := do(http.MethodGet, "/kv/k", nil)
	if rec4.Code != http.StatusOK || rec4.Body.String() != "v2" {
		t.Fatalf("干净回源后缓存应恢复服务，got code=%d body=%q", rec4.Code, rec4.Body.String())
	}
	if c := atomic.LoadInt32(&calls); c != 2 {
		t.Fatalf("后续 GET 应命中缓存不再回源（calls 应为 2），got %d", c)
	}
}
