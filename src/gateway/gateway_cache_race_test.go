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

// TestStoreFreshAtomicAgainstInvalidate 证明 I221：数据 GET 的落存动作
// （ETag 设头/落存、正/负缓存落存、etagStore 写回）与写失效 invalidateKeyCache
// 必须对写失效代数（invalGen）原子——复检与落存之间不得给 invalidate 留插缝窗口。
//
// 复现路径（修复前必失败；I208 测试拦不住本场景）：
//  1. GET 回源完成，wrap 记 gen0 后做唯一一次 fresh 复检（此刻 PUT 尚未发生 → 通过）
//  2. PUT /kv/k = v2 成功 → invalidateKeyCache：删缓存 + 删 ETag + invalGen++（拦不住步骤 3）
//  3. GET 的 cacheSet/etagSet 随后无条件执行 —— 写前旧值 v1 与陈旧 ETag 复活进
//     cacheStore/etagStore：TTL 内后续 GET 返回旧值；条件 GET 凭复活 ETag 误得 304，
//     客户端继续持有写前旧表示（read-your-writes 静默破坏，I193/I208 同族缺口）。
//
// 确定性白盒探针：storeFresh 的复检（invalGen.Load）与落存之间恰有一次 w.Header()
// 调用（ETag 头 Set），探针 writer 在该调用点同步触发 invalidateKeyCache——
// 修复前（无锁）invalidate 即刻完成、cacheSet/etagSet 照常执行 → 复活必现；
// 修复后（落存段整体持 cacheMu）invalidate 阻塞在锁上，探针超时回退防死锁，
// invalidate 改在落存段释放锁后完成删除+递增 → 复活值被清 → 绿。两条路径均由
// 同步/锁序保证，不依赖调度运气。
func TestStoreFreshAtomicAgainstInvalidate(t *testing.T) {
	s := NewServer(nil)
	s.SetCache(time.Hour, 16) // 长 TTL：确保若复活，测试窗口内必然可观测
	s.SetETag(true)
	ck := "GET /kv/k plain"

	gen0 := s.invalGen.Load() // 模拟 wrap 回源前的代数记录（此刻该 gen 注定复检通过）
	done := make(chan struct{})
	var injectOnce sync.Once
	pw := &probeHeaderWriter{ResponseWriter: httptest.NewRecorder()}
	pw.onFirstHeader = func() {
		injectOnce.Do(func() {
			// 竞态注入：invalidateKeyCache 恰好插在「复检之后、落存之前」。
			go func() {
				s.invalidateKeyCache("/kv/k")
				close(done)
			}()
			// 修复前：无锁竞争，invalidate 即刻完成（等价于「删除+递增」先于落存）；
			// 修复后：invalidate 阻塞在落存段的 cacheMu 上，超时回退让落存先行完成，
			// invalidate 随后拿锁清场（与「失效发生在落存完成之后」的正常语义等价）。
			select {
			case <-done:
			case <-time.After(time.Second):
			}
		})
	}

	s.storeFresh(pw, ck, gen0, http.StatusOK, []byte("v1"))
	<-done // 修复后路径：等 invalidate 清场完成再断言最终态

	if _, ok := s.cacheStore[ck]; ok {
		t.Fatal("写失效后落存仍把写前旧值复活进缓存（I221：复检与落存必须对 gen 原子）")
	}
	if got := s.etagGet(ck); got != "" {
		t.Fatalf("陈旧 ETag %s 复活进 etagStore（I221：后续条件 GET 将凭它误得 304）", got)
	}
}

// probeHeaderWriter 首次 Header() 调用时触发一次注入回调（一次性），用于在
// storeFresh 的复检与落存之间的确定性位置插入写失效。
type probeHeaderWriter struct {
	http.ResponseWriter
	onFirstHeader func()
	once          sync.Once
}

func (p *probeHeaderWriter) Header() http.Header {
	p.once.Do(func() {
		if p.onFirstHeader != nil {
			p.onFirstHeader()
		}
	})
	return p.ResponseWriter.Header()
}

// TestStoreFreshNormalPathStillStores 回归护栏：I221 修复不得把正常路径改坏——
// gen 未变（无并发写）时 storeFresh 行为与原 wrap 内联序列逐项一致：
// 200 正缓存落存（TTL/快照头含 ETag，I210）、ETag 设头与 etagStore 写回（I213 表示口径
// 由调用方保证传入实际传输形式 body）、5xx 负缓存落存、非 fresh 全放弃。
func TestStoreFreshNormalPathStillStores(t *testing.T) {
	s := NewServer(nil)
	s.SetCache(time.Hour, 16)
	s.SetETag(true)
	ck := "GET /kv/k plain"

	// 1. gen 未变 + 200：正缓存 + ETag 双落
	gen0 := s.invalGen.Load()
	rec := httptest.NewRecorder()
	s.storeFresh(rec, ck, gen0, http.StatusOK, []byte("v1"))
	cv := s.cacheGet(ck)
	if cv == nil || string(cv.Body) != "v1" {
		t.Fatalf("正常落存应写入正缓存，got %+v", cv)
	}
	etag := s.etagGet(ck)
	if etag == "" {
		t.Fatal("正常落存应写回 etagStore")
	}
	if rec.Header().Get("ETag") != etag {
		t.Fatalf("ETag 头应与落存值一致，头=%q store=%q", rec.Header().Get("ETag"), etag)
	}
	if cv.Header.Get("ETag") != etag {
		t.Fatal("缓存快照头缺 ETag（I210 回归：快照必须含 ETag）")
	}

	// 2. 5xx：负缓存落存、无 ETag
	s2ck := "GET /kv/e plain"
	genE := s.invalGen.Load()
	recE := httptest.NewRecorder()
	s.storeFresh(recE, s2ck, genE, http.StatusBadGateway, []byte("boom"))
	cvE := s.cacheGet(s2ck)
	if cvE == nil || cvE.Status != http.StatusBadGateway {
		t.Fatalf("5xx 应落负缓存，got %+v", cvE)
	}
	if s.etagGet(s2ck) != "" || recE.Header().Get("ETag") != "" {
		t.Fatal("5xx 不得落 ETag（负缓存无验证器，I209 语义）")
	}

	// 3. gen 已变（回源期间发生写失效）：全放弃（I208 既有语义不变）
	genOld := s.invalGen.Load()
	s.invalGen.Add(1)
	rec3 := httptest.NewRecorder()
	s.storeFresh(rec3, "GET /kv/z plain", genOld, http.StatusOK, []byte("stale"))
	if cv := s.cacheGet("GET /kv/z plain"); cv != nil {
		t.Fatalf("非 fresh 落存应被放弃，got %+v", cv)
	}
	if s.etagGet("GET /kv/z plain") != "" || rec3.Header().Get("ETag") != "" {
		t.Fatal("非 fresh 时 ETag 不得落存/设头（I208 语义）")
	}
}
