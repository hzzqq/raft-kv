// gateway_gzip_pool_test.go —— T-149：gzip.Writer 池化复用下的流完整性锁（2026-09-19）。
//
// 池化 gzip.Writer 的核心风险面是脏状态复用：未经成功 Close（提前 return / 底层写
// 错误 / handler panic 后 Close）的 writer 若带状态回池，下一个请求会产出损坏的
// gzip 流——静默数据损坏比性能退化严重得多。本文件用确定性测试锁住：
//  1. 连续多次请求（池复用稳定发生），逐次解码验证产物与原文一致；
//  2. handler 中途 panic（wrap 的 defer Close 仍执行，成功回池）后的下一请求流仍完整。
package main

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// gzipPoolPayload 大于 gzip.Writer 内部缓冲（默认级别下 ~64KB 级），强制多轮 flush，
// 放大「复用对象带残留缓冲/压缩状态」这类脏状态缺陷的暴露概率。
var gzipPoolPayload = strings.Repeat("raftkv-gzip-pool-0123456789abcdef-", 4096)

// assertPooledGzipBody 断言该响应为 200 + Content-Encoding: gzip，且 gzip 流可完整
// 解码回原文（流损坏/截断在 NewReader/ReadAll 处报错）。
func assertPooledGzipBody(t *testing.T, rec *httptest.ResponseRecorder, payload string) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", rec.Header().Get("Content-Encoding"))
	}
	gr, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("gzip.NewReader: %v（池化复用产出损坏流？）", err)
	}
	out, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("gzip stream decode: %v（池化复用产出截断/损坏流？）", err)
	}
	gr.Close()
	if string(out) != payload {
		t.Fatalf("decompressed body mismatch: got %d bytes, want %d", len(out), len(payload))
	}
}

func gzipPoolReq(path string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Accept-Encoding", "gzip")
	return req
}

// TestGzipPooledWriterStreamIntegrity：池化复用下 gzip 流完整性（T-149）。
func TestGzipPooledWriterStreamIntegrity(t *testing.T) {
	s := NewServer(nil)
	s.SetSecurityHeaders(false)
	s.SetClientRateLimit(0, 0) // 关闭每客户端令牌桶（默认 200rps 会把高频请求打成 429）
	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kv/pool-panic" { // 精确匹配（子串会误伤 pool-after-panic 路径）
			io.WriteString(w, "partial")
			panic("boom: mid-write handler failure")
		}
		io.WriteString(w, gzipPoolPayload)
	})

	// 1) 连续 64 次：首次之后即走池复用路径，逐次解码验证流完整。
	for i := 0; i < 64; i++ {
		rec := httptest.NewRecorder()
		h(rec, gzipPoolReq("/kv/pool"))
		assertPooledGzipBody(t, rec, gzipPoolPayload)
	}

	// 2) handler panic：wrap 的 defer 关闭路径仍执行（Close 成功 → writer 回池），
	//    panic 沿调用栈传播由测试侧 recover。随后请求大概率复用刚回池的 writer，
	//    必须产出完整流——锁死脏状态复用风险面。
	func() {
		defer func() { _ = recover() }()
		rec := httptest.NewRecorder()
		h(rec, gzipPoolReq("/kv/pool-panic"))
	}()
	rec := httptest.NewRecorder()
	h(rec, gzipPoolReq("/kv/pool-after-panic"))
	assertPooledGzipBody(t, rec, gzipPoolPayload)
}
