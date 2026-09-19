// gateway_bench_test.go —— 网关数据面微基准（cluster-free，经 s.Wrap 注入 stub 回源）。
//
// 与既有单测同构（httptest + Recorder），但直接以 httptest.NewRequest + NewRecorder
// 逐请求驱动 wrap 管线（无 TCP/loopback 噪声），量化 15 轮正确性修复所叠加的网关
// 管线在热路径上的开销：
//   - GET 缓存命中（plain / gzip 变体）：replayCache 回放路径（I209/I213 口径）；
//   - GET 缓存回源 miss（plain / gzip 变体）：完整管线 + computeETag(SHA256) +
//     cacheSet/etagSet 落缓存（gzip 变体额外含压缩税，I213 实际传输表示口径）；
//   - 条件 GET 304 短路：If-None-Match 命中 serveNotModified（I211/I214 口径）；
//   - PUT 写路径：含写失效代数护栏（I193 invalidateKeyCache + I208 invalGen 递增）。
//
// 口径说明（docs/benchmarks.md 有完整基线与对比方法，数字绑定机器环境，跨机器不可直接比）：
//   - 单客户端串行驱动，SetClientRateLimit(0,0) 关闭令牌桶（生产默认 200rps/burst40
//     会把高频基准打成 429，限流语义已有 limiter_e2e_test 覆盖，非本基准被测对象）；
//   - 压缩/安全头保持 NewServer 生产默认；缓存 + ETag 双开与 main.go 生产配置一致
//     （plain 变体请求不带 Accept-Encoding，不触发 gzip，可与 gzip 变体对照）；
//   - miss 基准以 1024 key 轮转 + cacheMax=8 维持稳态全 miss（FIFO 先于复用淘汰），
//     etagStore 以 1024 个 key 为上界，内存有界；
//   - 每迭代新建 Recorder 属各基准同口径的固定 harness 开销，不影响相对对比。
//
// 运行：go test ./src/gateway/ -run '^$' -bench 'BenchmarkGatewayGet|BenchmarkGatewayPut' -benchtime 1s
package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// benchBody 是基准回源响应体（≈504B 文本值），代表典型 KV value 规模：
// 小到不失真于缓存回放，大到让 gzip 压缩与 SHA256 ETag 的开销可观测。
var benchBody = strings.Repeat("raftkv-bench-0123456789abcdef", 18)

// benchKeySpace 是 miss 基准的 key 轮转空间：与 cacheMax=8 组合保证稳态全 miss。
const benchKeySpace = 1024

// newBenchServer 构造生产口径的 cluster-free 基准网关：缓存 + ETag 双开（与 main.go
// 一致），仅关闭每客户端限流（理由见文件头口径说明）。
func newBenchServer() *Server {
	s := NewServer(nil)
	s.SetClientRateLimit(0, 0)
	s.SetCache(time.Hour, 8)
	s.SetETag(true)
	return s
}

// benchStubGET 回源 stub：与真实 handleGet 同口径写出定长文本响应。
func benchStubGET(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	io.WriteString(w, benchBody)
}

// benchReq 构造一个带可选头的基准请求（Accept-Encoding 用于 gzip 变体）。
func benchReq(path, acceptEncoding string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	return req
}

// BenchmarkGatewayGetCacheMiss 缓存未命中的完整回源路径：wrap 早期管线（请求 ID、
// 限流/追踪头）+ 信号量 + 回源 + computeETag + cacheSet/etagSet 落缓存。
func BenchmarkGatewayGetCacheMiss(b *testing.B) {
	s := newBenchServer()
	h := s.Wrap(benchStubGET)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h(httptest.NewRecorder(), benchReq("/kv/m"+strconv.Itoa(i%benchKeySpace), ""))
	}
}

// BenchmarkGatewayGetCacheMissGzip 同上，但客户端接受 gzip：额外含响应体压缩与
// 对压缩字节的 ETag 计算。与 plain miss 相除即网关「压缩税」。
func BenchmarkGatewayGetCacheMissGzip(b *testing.B) {
	s := newBenchServer()
	h := s.Wrap(benchStubGET)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h(httptest.NewRecorder(), benchReq("/kv/mg"+strconv.Itoa(i%benchKeySpace), "gzip"))
	}
}

// BenchmarkGatewayGetCacheHit 缓存命中回放路径：replayCache（头克隆 + body 回写 +
// 访问日志/指标记录），不回源、不压缩、不重算 ETag。网关热路径最快口径。
func BenchmarkGatewayGetCacheHit(b *testing.B) {
	s := newBenchServer()
	h := s.Wrap(benchStubGET)
	h(httptest.NewRecorder(), benchReq("/kv/hit", "")) // 预热：回源一次落缓存（计时外）
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h(httptest.NewRecorder(), benchReq("/kv/hit", ""))
	}
}

// BenchmarkGatewayGetCacheHitGzip gzip 变体缓存命中：回放的是回源时压缩落存的字节
// （不重复压缩），额外含 gzip 变体缓存键解析（I212 编码维度分键）开销。
func BenchmarkGatewayGetCacheHitGzip(b *testing.B) {
	s := newBenchServer()
	h := s.Wrap(benchStubGET)
	h(httptest.NewRecorder(), benchReq("/kv/hitg", "gzip")) // 预热 gzip 变体缓存
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h(httptest.NewRecorder(), benchReq("/kv/hitg", "gzip"))
	}
}

// BenchmarkGatewayGetETag304 条件 GET 短路：If-None-Match 命中已存 ETag →
// serveNotModified 304（无 body、无信号量占用、不回源）。缓存关闭以隔离纯 ETag
// 短路（缓存命中路径的 304 已有 I209/I214 单测覆盖语义）。与 CacheHit 对照即
// 「条件 GET 相对整包回放的节省」。
func BenchmarkGatewayGetETag304(b *testing.B) {
	s := newBenchServer()
	s.SetCacheEnabled(false)
	h := s.Wrap(benchStubGET)
	// 预热：一次普通 GET 落 ETag（计时外），捕获回写值供条件 GET 命中。
	warm := httptest.NewRecorder()
	h(warm, benchReq("/kv/etag", ""))
	etag := warm.Header().Get("ETag")
	if etag == "" {
		b.Fatal("warm-up GET did not produce ETag")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := benchReq("/kv/etag", "")
		req.Header.Set("If-None-Match", etag)
		h(httptest.NewRecorder(), req)
	}
}

// BenchmarkGatewayPutPath 写路径：PUT 经完整早期管线 + 信号量后交给 handler，stub 在
// 写成功后调用 invalidateKeyCache（与真实 handlePut/handleAppend 同口径），量化写失效
// 护栏的每写开销：plain/gzip 双变体缓存与 ETag 键构造 + 双锁对 + I208 invalGen 递增。
// 写密集稳态下缓存中通常无该 key 条目（前一次写已清空），循环即该稳态的真实口径。
func BenchmarkGatewayPutPath(b *testing.B) {
	s := newBenchServer()
	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) {
		s.invalidateKeyCache(r.URL.Path)
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{"ok":true}`)
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPut, "/kv/pk", strings.NewReader("v"))
		h(httptest.NewRecorder(), req)
	}
}
