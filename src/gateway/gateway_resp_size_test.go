// gateway_resp_size_test.go —— X-Response-Size 响应体大小头的 cluster-free 单测（#205）。
package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// respSizeStub 是一个 Write-first（隐式 200、单次 Write）的 stub handler，
// 直接走 s.Wrap 即可验证外层 metricsWriter 的字节计数/X-Response-Size 注入，
// 不依赖任何生产路由的写响应风格（cluster-free，不启 raft 集群）。
func respSizeStub(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
}

// TestGatewayResponseSizeHeader 验证：Write-first 的 JSON 响应，X-Response-Size
// 精确等于完整 body 字节数（net/http 隐式 200 + 自动 Content-Length 同口径）。
func TestGatewayResponseSizeHeader(t *testing.T) {
	s := timingTestServer()
	body := `{"hello":"world","n":123}`
	ts := httptest.NewServer(s.Wrap(respSizeStub(body)))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/anything")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	v := resp.Header.Get("X-Response-Size")
	if v == "" {
		t.Fatalf("missing X-Response-Size header")
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("X-Response-Size = %q not an integer: %v", v, err)
	}
	if n != int64(len(got)) || int64(len(got)) != int64(len(body)) {
		t.Fatalf("X-Response-Size = %d, want %d (body len)", n, len(body))
	}
}

// TestGatewayResponseSizeOverGzip 验证：开启 gzip 时 X-Response-Size 仍被写入，且
// 反映「线上压缩后」字节（>=0 的整数计数，由 metricsWriter 在首写时记录）。
// 注：http.Client 默认自动解压，故仅断言头存在且为非负整数，不比较解压后 body 长度。
func TestGatewayResponseSizeOverGzip(t *testing.T) {
	s := timingTestServer()
	s.compress = true
	body := `{"hello":"world","n":123,"msg":"compress me please"}`
	ts := httptest.NewServer(s.Wrap(respSizeStub(body)))
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/anything", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	v := resp.Header.Get("X-Response-Size")
	if v == "" {
		t.Fatalf("gzip path missing X-Response-Size header")
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("X-Response-Size = %q not an integer: %v", v, err)
	}
	if n < 0 {
		t.Fatalf("X-Response-Size = %d, want >=0", n)
	}
}

// TestGatewayRequestSizeHeader 验证：入站请求带已知 Content-Length 时，响应头
// X-Request-Size 精确等于请求体声明字节数（与 X-Response-Size 配对，便于识别
// 超大请求/带宽可观测）。分块（ContentLength=-1）的未知长度由 metricsWriter 跳过（不输出负值）。
func TestGatewayRequestSizeHeader(t *testing.T) {
	s := timingTestServer()
	body := `{"key":"value","payload":"raft-kv-request"}`
	ts := httptest.NewServer(s.Wrap(respSizeStub("ok")))
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/upload", strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	v := resp.Header.Get("X-Request-Size")
	if v == "" {
		t.Fatalf("missing X-Request-Size header")
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("X-Request-Size = %q not an integer: %v", v, err)
	}
	if n != int64(len(body)) {
		t.Fatalf("X-Request-Size = %d, want %d", n, len(body))
	}
}
