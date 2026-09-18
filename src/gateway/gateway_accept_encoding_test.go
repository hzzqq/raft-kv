package main

// cycle 211 / I212：Accept-Encoding 的 gzip 判定此前用 strings.Contains 子串匹配，
// "gzip;q=0"（客户端按 RFC 9110 §12.5.3 显式拒绝 gzip）也命中子串，导致两个实锤偏差：
//  1. wrap 对明确不可接受压缩的客户端发送 gzip 响应（Content-Encoding: gzip 的字节流
//     客户端无法解码，静默坏）；
//  2. cacheKey 把 q=0 客户端归入 gzip 编码变体键，plain/gzip 双变体缓存维度被污染。
// 修复为 acceptsGzip：解析逗号分隔的 coding;q=weight 列表，仅当存在显式 "gzip" 条目
// 且权重 q>0（缺省 q=1）才判定可接受；通配 * 不触发压缩（保守方向：宁可少压缩，
// 不错发客户端不可接受的编码）。
//
// 测试环境注意：请求手动设置 Accept-Encoding 后，Go transport 不自动加头也不做
// 透明解压，故响应 body 即客户端真实收到的原始字节，可直接断言。

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// doAE 向 ts 发一个手动指定 Accept-Encoding 的 GET（/kv/ 数据路径），
// 返回状态码、Content-Encoding 与原始 body。
func doAE(t *testing.T, ts *httptest.Server, ae string) (int, string, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/kv/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ae != "" {
		req.Header.Set("Accept-Encoding", ae)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header.Get("Content-Encoding"), string(b)
}

// TestAcceptEncodingQZeroNotCompressed：Accept-Encoding 显式拒绝 gzip（q=0）时
// 网关必须回明文；显式接受（q>0 缺省/正权重）时才压缩。修复前 q=0 三例被
// Contains 子串匹配误判为接受 gzip，响应体被压缩成客户端无法解码的字节。
func TestAcceptEncodingQZeroNotCompressed(t *testing.T) {
	s := NewServer(nil)
	s.SetSecurityHeaders(false)
	payload := "plain-payload"
	ts := httptest.NewServer(s.Wrap(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, payload)
	}))
	defer ts.Close()

	cases := []struct {
		ae        string
		wantCE    string
		wantPlain bool // body 是否应为明文 payload
	}{
		{"gzip;q=0", "", true},           // 显式拒绝 gzip → 明文
		{"deflate, gzip;q=0", "", true},  // 列表尾显式拒绝 → 明文
		{"gzip;q=0, identity", "", true}, // 列表首显式拒绝 → 明文
		{"gzip", "gzip", false},          // 对照：缺省权重接受 → 压缩
		{"gzip;q=0.5", "gzip", false},    // 对照：正权重接受 → 压缩
		{"deflate", "", true},            // 对照：不含 gzip → 明文
	}
	for _, tc := range cases {
		code, ce, body := doAE(t, ts, tc.ae)
		if code != http.StatusOK {
			t.Errorf("AE %q: status = %d, want 200", tc.ae, code)
			continue
		}
		if ce != tc.wantCE {
			t.Errorf("AE %q: Content-Encoding = %q, want %q", tc.ae, ce, tc.wantCE)
		}
		if tc.wantPlain && body != payload {
			t.Errorf("AE %q: body not plaintext (len=%d), want plaintext %q", tc.ae, len(body), payload)
		}
		if tc.wantCE == "gzip" {
			gr, err := gzip.NewReader(strings.NewReader(body))
			if err != nil {
				t.Errorf("AE %q: body not gzip: %v", tc.ae, err)
				continue
			}
			out, _ := io.ReadAll(gr)
			if string(out) != payload {
				t.Errorf("AE %q: decompressed body = %q, want %q", tc.ae, string(out), payload)
			}
		}
	}
}

// TestAcceptEncodingCacheVariantsIndependent：q=0 客户端与真实 gzip 客户端必须落
// 在不同的缓存编码变体键（I212 的缓存维度）：q=0 走 plain 键存明文，gzip 客户端
// 独立回源压缩，互不串味。修复前 q=0 客户端被归入 gzip 键，其首取响应即被压缩，
// 且与 gzip 客户端共用同一缓存条目（calls 不会到 2）。
func TestAcceptEncodingCacheVariantsIndependent(t *testing.T) {
	s := NewServer(nil)
	s.SetSecurityHeaders(false)
	s.SetCache(2*time.Second, 8)
	payload := "cached-payload"
	var mu sync.Mutex
	var calls int
	ts := httptest.NewServer(s.Wrap(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		io.WriteString(w, payload)
	}))
	defer ts.Close()

	// 1) q=0 客户端首取：明文回源
	code, ce, body := doAE(t, ts, "gzip;q=0")
	if code != http.StatusOK || ce != "" || body != payload {
		t.Fatalf("q=0 first GET: status=%d CE=%q body-len=%d, want 200/''/plaintext", code, ce, len(body))
	}
	// 2) q=0 客户端二取：命中 plain 键缓存回放，仍明文
	code, ce, body = doAE(t, ts, "gzip;q=0")
	if code != http.StatusOK || ce != "" || body != payload {
		t.Fatalf("q=0 cached GET: status=%d CE=%q body-len=%d, want 200/''/plaintext", code, ce, len(body))
	}
	mu.Lock()
	if calls != 1 {
		mu.Unlock()
		t.Fatalf("after two q=0 GETs calls = %d, want 1 (plain-variant cache hit)", calls)
	}
	mu.Unlock()

	// 3) gzip 客户端：独立 gzip 变体键，回源压缩
	code, ce, body = doAE(t, ts, "gzip")
	if code != http.StatusOK || ce != "gzip" {
		t.Fatalf("gzip GET: status=%d CE=%q, want 200/gzip", code, ce)
	}
	gr, err := gzip.NewReader(strings.NewReader(body))
	if err != nil {
		t.Fatalf("gzip GET body not gzip: %v", err)
	}
	out, _ := io.ReadAll(gr)
	if string(out) != payload {
		t.Fatalf("gzip GET decompressed = %q, want %q", string(out), payload)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Fatalf("calls = %d, want 2 (plain/gzip variants each fetched once)", calls)
	}
}
