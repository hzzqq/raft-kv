package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"raftkv/src/cluster"
)

// TestGatewayCacheInvalidatedOnWrite 证明：开启响应缓存后，PUT/Append 必须立即使
// 该 key 的 GET 缓存失效，否则网关会向客户端返回写入前的旧值（违反线性一致）。
//
// 复现路径（修复前必失败）：
//  1. PUT /kv/x = v1
//  2. GET /kv/x -> v1（回源并写入缓存）
//  3. PUT /kv/x = v2（修复前：不失效缓存）
//  4. GET /kv/x -> 修复前返回缓存的 v1（陈旧），修复后回源返回 v2
//
// 这是 I193 的回归测试：原 handlePut/handleAppend 不调用 invalidateKeyCache。
func TestGatewayCacheInvalidatedOnWrite(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)
	s.SetCache(2*time.Second, 64) // 开启响应缓存（SetCache 标注"生产可用"）
	s.SetCompress(false)
	s.SetSecurityHeaders(false)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	do := func(method, path, body string) string {
		var r *http.Request
		var err error
		if body == "" {
			r, err = http.NewRequest(method, ts.URL+path, nil)
		} else {
			r, err = http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		}
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}

	// 写 v1，读应为 v1
	do("PUT", "/kv/bug", "v1")
	if g := do("GET", "/kv/bug", ""); g != "v1" {
		t.Fatalf("after first PUT, GET = %q, want v1", g)
	}
	// 写 v2 后再读：必须反映新值，不得返回缓存的旧 v1
	do("PUT", "/kv/bug", "v2")
	if g := do("GET", "/kv/bug", ""); g != "v2" {
		t.Fatalf("stale read after second PUT: GET = %q, want v2 (cache not invalidated on write)", g)
	}

	// Append 同样必须失效缓存：Append "!" 后 GET 应为 "v2!"
	do("POST", "/kv/bug/append", "!")
	if g := do("GET", "/kv/bug", ""); g != "v2!" {
		t.Fatalf("stale read after Append: GET = %q, want v2! (cache not invalidated on append)", g)
	}
}
