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

// TestGatewayDataPathETagConditional 证明 I194 的前置正确性：
// 数据路径 GET /kv/{key} 在仅开启 ETag（缓存关）时，条件 GET（带 If-None-Match）
// 必须返回 304，说明 ETag/条件-GET 确实作用于数据路径（未被 isDataGET 误伤）。
func TestGatewayDataPathETagConditional(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)
	s.SetCacheEnabled(false) // 仅验证 ETag 分支
	s.SetETag(true)
	s.SetCompress(false)
	s.SetSecurityHeaders(false)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	do := func(method, path, body, inm string) (*http.Response, string) {
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
		if inm != "" {
			r.Header.Set("If-None-Match", inm)
		}
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp, string(b)
	}

	_, _ = do("PUT", "/kv/scope", "v1", "")
	r1, _ := do("GET", "/kv/scope", "", "")
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("GET /kv/scope first = %d, want 200", r1.StatusCode)
	}
	etag := r1.Header.Get("ETag")
	if etag == "" {
		t.Fatal("GET /kv/scope should carry ETag when SetETag on")
	}
	r2, _ := do("GET", "/kv/scope", "", etag)
	if r2.StatusCode != http.StatusNotModified {
		t.Fatalf("conditional GET /kv/scope = %d, want 304 (data-path ETag/condition-GET broken)", r2.StatusCode)
	}
}

// TestGatewayDiagnosticsNotCached 证明 I194 的核心护栏：
// 缓存+ETag 全开后，数据路径 GET /kv/{key} 命中缓存（200 并带 ETag），
// 而诊断/指标端点 /metrics、/status 必须返回 200、不带 ETag，且带
// If-None-Match 仍返回 200（不被 304 冻结）——即被 isDataGET 正确排除。
func TestGatewayDiagnosticsNotCached(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)
	s.SetCache(2*time.Second, 1024)
	s.SetETag(true)
	s.SetCompress(false)
	s.SetSecurityHeaders(false)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	do := func(method, path, body, inm string) (*http.Response, string) {
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
		if inm != "" {
			r.Header.Set("If-None-Match", inm)
		}
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp, string(b)
	}

	_, _ = do("PUT", "/kv/scope", "v1", "")
	// 数据路径：缓存命中返回 200（并带 ETag），证明缓存/ETag 在数据路径生效
	rb, _ := do("GET", "/kv/scope", "", "")
	if rb.StatusCode != http.StatusOK {
		t.Fatalf("GET /kv/scope = %d, want 200", rb.StatusCode)
	}
	if rb.Header.Get("ETag") == "" {
		t.Fatal("cached GET /kv/scope should carry ETag")
	}
	// 诊断 /metrics：200，不带 ETag
	rm, _ := do("GET", "/metrics", "", "")
	if rm.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", rm.StatusCode)
	}
	if rm.Header.Get("ETag") != "" {
		t.Fatal("GET /metrics must NOT carry ETag (would freeze live metrics)")
	}
	// 带 If-None-Match 的 /metrics 仍 200（不被 304 冻结）
	rm2, _ := do("GET", "/metrics", "", rb.Header.Get("ETag"))
	if rm2.StatusCode != http.StatusOK {
		t.Fatalf("conditional GET /metrics = %d, want 200 (must not be 304-frozen)", rm2.StatusCode)
	}
	// 诊断 /status 同样不得被 ETag/缓存影响
	rs, _ := do("GET", "/status", "", "")
	if rs.StatusCode != http.StatusOK {
		t.Fatalf("GET /status = %d, want 200", rs.StatusCode)
	}
	if rs.Header.Get("ETag") != "" {
		t.Fatal("GET /status must NOT carry ETag (would freeze live status)")
	}
	rs2, _ := do("GET", "/status", "", rb.Header.Get("ETag"))
	if rs2.StatusCode != http.StatusOK {
		t.Fatalf("conditional GET /status = %d, want 200 (must not be 304-frozen)", rs2.StatusCode)
	}
}
