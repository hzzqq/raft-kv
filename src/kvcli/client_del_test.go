package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestClientDel(t *testing.T) {
	srv, store := newStatefulKVServer(t)
	defer srv.Close()
	c := NewClient(srv.URL)

	store.put("k1", "v1")
	if err := c.Del("k1"); err != nil {
		t.Fatalf("Del 应成功，实际 %v", err)
	}
	if _, ok := store.get("k1"); ok {
		t.Fatal("Del 后 key 应已被删除")
	}
	// 删除不存在的 key 也应成功（幂等）
	if err := c.Del("nope"); err != nil {
		t.Fatalf("删除不存在的 key 应成功，实际 %v", err)
	}
	// 网关非 200（如 405 错误方法）应返回带响应体的错误
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer bad.Close()
	c2 := NewClient(bad.URL)
	if err := c2.Del("x"); err == nil {
		t.Fatal("网关 405 应返回错误")
	}
}

// closeTrackingBody 包装响应体，记录是否被 Close（响应体泄漏回归断言用）。
type closeTrackingBody struct {
	io.ReadCloser
	closed *atomic.Bool
}

func (b *closeTrackingBody) Close() error {
	b.closed.Store(true)
	return b.ReadCloser.Close()
}

// closeTrackingTransport 包装内层 Transport，把每个响应体替换为可观测 Close 的版本。
type closeTrackingTransport struct {
	inner  http.RoundTripper
	closed *atomic.Bool
}

func (t *closeTrackingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.inner.RoundTrip(req)
	if err == nil && resp != nil && resp.Body != nil {
		resp.Body = &closeTrackingBody{ReadCloser: resp.Body, closed: t.closed}
	}
	return resp, err
}

// TestClientDelClosesResponseBody 验证 Del 的所有出径（成功 200 / 业务错误非 200）
// 都关闭响应体。缺陷：deleteCtx 仅在 503/504 重试路径 Close，成功路径 return nil 前、
// 业务错误 return respErr(...) 前均不 Close——响应体未关闭的连接无法归还连接池复用，
// 每调用一次 Del/MDel 泄漏一个连接（fd 累积至 GC finalizer 才兜底，长跑进程可耗尽 fd），
// 与包内 fetchGet/putCtx/appendCtx/Ping/Healthy/Ready「每条出径必 Close」纪律不一致。
func TestClientDelClosesResponseBody(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{"success 200", http.StatusOK, false},
		{"error 405", http.StatusMethodNotAllowed, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()
			var closed atomic.Bool
			c := NewClient(srv.URL)
			c.http = &http.Client{Transport: &closeTrackingTransport{inner: http.DefaultTransport, closed: &closed}}
			err := c.Del("k")
			if tc.wantErr && err == nil {
				t.Fatal("非 200 响应应返回错误")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Del 应成功，实际 %v", err)
			}
			if !closed.Load() {
				t.Fatalf("Del 在 %d 响应返回后未 Close 响应体：连接泄漏（不可复用，fd 累积）", tc.status)
			}
		})
	}
}
