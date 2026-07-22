package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestClientRequestTimeout 验证：未传 deadline 时，请求级超时生效，慢后端触发超时错误。
func TestClientRequestTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.SetRequestTimeout(20 * time.Millisecond)
	_, err := c.Get("k")
	if err == nil {
		t.Fatalf("期望超时错误，实际 nil")
	}
	if !strings.Contains(err.Error(), "deadline") && !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("期望超时类错误，实际 %v", err)
	}
}

// TestClientRequestTimeoutNoOverride 验证：调用方 ctx 已带更长 deadline 时，请求级短超时不覆盖。
func TestClientRequestTimeoutNoOverride(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.SetRequestTimeout(10 * time.Millisecond) // 请求级短超时
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v, err := c.GetCtx(ctx, "k")
	if err != nil {
		t.Fatalf("调用方长 deadline 应覆盖短请求级超时，期望成功，实际 %v", err)
	}
	if v != "ok" {
		t.Fatalf("期望 'ok'，实际 %q", v)
	}
}

// TestClientRequestTimeoutDisabled 验证：默认不附加请求级超时，慢后端仍成功。
func TestClientRequestTimeoutDisabled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer srv.Close()
	c := NewClient(srv.URL) // 默认无请求级超时
	v, err := c.Get("k")
	if err != nil {
		t.Fatalf("无超时期望成功，实际 %v", err)
	}
	if v != "ok" {
		t.Fatalf("期望 'ok'，实际 %q", v)
	}
}
