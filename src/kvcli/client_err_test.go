// client_err_test.go —— kvcli 错误路径与 Bench 超时的 cluster-free 单测。
// 不依赖真实 raft 集群，直接以 httptest 模拟网关的各种响应，验证：
//  1. 网关返回非 200 时，错误信息包含状态码与响应体（而非静默丢弃）；
//  2. Bench 在后端挂死时受整体墙钟超时约束，不会无限拖尾。
package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// errHandler 返回指定状态码与错误体，用于验证客户端的错误透传。
func errHandler(status int, body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		io.WriteString(w, body)
	})
}

func TestClientGetErrorBody(t *testing.T) {
	ts := httptest.NewServer(errHandler(http.StatusNotFound, `{"error":"key not found"}`))
	defer ts.Close()

	cl := NewClient(ts.URL)
	_, err := cl.Get("missing")
	if err == nil {
		t.Fatal("expected error for 404, got nil")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Fatalf("error should contain status code 404: %v", err)
	}
	if !strings.Contains(err.Error(), "key not found") {
		t.Fatalf("error should contain response body 'key not found': %v", err)
	}
}

func TestClientPutErrorBody(t *testing.T) {
	ts := httptest.NewServer(errHandler(http.StatusInternalServerError, "raft not leader"))
	defer ts.Close()

	cl := NewClient(ts.URL)
	err := cl.Put("k", "v")
	if err == nil {
		t.Fatal("expected error for 500, got nil")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Fatalf("error should contain status code 500: %v", err)
	}
	if !strings.Contains(err.Error(), "raft not leader") {
		t.Fatalf("error should contain response body 'raft not leader': %v", err)
	}
}

func TestClientAppendErrorBody(t *testing.T) {
	ts := httptest.NewServer(errHandler(http.StatusBadRequest, "value too large"))
	defer ts.Close()

	cl := NewClient(ts.URL)
	err := cl.Append("k", "v")
	if err == nil {
		t.Fatal("expected error for 400, got nil")
	}
	if !strings.Contains(err.Error(), "400") || !strings.Contains(err.Error(), "value too large") {
		t.Fatalf("error should surface status and body: %v", err)
	}
}

// slowHandler 故意在 timeout 之后才响应，用于验证 Bench 的整体超时能提前退出。
func slowHandler(delay time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(delay):
			io.WriteString(w, "ok")
		case <-r.Context().Done():
			return // ctx 取消后直接退出，不写响应
		}
	})
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(delay):
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
			return
		}
	})
	return mux
}

func TestBenchWithTimeout(t *testing.T) {
	// 后端每个请求耗时 200ms，但 Bench 整体超时仅 80ms。
	// 若整体超时生效，应在 ~80ms 而非 200ms*ops 后返回。
	ts := httptest.NewServer(slowHandler(200 * time.Millisecond))
	defer ts.Close()

	cl := NewClient(ts.URL)
	start := time.Now()
	res := cl.BenchWithTimeout(40, 4, "mixed", 16, 80*time.Millisecond)
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Fatalf("Bench ignored overall timeout: took %s (want < 2s)", elapsed)
	}
	if res.Ops != 40 {
		t.Fatalf("Bench Ops = %d, want 40", res.Ops)
	}
	if res.Workers != 4 {
		t.Fatalf("Bench Workers = %d, want 4", res.Workers)
	}
	// 在超时 backend 下，大量请求应被计入错误（ctx 取消）。
	if res.Errors == 0 {
		t.Fatalf("Bench Errors = 0, expected > 0 under slow backend with tight timeout")
	}
}
