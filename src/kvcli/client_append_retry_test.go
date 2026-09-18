// client_append_retry_test.go —— 验证非幂等 Append 的重试安全边界。
//
// 缺陷背景：POST /kv/{key}/append 是非幂等操作（网关对每个 HTTP 请求分配新
// shardkv seq，无请求级去重）。开启重试的客户端在两类「结果模糊」的失败后
// 自动重试，会把同一值追加两次（静默数据损坏）：
//  1. 网络错误：请求可能已被服务端处理，只是响应丢失；
//  2. 504（ErrTimeout）：shardkv Clerk 等待应用结果超时，op 仍可能在超时后
//     被提交应用（等待器已返回，applier 照常执行）。
//
// 修复语义（fail-closed）：上述两类失败立即返回错误，不自动重试；仅 503
// （ErrWrongLeader=网关在 propose 前明确拒绝，确定未应用）保留重试。
package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestClientAppendNoRetryOnNetworkError：首次请求已落盘但连接被断开（响应丢失），
// 开启重试的客户端不得再次发送追加。
func TestClientAppendNoRetryOnNetworkError(t *testing.T) {
	var mu sync.Mutex
	var appends []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		appends = append(appends, string(b))
		n := len(appends)
		mu.Unlock()
		if n == 1 {
			// 已落盘但不回响应：劫持底层连接并断开，客户端将看到网络错误（EOF）。
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("server does not support hijack")
				return
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.SetRetry(2, time.Millisecond)
	err := c.Append("k", "v1")
	if err == nil {
		t.Fatal("Append 在网络错误（首次可能已应用）后应 fail-closed 返回错误，而非重试后误报成功")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(appends) != 1 {
		t.Fatalf("服务端应恰好收到 1 次追加（非幂等 POST 禁止自动重试），实际收到 %d 次: %q",
			len(appends), appends)
	}
}

// TestClientAppendNoRetryOn504：首次请求返回 504（ErrTimeout，op 可能已应用），
// 不得触发重试。
func TestClientAppendNoRetryOn504(t *testing.T) {
	var mu sync.Mutex
	var appends []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		appends = append(appends, string(b))
		n := len(appends)
		mu.Unlock()
		if n == 1 {
			http.Error(w, "ErrTimeout", http.StatusGatewayTimeout)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.SetRetry(2, time.Millisecond)
	err := c.Append("k", "v1")
	if err == nil {
		t.Fatal("Append 在 504（ErrTimeout=结果模糊）后应 fail-closed 返回错误，而非重试后误报成功")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(appends) != 1 {
		t.Fatalf("服务端应恰好收到 1 次追加，实际收到 %d 次", len(appends))
	}
}

// TestClientAppendRetryOn503：503（ErrWrongLeader=propose 前明确拒绝，确定未应用）
// 保留既有重试语义——这是修复后仍应成立的正向对照，防止过度收紧。
func TestClientAppendRetryOn503(t *testing.T) {
	var mu sync.Mutex
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		mu.Lock()
		hits++
		n := hits
		mu.Unlock()
		if n == 1 {
			http.Error(w, "ErrWrongLeader", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.SetRetry(2, time.Millisecond)
	if err := c.Append("k", "v1"); err != nil {
		t.Fatalf("503（确定未应用）应照常重试直至成功: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if hits != 2 {
		t.Fatalf("预期 503 触发一次重试（共 2 次请求），实际 %d 次", hits)
	}
}
