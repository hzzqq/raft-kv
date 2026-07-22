// client_retry_test.go —— 验证 kvcli 客户端级重试（#74）：对网络错误 / 503 / 504 等
// 瞬态故障指数退避重试；不配置重试时首次瞬态即失败。全程 cluster-free（flaky httptest）。
package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// newFlakyServer 返回一个前 failTimes 次返回 503、之后返回 "ok" 的 httptest 服务。
func newFlakyServer(t *testing.T, failTimes int) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	n := 0
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n++
		cur := n
		mu.Unlock()
		if cur <= failTimes {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, "ok")
	}))
}

func TestClientRetryTransient(t *testing.T) {
	// 不重试：前两次 503、第三次才成功。无重试客户端第一次即失败。
	noRetry := newFlakyServer(t, 2)
	defer noRetry.Close()
	cNo := NewClient(noRetry.URL)
	if _, err := cNo.Get("k"); err == nil {
		t.Fatal("expected error without retry (first 503 should fail fast)")
	}

	// 重试 3 次：前两次 503 被重试，第三次成功返回 "ok"。
	withRetry := newFlakyServer(t, 2)
	defer withRetry.Close()
	c := NewClient(withRetry.URL)
	c.SetRetry(3, 10*time.Millisecond)
	v, err := c.Get("k")
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if v != "ok" {
		t.Fatalf("got %q want ok", v)
	}
}

func TestClientRetryExhausted(t *testing.T) {
	// 服务端恒 503：重试耗尽后仍应返回错误（不无限阻塞）。
	srv := newFlakyServer(t, 1<<30)
	defer srv.Close()
	c := NewClient(srv.URL)
	c.SetRetry(2, 5*time.Millisecond) // 最多 3 次尝试
	start := time.Now()
	_, err := c.Get("k")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected error after retries exhausted")
	}
	// 上限退避 2s * 2 次 ≈ 4s；应远小于此（failTimes 服务器立即返回，无 sleep 之外开销）。
	if elapsed > 5*time.Second {
		t.Fatalf("retry took too long: %v (should fail fast)", elapsed)
	}
}
