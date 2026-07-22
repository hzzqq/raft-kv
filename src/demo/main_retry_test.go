// main_retry_test.go —— 验证 demo.waitHealth 的指数退避重试（#79），全程 cluster-free：
// 用 httptest 起一个前两次返回 503、第三次起 200 的平凡服务，断言 waitHealth 能在有限
// 时间内返回（重试成功）而非永久阻塞，且实际发生了多次探测。
package main

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestDemoWaitHealthRetry(t *testing.T) {
	var mu sync.Mutex
	n := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n++
		cur := n
		mu.Unlock()
		if cur <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	start := time.Now()
	waitHealth(ts.URL, client)
	elapsed := time.Since(start)

	// 应在整体上限内返回（重试成功），不应永久挂起。
	if elapsed > 4*time.Second {
		t.Fatalf("waitHealth took too long: %v (want < 4s)", elapsed)
	}
	// 失败两次后应至少探测到第 3 次（成功）。
	mu.Lock()
	final := n
	mu.Unlock()
	if final < 3 {
		t.Fatalf("expected >=3 probes, got %d", final)
	}
}
