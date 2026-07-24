// gateway_concurrency_test.go —— 网关并发上限（util.Semaphore）的 cluster-free 单测（#211）。
// 直接构造 Server + s.Wrap（自定义阻塞 handler），不启进程内 raft 集群（沙箱纪律）。
package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"raftkv/src/util"
)

// TestGatewayConcurrencyReject 验证：sem 容量为 1 时，已在途的请求占满槽位，
// 第二并发请求应被 TryAcquire 非阻塞拒绝（429 满即拒），在途请求完成后才恢复。
func TestGatewayConcurrencyReject(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	s := &Server{
		sem:            util.NewSemaphore(1),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
		startedAt:      time.Now(),
	}
	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) {
		close(entered) // 进入 handler 即代表已持有唯一并发槽
		<-release
		io.WriteString(w, "ok")
	})
	ts := httptest.NewServer(h)
	defer ts.Close()

	done1 := make(chan int, 1)
	go func() {
		resp, err := http.Get(ts.URL + "/x")
		if err != nil {
			done1 <- -1
			return
		}
		defer resp.Body.Close()
		done1 <- resp.StatusCode
	}()

	// 等待请求 1 真正占住槽位（handler 已执行），确保并发上限已饱和。
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("请求 1 未在超时内进入 handler（信号量未放行）")
	}

	// 请求 2 此时应被非阻塞拒绝（429），而非阻塞等待。
	resp2, err := http.Get(ts.URL + "/y")
	if err != nil {
		t.Fatalf("请求 2 发出失败：%v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("并发满时请求 2 应 429，实际 %d", resp2.StatusCode)
	}

	// 放行请求 1，确认槽位释放后首个被拒的请求通道恢复（此处仅验证不挂死）。
	close(release)
	select {
	case code1 := <-done1:
		if code1 != http.StatusOK {
			t.Fatalf("请求 1 应 200，实际 %d", code1)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("请求 1 在放行后仍未返回（信号量未释放）")
	}
}

// TestGatewayConcurrentGauge 验证：并发放行时 gateway_concurrent_in_use 观测值随占用变化。
func TestGatewayConcurrentGauge(t *testing.T) {
	release := make(chan struct{})
	hold := make(chan struct{})
	s := &Server{
		sem:            util.NewSemaphore(4),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
		startedAt:      time.Now(),
	}
	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) {
		select {
		case hold <- struct{}{}:
		default:
		}
		<-release
		io.WriteString(w, "ok")
	})
	ts := httptest.NewServer(h)
	defer ts.Close()

	const n = 3
	done := make(chan int, n)
	for i := 0; i < n; i++ {
		go func() {
			resp, err := http.Get(ts.URL + "/x")
			if err != nil {
				done <- -1
				return
			}
			defer resp.Body.Close()
			done <- resp.StatusCode
		}()
	}
	// 等 3 个请求全部进入 handler（各持一个槽）。
	for i := 0; i < n; i++ {
		select {
		case <-hold:
		case <-time.After(2 * time.Second):
			t.Fatalf("第 %d 个请求未进入 handler", i+1)
		}
	}
	// 槽位占用应等于 n（4 容量内未满，不应有 429）。
	if got := s.sem.InUse(); got != n {
		t.Fatalf("并发占满时 InUse=%d want %d", got, n)
	}
	close(release)
	for i := 0; i < n; i++ {
		select {
		case c := <-done:
			if c != http.StatusOK {
				t.Fatalf("请求应 200，实际 %d", c)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("请求未返回")
		}
	}
	if got := s.sem.InUse(); got != 0 {
		t.Fatalf("全部释放后 InUse=%d want 0", got)
	}
}
