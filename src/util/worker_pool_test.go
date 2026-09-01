package util

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool(t *testing.T) {
	p := NewWorkerPool(4)
	var done int32
	const n = 100
	for i := 0; i < n; i++ {
		if err := p.Submit(func() { atomic.AddInt32(&done, 1) }); err != nil {
			t.Fatalf("Submit 不应报错：%v", err)
		}
	}
	p.StopAndWait()
	if atomic.LoadInt32(&done) != n {
		t.Fatalf("期望执行 %d 个任务，实际 %d", n, done)
	}
	// 停止后提交应报错
	if err := p.Submit(func() {}); err != ErrPoolStopped {
		t.Fatalf("停止后 Submit 应返回 ErrPoolStopped，实际 %v", err)
	}
	// 重复 StopAndWait 必须安全
	p.StopAndWait()
}

func TestWorkerPoolSingleWorker(t *testing.T) {
	p := NewWorkerPool(1)
	var val int32
	p.Submit(func() { atomic.StoreInt32(&val, 7) })
	p.StopAndWait()
	if atomic.LoadInt32(&val) != 7 {
		t.Fatal("单 worker 也应执行任务")
	}
}

// TestWorkerPoolTrySubmitFull 验证非阻塞提交在缓冲满时返回 ErrPoolFull，
// 而非无限阻塞或 panic（R6 可验证收益：背压语义成立）。
//
// 旧写法在重负载下偶发失败：单 worker 被阻塞任务占住时，该在途任务可能仍暂存于
// 缓冲（取决于调度时序），导致"可成功入队数"为 1023 而非 1024。此处用 started
// 通道等 worker 真正取走阻塞任务（离开缓冲）后再计数，消除该时序竞态，断言精确。
func TestWorkerPoolTrySubmitFull(t *testing.T) {
	block := make(chan struct{})
	started := make(chan struct{})
	p := NewWorkerPool(1)
	// 占住唯一 worker，使其无法消费缓冲区。
	p.Submit(func() {
		close(started) // 通知测试：worker 已取出阻塞任务（离开缓冲、占住 worker）
		<-block
	})
	<-started // 等 worker 真正开始执行阻塞任务，确保缓冲中不再占该在途任务
	// 此时缓冲 1024 全空闲，可精确填满。
	for i := 0; i < 1024; i++ {
		if err := p.TrySubmit(func() {}); err != nil {
			t.Fatalf("缓冲区未满前应成功入队，第 %d 次提交返回 %v", i, err)
		}
	}
	// 缓冲区已满，再提交必须被拒。
	if err := p.TrySubmit(func() {}); err != ErrPoolFull {
		t.Fatalf("缓冲满时 TrySubmit 应返回 ErrPoolFull，实际 %v", err)
	}
	close(block)
	p.StopAndWait()
}

// TestWorkerPoolSubmitCtxCanceled 验证 ctx 已取消时 SubmitCtx 立即返回 ctx.Err()，
// 不阻塞、不执行任务（R2 隐性：调用方持锁 / 超时场景下不应被卡住）。
func TestWorkerPoolSubmitCtxCanceled(t *testing.T) {
	p := NewWorkerPool(1)
	ran := int32(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 预先取消
	if err := p.SubmitCtx(ctx, func() { atomic.StoreInt32(&ran, 1) }); err != context.Canceled {
		t.Fatalf("ctx 已取消时 SubmitCtx 应返回 Canceled，实际 %v", err)
	}
	p.StopAndWait()
	if atomic.LoadInt32(&ran) != 0 {
		t.Fatal("ctx 取消的任务不应被执行")
	}
}

// TestWorkerPoolSubmitCtxTimeoutWhileFull 验证「缓冲满 + worker 忙」时 SubmitCtx
// 会随 ctx 超时退出而非永久阻塞（有界并发 + 背压 + ctx 取消三位一体）。
//
// 与 TestWorkerPoolTrySubmitFull 同理：用 started 通道等 worker 取走阻塞任务后再填缓冲，
// 消除"在途任务占缓冲位"的时序竞态（旧写法在重负载下偶发"填满缓冲区失败"）。
func TestWorkerPoolSubmitCtxTimeoutWhileFull(t *testing.T) {
	block := make(chan struct{})
	started := make(chan struct{})
	p := NewWorkerPool(1)
	p.Submit(func() {
		close(started)
		<-block
	}) // 占住 worker
	<-started // 等 worker 取走阻塞任务，缓冲清空
	for i := 0; i < 1024; i++ { // 填满缓冲区
		if err := p.TrySubmit(func() {}); err != nil {
			t.Fatalf("填满缓冲区失败：%v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := p.SubmitCtx(ctx, func() {})
	if err != context.DeadlineExceeded {
		t.Fatalf("ctx 超时时应返回 DeadlineExceeded，实际 %v", err)
	}
	if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
		t.Fatalf("应在超时附近返回，实际耗时 %v", elapsed)
	}
	close(block)
	p.StopAndWait()
}

// TestWorkerPoolStopNoPanicOnConcurrentSubmit 验证「停止期间并发提交」不会因向
// 已关闭通道发送而 panic（重构后 tasks 通道永不关闭，仅 stopCh 关闭）。
func TestWorkerPoolStopNoPanicOnConcurrentSubmit(t *testing.T) {
	p := NewWorkerPool(4)
	var submitted, executed int32
	for i := 0; i < 200; i++ {
		go func() {
			// 不关心成功与否，关键是绝不应 panic。
			_ = p.Submit(func() { atomic.AddInt32(&executed, 1) })
			atomic.AddInt32(&submitted, 1)
		}()
	}
	time.Sleep(5 * time.Millisecond)
	p.StopAndWait()
	if atomic.LoadInt32(&submitted) == 0 {
		t.Fatal("应有并发提交尝试")
	}
	// 停止后提交应稳定报错。
	if err := p.Submit(func() {}); err != ErrPoolStopped {
		t.Fatalf("停止后 Submit 应返回 ErrPoolStopped，实际 %v", err)
	}
}
