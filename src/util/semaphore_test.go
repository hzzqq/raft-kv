package util

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestSemaphoreBasic 验证：容量为 2 时，第 3 个获取须阻塞直到释放。
func TestSemaphoreBasic(t *testing.T) {
	s := NewSemaphore(2)
	if s.Cap() != 2 {
		t.Fatalf("cap=%d want 2", s.Cap())
	}
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("acquire1: %v", err)
	}
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("acquire2: %v", err)
	}
	// 第 3 个应阻塞。
	got := make(chan error, 1)
	go func() { got <- s.Acquire(context.Background()) }()
	select {
	case err := <-got:
		t.Fatalf("第 3 个获取不应立即成功，却返回 %v", err)
	case <-time.After(50 * time.Millisecond):
		// 正确：阻塞中
	}
	s.Release() // 释放一个
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("释放后获取应成功，却返回 %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("释放后第 3 个获取仍阻塞")
	}
	s.Release()
}

// TestSemaphoreContextCancel 验证：容量占满时带取消的获取返回 ctx.Err() 且不泄漏许可。
func TestSemaphoreContextCancel(t *testing.T) {
	s := NewSemaphore(1)
	_ = s.Acquire(context.Background()) // 占满
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消
	if err := s.Acquire(ctx); err == nil {
		t.Fatal("已取消的获取应返回错误")
	}
	// 释放后容量应恢复，新获取可立即成功（无泄漏）。
	s.Release()
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("释放后获取应成功，却返回 %v", err)
	}
	s.Release()
}

// TestSemaphoreWeighted 验证：权重获取按权重占位，剩余不足时阻塞。
func TestSemaphoreWeighted(t *testing.T) {
	s := NewSemaphore(4)
	if err := s.AcquireWeighted(context.Background(), 3); err != nil {
		t.Fatalf("acquire 3: %v", err)
	}
	got := make(chan error, 1)
	go func() { got <- s.AcquireWeighted(context.Background(), 2) }() // 仅剩 1，需 2 → 阻塞
	select {
	case err := <-got:
		t.Fatalf("权重不足时不应立即成功，却返回 %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	s.ReleaseWeighted(3) // 全部释放
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("释放后权重获取应成功，却返回 %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("释放后权重获取仍阻塞")
	}
	s.ReleaseWeighted(2)
}

// TestSemaphoreNoUnderflow 验证：超额释放不会让容量突破上限（无符号下溢/负许可）。
func TestSemaphoreNoUnderflow(t *testing.T) {
	s := NewSemaphore(2)
	s.Release() // 未获取即释放
	s.Release()
	s.Release()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ { // 恰好两个许可可同时持有
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Acquire(context.Background()); err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			s.Release()
		}()
	}
	wg.Wait()
}

// TestSemaphoreTryAcquire 验证：TryAcquire 非阻塞——满时立即失败，释放后可成功；InUse 准确。
func TestSemaphoreTryAcquire(t *testing.T) {
	s := NewSemaphore(2)
	if !s.TryAcquire() || !s.TryAcquire() {
		t.Fatal("前两个 TryAcquire 应成功")
	}
	if s.InUse() != 2 {
		t.Fatalf("InUse=%d want 2", s.InUse())
	}
	if s.TryAcquire() {
		t.Fatal("已满时 TryAcquire 应失败（满即拒）")
	}
	if s.InUse() != 2 {
		t.Fatalf("失败后 InUse 应仍为 2，实际 %d", s.InUse())
	}
	s.Release()
	if !s.TryAcquire() {
		t.Fatal("释放一个后 TryAcquire 应成功")
	}
	if s.InUse() != 2 {
		t.Fatalf("InUse=%d want 2", s.InUse())
	}
	s.Release()
	s.Release()
	if s.InUse() != 0 {
		t.Fatalf("全释放后 InUse=%d want 0", s.InUse())
	}
}

// TestSemaphoreTryAcquireNoPartial 验证：权重不足时 TryAcquireWeighted 不残留部分获取。
func TestSemaphoreTryAcquireNoPartial(t *testing.T) {
	s := NewSemaphore(2)
	if !s.TryAcquireWeighted(2) {
		t.Fatal("恰好 2 许可应成功")
	}
	if s.TryAcquireWeighted(1) {
		t.Fatal("已满时权重获取应失败")
	}
	if s.InUse() != 2 {
		t.Fatalf("失败不应残留许可，InUse=%d want 2", s.InUse())
	}
	s.ReleaseWeighted(2)
	if s.InUse() != 0 {
		t.Fatalf("释放后 InUse=%d want 0", s.InUse())
	}
}
