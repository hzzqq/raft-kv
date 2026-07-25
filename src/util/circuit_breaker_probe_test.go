package util

import (
	"sync"
	"testing"
	"time"
)

// TestCircuitBreakerHalfOpenBoundedProbes 验证：半开窗口内并发探针被限制在
// successThresh 以内（此前无上限，Open→HalfOpen 后所有并发请求都放行，会洪泛下游）。
func TestCircuitBreakerHalfOpenBoundedProbes(t *testing.T) {
	nowVal := time.Unix(0, 0)
	cb := NewCircuitBreaker(3, 2, 100*time.Millisecond) // successThresh=2
	cb.now = func() time.Time { return nowVal }
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}
	nowVal = nowVal.Add(150 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("冷却后应转 HalfOpen 并放行首个探针")
	}
	if !cb.Allow() {
		t.Fatal("第二个探针应被放行（successThresh=2）")
	}
	if cb.Allow() {
		t.Fatal("半开在途探针达 successThresh 后，其余请求应快速失败")
	}
	// 一个探针成功 → 释放一个名额，可再放行一个。
	cb.RecordSuccess()
	if !cb.Allow() {
		t.Fatal("一个探针成功后应释放名额，再放行一个")
	}
	// 累计 2 次成功 → 关闭。
	cb.RecordSuccess()
	if cb.State() != CBClosed {
		t.Fatalf("累计 2 次成功应回 Closed，实际 %v", cb.State())
	}
}

// TestCircuitBreakerHalfOpenSingleProbe 验证：successThresh=1 时半开只放行一个探针。
func TestCircuitBreakerHalfOpenSingleProbe(t *testing.T) {
	nowVal := time.Unix(0, 0)
	cb := NewCircuitBreaker(3, 1, 100*time.Millisecond)
	cb.now = func() time.Time { return nowVal }
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}
	nowVal = nowVal.Add(150 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("冷却后应转 HalfOpen 并放行探针")
	}
	if cb.Allow() {
		t.Fatal("successThresh=1 时半开只应放行一个探针")
	}
	cb.RecordSuccess() // 探针成功 → 应直接关闭
	if cb.State() != CBClosed {
		t.Fatalf("单次成功应回 Closed，实际 %v", cb.State())
	}
}

// TestCircuitBreakerHalfOpenConcurrentLimit 验证：并发 Allow 在半开窗口下最多放行
// successThresh 个（此处=1），其余快速失败，不会惊群。
func TestCircuitBreakerHalfOpenConcurrentLimit(t *testing.T) {
	nowVal := time.Unix(0, 0)
	cb := NewCircuitBreaker(3, 1, 100*time.Millisecond)
	cb.now = func() time.Time { return nowVal }
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}
	nowVal = nowVal.Add(150 * time.Millisecond) // 已到冷却；下一次 Allow 触发 HalfOpen

	var mu sync.Mutex
	allowed := 0
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if cb.Allow() {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if allowed != 1 {
		t.Fatalf("半开并发下应仅放行 1 个探针，实际 %d", allowed)
	}
}
