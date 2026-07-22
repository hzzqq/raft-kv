package util

import (
	"testing"
	"time"
)

func newTestCB() (*CircuitBreaker, *time.Time) {
	nowVal := time.Unix(0, 0)
	now := &nowVal
	cb := NewCircuitBreaker(3, 2, 100*time.Millisecond)
	cb.now = func() time.Time { return *now }
	return cb, now
}

// TestCircuitBreakerClosed 验证：初始 Closed，放行。
func TestCircuitBreakerClosed(t *testing.T) {
	cb, _ := newTestCB()
	if cb.State() != CBClosed {
		t.Fatalf("初始应为 Closed，实际 %v", cb.State())
	}
	if !cb.Allow() {
		t.Fatalf("Closed 应放行")
	}
}

// TestCircuitBreakerTrip 验证：连续失败达阈值转 Open，之后拒绝放行。
func TestCircuitBreakerTrip(t *testing.T) {
	cb, now := newTestCB() // threshold=3
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}
	if cb.State() != CBOpen {
		t.Fatalf("连续 3 次失败应转 Open，实际 %v", cb.State())
	}
	// 冷却未到，拒绝。
	if cb.Allow() {
		t.Fatalf("Open 且冷却未到应拒绝")
	}
	// 前进冷却时间后允许（转 HalfOpen）。
	*now = now.Add(150 * time.Millisecond)
	if !cb.Allow() {
		t.Fatalf("冷却后应转 HalfOpen 并放行")
	}
}

// TestCircuitBreakerHalfOpenRecover 验证：HalfOpen 连续成功达阈值→Closed。
func TestCircuitBreakerHalfOpenRecover(t *testing.T) {
	cb, now := newTestCB()
	for i := 0; i < 3; i++ { // trip
		cb.RecordFailure()
	}
	*now = now.Add(150 * time.Millisecond) // 冷却
	cb.Allow()                             // → HalfOpen
	if cb.State() != CBHalfOpen {
		t.Fatalf("应为 HalfOpen，实际 %v", cb.State())
	}
	cb.RecordSuccess()
	if cb.State() != CBHalfOpen { // successThresh=2，还需一次
		t.Fatalf("首次成功仍 HalfOpen，实际 %v", cb.State())
	}
	cb.RecordSuccess()
	if cb.State() != CBClosed {
		t.Fatalf("连续 2 次成功应回 Closed，实际 %v", cb.State())
	}
}

// TestCircuitBreakerHalfOpenFail 验证：HalfOpen 失败→重新 Open 冷却。
func TestCircuitBreakerHalfOpenFail(t *testing.T) {
	cb, now := newTestCB()
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}
	*now = now.Add(150 * time.Millisecond)
	cb.Allow() // HalfOpen
	cb.RecordFailure()
	if cb.State() != CBOpen {
		t.Fatalf("HalfOpen 失败应回 Open，实际 %v", cb.State())
	}
}

// TestCircuitBreakerResetOnSuccess 验证：Closed 下成功重置失败计数（不误熔断）。
func TestCircuitBreakerResetOnSuccess(t *testing.T) {
	cb, _ := newTestCB()
	cb.RecordFailure()
	cb.RecordFailure() // 2 次
	cb.RecordSuccess() // 重置
	cb.RecordFailure()
	cb.RecordFailure() // 又 2 次，未满阈值 3
	if cb.State() != CBClosed {
		t.Fatalf("成功重置后不应熔断，实际 %v", cb.State())
	}
}
