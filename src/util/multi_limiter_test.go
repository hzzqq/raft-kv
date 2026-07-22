package util

import (
	"testing"
)

// TestMultiLimiterAllPass 验证：所有子判据允许时放行。
func TestMultiLimiterAllPass(t *testing.T) {
	var a, b bool = true, true
	ml := NewMultiLimiter(func() bool { return a }, func() bool { return b })
	if !ml.Allow() {
		t.Fatalf("全部允许时应放行")
	}
}

// TestMultiLimiterOneReject 验证：任一子判据拒绝即整体拒绝（AND 语义）。
func TestMultiLimiterOneReject(t *testing.T) {
	var a, b bool = true, false
	ml := NewMultiLimiter(func() bool { return a }, func() bool { return b })
	if ml.Allow() {
		t.Fatalf("含拒绝判据时应不放行")
	}
}

// TestMultiLimiterWithTokenBucket 验证：组合两个 TokenBucket，仅当两者都满时放行。
func TestMultiLimiterWithTokenBucket(t *testing.T) {
	tb1 := NewTokenBucket(1, 2) // 容量 2
	tb2 := NewTokenBucket(1, 1) // 容量 1
	ml := NewMultiLimiter(tb1.Allow, tb2.Allow)
	// 初始：tb1 有 2 令牌，tb2 有 1 令牌 → 都允许，放行一次。
	if !ml.Allow() {
		t.Fatalf("两者都满时应放行")
	}
	// 再放行：tb2 耗尽 → 拒绝。
	if ml.Allow() {
		t.Fatalf("tb2 耗尽后应拒绝")
	}
}

// TestMultiLimiterNilSkipped 验证：nil 判据被忽略，不阻断整体。
func TestMultiLimiterNilSkipped(t *testing.T) {
	ml := NewMultiLimiter(nil, func() bool { return true }, nil)
	if !ml.Allow() {
		t.Fatalf("忽略 nil 后仅余允许判据，应放行")
	}
	var onlyFalse bool = false
	ml2 := NewMultiLimiter(nil, func() bool { return onlyFalse })
	if ml2.Allow() {
		t.Fatalf("含拒绝判据时应拒绝")
	}
}

// TestMultiLimiterEmpty 验证：无判据时 Allow 返回 true（无约束即放行）。
func TestMultiLimiterEmpty(t *testing.T) {
	ml := NewMultiLimiter()
	if !ml.Allow() {
		t.Fatalf("空组合应放行")
	}
}
