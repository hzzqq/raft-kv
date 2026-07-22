// backoff_test.go —— 验证 util.Backoff（#78），纯函数白盒测试。
package util

import (
	"testing"
	"time"
)

func TestBackoffDeterministic(t *testing.T) {
	base := 10 * time.Millisecond
	max := 500 * time.Millisecond
	// jitter=0 时完全确定：base, 2*base, 4*base, ... 封顶于 max。
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, base},
		{2, 20 * time.Millisecond},
		{3, 40 * time.Millisecond},
		{4, 80 * time.Millisecond},
		{5, 160 * time.Millisecond},
		{6, 320 * time.Millisecond},
		{7, 500 * time.Millisecond}, // 640ms 超 max -> 封顶
		{99, 500 * time.Millisecond},
	}
	for _, c := range cases {
		got := Backoff(base, max, c.attempt, 0)
		if got != c.want {
			t.Fatalf("Backoff(attempt=%d, jitter=0)=%v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestBackoffMonotonicAndCapped(t *testing.T) {
	base := 5 * time.Millisecond
	max := 100 * time.Millisecond
	prev := time.Duration(-1)
	for a := 1; a <= 12; a++ {
		got := Backoff(base, max, a, 0)
		if got < prev {
			t.Fatalf("Backoff not monotonic at attempt %d: %v < %v", a, got, prev)
		}
		if got > max {
			t.Fatalf("Backoff exceeded max at attempt %d: %v > %v", a, got, max)
		}
		prev = got
	}
	// 封顶后保持 == max（不继续增长）
	if got := Backoff(base, max, 50, 0); got != max {
		t.Fatalf("Backoff should cap at max, got %v", got)
	}
}

func TestBackoffAttemptGuard(t *testing.T) {
	base := 10 * time.Millisecond
	max := 1 * time.Second
	// attempt<=0 视作第 1 次
	if got := Backoff(base, max, 0, 0); got != base {
		t.Fatalf("Backoff(attempt=0) = %v, want base %v", got, base)
	}
	if got := Backoff(base, max, -3, 0); got != base {
		t.Fatalf("Backoff(attempt<0) = %v, want base %v", got, base)
	}
}

func TestBackoffJitterBounded(t *testing.T) {
	base := 100 * time.Millisecond
	max := 10 * time.Second
	// 带抖动时，单次结果仍应落在 [base*(1-j), base*(1+j)] 附近且 <= max。
	// 多跑几次，确保从不超过 max 且不小于 0。
	for i := 0; i < 200; i++ {
		got := Backoff(base, max, 3, 0.3)
		if got < 0 {
			t.Fatalf("jitter produced negative backoff: %v", got)
		}
		if got > max {
			t.Fatalf("jitter exceeded max: %v", got)
		}
	}
}
