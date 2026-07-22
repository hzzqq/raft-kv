package util

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRetryImmediateSuccess 验证：首次成功不重试、无延迟。
func TestRetryImmediateSuccess(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), 3, time.Millisecond, func() (bool, error) {
		calls++
		return true, nil
	})
	if err != nil {
		t.Fatalf("期望成功，实际 %v", err)
	}
	if calls != 1 {
		t.Fatalf("期望仅调用 1 次，实际 %d", calls)
	}
}

// TestRetryEventuallySucceeds 验证：前两次失败后第三次成功，共 3 次调用。
func TestRetryEventuallySucceeds(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), 5, time.Millisecond, func() (bool, error) {
		calls++
		if calls < 3 {
			return true, errors.New("transient")
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("期望最终成功，实际 %v", err)
	}
	if calls != 3 {
		t.Fatalf("期望调用 3 次，实际 %d", calls)
	}
}

// TestRetryNonRetryable 验证：不可重试错误立即返回，不重试。
func TestRetryNonRetryable(t *testing.T) {
	calls := 0
	myErr := errors.New("bad param")
	err := Retry(context.Background(), 5, time.Millisecond, func() (bool, error) {
		calls++
		return false, myErr
	})
	if !errors.Is(err, myErr) {
		t.Fatalf("期望返回不可重试错误，实际 %v", err)
	}
	if calls != 1 {
		t.Fatalf("不可重试错误应仅调用 1 次，实际 %d", calls)
	}
}

// TestRetryExhausted 验证：始终失败且可重试，耗尽 maxAttempts 后返回最后错误。
func TestRetryExhausted(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), 3, time.Millisecond, func() (bool, error) {
		calls++
		return true, errors.New("always fail")
	})
	if err == nil {
		t.Fatalf("期望返回错误")
	}
	if calls != 3 {
		t.Fatalf("期望调用 3 次（maxAttempts），实际 %d", calls)
	}
}

// TestRetryCtxCancel 验证：ctx 取消后停止重试并返回 ctx.Err。
func TestRetryCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := Retry(ctx, 10, 50*time.Millisecond, func() (bool, error) {
		calls++
		if calls == 1 {
			cancel() // 第一次成功后取消，第二次尝试前 ctx 已结束
		}
		return true, errors.New("fail")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("期望 context.Canceled，实际 %v", err)
	}
	if calls != 1 {
		t.Fatalf("ctx 取消后应仅调用 1 次，实际 %d", calls)
	}
}
