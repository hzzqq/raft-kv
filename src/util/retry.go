package util

import (
	"context"
	"time"
)

// Retry 以指数退避（复用 ExpBackoff，上限 2s）重试 do，直到成功、ctx 取消/超时、
// 达到 maxAttempts，或遇到不可重试错误（do 返回 retryable=false 时立即返回该错误）。
//
// do 的签名 (retryable bool, err error)：err==nil 视为成功；err!=nil 且 retryable=true 表示
// 可重试（按退避等待后再次尝试）；retryable=false 表示不可重试（如参数错误），立即终止。
// 全程尊重 ctx：每次尝试前及退避等待期间一旦 ctx 结束即返回 ctx.Err()。
func Retry(ctx context.Context, maxAttempts int, base time.Duration, do func() (bool, error)) error {
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return lastErr
			}
			return err
		}
		retryable, err := do()
		if err == nil {
			return nil
		}
		lastErr = err
		if !retryable {
			return err
		}
		if attempt < maxAttempts-1 {
			wait := ExpBackoff(base, 2*time.Second, attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}
	}
	return lastErr
}
