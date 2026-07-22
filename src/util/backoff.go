// backoff.go —— 指数退避计算器（零依赖纯函数），供 kvcli / demo 等重试场景复用。
//
// 设计要点：
//   - 退避 = base * 2^(attempt-1)，attempt 从 1（第 1 次重试）起算；
//   - 上限封顶 max，避免长链条累积到不可接受的大延迟；
//   - 可选抖动 jitter ∈ [0,1)：在 [1-jitter, 1+jitter] 区间随机扰动，打散重试风暴
//     （防止大量客户端在同一时刻同步重试造成的「惊群」）。
// 纯函数、无状态、并发安全（不读写任何共享变量）。
package util

import (
	"math"
	"math/rand"
	"time"
)

// Backoff 计算第 attempt 次重试（attempt>=1）的退避时长。
// base 初始间隔，max 上限，jitter 抖动比例（<=0 表示不抖）。
func Backoff(base, max time.Duration, attempt int, jitter float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(base) * math.Pow(2, float64(attempt-1))
	if d > float64(max) {
		d = float64(max)
	}
	if jitter > 0 {
		f := 1 + (rand.Float64()*2-1)*jitter
		if f < 0 {
			f = 0
		}
		d *= f
	}
	if d < float64(base) {
		d = float64(base)
	}
	return time.Duration(d)
}

// ExpBackoff 是带默认抖动（0.2）的便捷封装。
func ExpBackoff(base, max time.Duration, attempt int) time.Duration {
	return Backoff(base, max, attempt, 0.2)
}
