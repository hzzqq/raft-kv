package util

import (
	"sync"
	"time"
)

// TokenBucket 令牌桶限流器：以固定速率 refill 令牌，桶容量 cap 决定允许的最大突发。
// 与 SlidingWindowLimiter 互补——滑窗限制「时间窗内总请求数」，令牌桶限制「瞬时突发 + 平均速率」，
// 更适合允许短时突发、但需要平滑长期速率的场景（如后端回源、对外 API 调用）。
type TokenBucket struct {
	mu       sync.Mutex
	capacity float64
	tokens   float64
	rate     float64 // 令牌/秒
	last     time.Time
	now      func() time.Time // 可注入时钟，便于白盒测试
}

// NewTokenBucket 创建令牌桶：rate 为每秒补充的令牌数，capacity 为桶容量（同时是初始令牌数）。
func NewTokenBucket(rate, capacity float64) *TokenBucket {
	return &TokenBucket{
		capacity: capacity,
		tokens:   capacity,
		rate:     rate,
		last:     time.Now(),
		now:      time.Now,
	}
}

// refill 根据流逝时间补充令牌（不超过 capacity）。调用方须持锁。
func (tb *TokenBucket) refill() {
	now := tb.now()
	elapsed := now.Sub(tb.last).Seconds()
	if elapsed <= 0 {
		return
	}
	tb.tokens += elapsed * tb.rate
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
	tb.last = now
}

// Allow 尝试取 1 个令牌，成功返回 true（非阻塞、不等待）。
func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(1)
}

// AllowN 尝试取 n 个令牌，成功返回 true。n 可超过 capacity（需积累足够时间），
// 不足时返回 false 且不消耗令牌。
func (tb *TokenBucket) AllowN(n float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()
	if tb.tokens >= n {
		tb.tokens -= n
		return true
	}
	return false
}

// Available 返回当前可用令牌数（仅观测用）。
func (tb *TokenBucket) Available() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()
	return tb.tokens
}
