// sliding_window_limiter.go —— 按 key 的滑动窗口限流
//
// 用于网关按客户端/IP 限制热点路由的并发度：每个 key 在 window 时长内
// 最多放行 max 次，超出被拒。并发安全，可注入时钟便于测试。
package util

import (
	"sync"
	"time"
)

// slidingWindow 是单个 key 的滑动窗口计数器（记录每次放行的时刻）。
type slidingWindow struct {
	window time.Duration
	max    int
	ts     []time.Time
}

// SlidingWindowLimiter 按 key 做滑动窗口限流。零值不可用，须用
// NewSlidingWindowLimiter 构造。
type SlidingWindowLimiter struct {
	window time.Duration
	max    int
	mu     sync.Mutex
	m      map[string]*slidingWindow
	now    func() time.Time
}

// NewSlidingWindowLimiter 构造限流器（window<=0 视为 1s，max<1 视为 1）。
func NewSlidingWindowLimiter(window time.Duration, max int) *SlidingWindowLimiter {
	if window <= 0 {
		window = time.Second
	}
	if max < 1 {
		max = 1
	}
	return &SlidingWindowLimiter{
		window: window,
		max:    max,
		m:      make(map[string]*slidingWindow),
		now:    time.Now,
	}
}

// Allow 判断 key 此刻是否放行（并记一笔）。窗口外旧记录被清扫。
func (l *SlidingWindowLimiter) Allow(key string) bool {
	return l.AllowN(key, 1)
}

// AllowN 判断 key 此刻是否放行 n 次（并记 n 笔）。n<1 视为 1。
func (l *SlidingWindowLimiter) AllowN(key string, n int) bool {
	if n < 1 {
		n = 1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	w, ok := l.m[key]
	if !ok {
		w = &slidingWindow{window: l.window, max: l.max}
		l.m[key] = w
	}
	return w.allow(l.now(), n)
}

// allow 清扫窗口外旧记录后判断是否放行 n 笔。
func (w *slidingWindow) allow(now time.Time, n int) bool {
	cut := now.Add(-w.window)
	kept := make([]time.Time, 0, len(w.ts))
	for _, t := range w.ts {
		if t.After(cut) {
			kept = append(kept, t)
		}
	}
	w.ts = kept
	if len(w.ts)+n > w.max {
		return false
	}
	for i := 0; i < n; i++ {
		w.ts = append(w.ts, now)
	}
	return true
}
