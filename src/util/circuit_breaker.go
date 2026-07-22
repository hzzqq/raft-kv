package util

import (
	"sync"
	"time"
)

// CbState 是熔断器状态。
type CbState int

const (
	CBClosed   CbState = iota // 正常放行
	CBOpen                    // 熔断中，快速失败（不调用下游）
	CBHalfOpen                // 冷却后试探放行一次
)

// CircuitBreaker 熔断器：连续失败达阈值转 Open（快速失败，保护下游），
// 冷却超时后转 HalfOpen 放行一次试探；试探成功回 Closed、失败回 Open 重新冷却。
// 用于防止下游雪崩。状态机无外部依赖，cluster-free 可测；now 可注入时钟便于测试。
type CircuitBreaker struct {
	mu            sync.Mutex
	state         CbState
	failures      int
	successes     int
	threshold     int // Closed→Open 的连续失败阈值
	successThresh int // HalfOpen→Closed 所需的连续成功数
	cooldown      time.Duration
	openedAt      time.Time
	now           func() time.Time
}

// NewCircuitBreaker 创建熔断器；threshold/successThresh/cooldown 非正时取安全默认值。
func NewCircuitBreaker(threshold, successThresh int, cooldown time.Duration) *CircuitBreaker {
	if threshold <= 0 {
		threshold = 5
	}
	if successThresh <= 0 {
		successThresh = 1
	}
	if cooldown <= 0 {
		cooldown = time.Second
	}
	return &CircuitBreaker{
		state:         CBClosed,
		threshold:     threshold,
		successThresh: successThresh,
		cooldown:      cooldown,
		now:           time.Now,
	}
}

// Allow 返回当前是否允许放行请求（Open 且冷却未到返回 false）。
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.maybeHalfOpen()
	return cb.state != CBOpen
}

// maybeHalfOpen 内部：Open 且冷却超时则转 HalfOpen（重置试探计数）。调用方须持锁。
func (cb *CircuitBreaker) maybeHalfOpen() {
	if cb.state == CBOpen && cb.now().Sub(cb.openedAt) >= cb.cooldown {
		cb.state = CBHalfOpen
		cb.successes = 0
		cb.failures = 0
	}
}

// RecordSuccess 记录一次成功，推进状态机（HalfOpen 连续成功达阈值→Closed）。
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.maybeHalfOpen()
	switch cb.state {
	case CBClosed:
		cb.failures = 0
	case CBHalfOpen:
		cb.successes++
		if cb.successes >= cb.successThresh {
			cb.state = CBClosed
			cb.failures = 0
			cb.successes = 0
		}
	}
}

// RecordFailure 记录一次失败，推进状态机（Closed 连续失败达阈值→Open；HalfOpen 失败→Open 重冷却）。
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.maybeHalfOpen()
	switch cb.state {
	case CBClosed:
		cb.failures++
		if cb.failures >= cb.threshold {
			cb.state = CBOpen
			cb.openedAt = cb.now()
		}
	case CBHalfOpen:
		cb.state = CBOpen
		cb.openedAt = cb.now()
		cb.successes = 0
	}
}

// State 返回当前状态（会先评估冷却超时，可能触发 HalfOpen）。
func (cb *CircuitBreaker) State() CbState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.maybeHalfOpen()
	return cb.state
}
