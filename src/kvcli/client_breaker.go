package main

import (
	"raftkv/src/util"
	"time"
)

// EnableBreaker 开启客户端熔断：连续失败达 threshold 转 Open（快速失败，不调用下游），
// 冷却 cooldown 后试探一次；试探成功回 Closed。复用 util.CircuitBreaker 状态机。
// 默认关闭（nil），对既有调用方透明。需在发请求前调用一次。
func (c *Client) EnableBreaker(threshold, successThresh int, cooldown time.Duration) {
	c.breaker = util.NewCircuitBreaker(threshold, successThresh, cooldown)
}

// breakerOpen 调用前检查：熔断打开则返回 true（快速失败，避免无谓打下游）。
func (c *Client) breakerOpen() bool {
	return c.breaker != nil && !c.breaker.Allow()
}

// breakerRecord 调用后按成败推进熔断状态机。
func (c *Client) breakerRecord(err error) {
	if c.breaker == nil {
		return
	}
	if err != nil {
		c.breaker.RecordFailure()
	} else {
		c.breaker.RecordSuccess()
	}
}
