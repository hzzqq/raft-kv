package main

import (
	"sync/atomic"
	"time"
)

// ClientMetrics 汇总客户端级可观测指标（原子计数，并发安全）。
type ClientMetrics struct {
	Requests  int64 // 总请求尝试数（含重试后的每次尝试）
	Errors    int64 // 失败请求数
	LatencyNs int64 // 累计请求耗时（纳秒），AvgLatency 据此计算均值
}

// Metrics 返回当前客户端指标快照。
func (c *Client) Metrics() ClientMetrics {
	return ClientMetrics{
		Requests:  atomic.LoadInt64(&c.mReq),
		Errors:    atomic.LoadInt64(&c.mErr),
		LatencyNs: atomic.LoadInt64(&c.mLatNs),
	}
}

// AvgLatency 返回平均请求延迟；无请求时返回 0。
func (m ClientMetrics) AvgLatency() time.Duration {
	if m.Requests == 0 {
		return 0
	}
	return time.Duration(m.LatencyNs / m.Requests)
}

// recordCall 记录一次请求结果（原子自增，并发安全）。
func (c *Client) recordCall(start time.Time, err error) {
	atomic.AddInt64(&c.mReq, 1)
	if err != nil {
		atomic.AddInt64(&c.mErr, 1)
	}
	atomic.AddInt64(&c.mLatNs, int64(time.Since(start)))
}
