package util

// MultiLimiter 组合多个限流判据，仅当所有子判据都允许时才放行（AND 语义）。
// 典型用途：同时满足「全局 QPS 限流」与「单租户限流」、「突发令牌桶」与「滑窗计数」等多策略叠加。
// 判据以 func() bool 形式注入，与具体限流器解耦——既支持 TokenBucket.Allow 这类无参判据，
// 也可包一层闭包适配带 key 的限流器（如 func() bool { return swl.Allow(clientIP) }）。
type MultiLimiter struct {
	checks []func() bool
}

// NewMultiLimiter 创建组合限流器；任一判据为 nil 会被忽略（不阻断整体）。
func NewMultiLimiter(checks ...func() bool) *MultiLimiter {
	clean := checks[:0]
	for _, c := range checks {
		if c != nil {
			clean = append(clean, c)
		}
	}
	return &MultiLimiter{checks: clean}
}

// Allow 仅当全部子判据允许时返回 true；短路：任一拒绝立即返回 false。
func (m *MultiLimiter) Allow() bool {
	for _, c := range m.checks {
		if !c() {
			return false
		}
	}
	return true
}
