package util

import "sync/atomic"

// OnceFlag 是一个一次性开关：从「未触发」原子翻转到「已触发」至多一次，并发安全。
// 与 sync.Once 的区别：
//   - 暴露当前状态（Done()），可观测；
//   - Trigger() 返回「本次是否由自己触发成功」，便于调用方据返回值决定后续动作；
//   - 不持有待执行函数，纯状态位，零分配、可被多处共享只读观察。
//
// 典型用途：shutdown 已发起、migration 已开始、配置已提交等「只做一次」的护栏，
// 防止重复执行代价高昂或破坏性的操作（如重复发送迁移、重复关闭监听）。
type OnceFlag struct {
	state int32 // 0=未触发, 1=已触发
}

// Trigger 尝试触发开关。若此前未触发，原子翻转为 true 并返回 true（本次触发成功）；
// 若已触发，立即返回 false（幂等，不阻塞、不 panic）。并发安全。
func (f *OnceFlag) Trigger() bool {
	return atomic.CompareAndSwapInt32(&f.state, 0, 1)
}

// Done 报告开关是否已触发（只读快照，并发安全）。
func (f *OnceFlag) Done() bool {
	return atomic.LoadInt32(&f.state) == 1
}
