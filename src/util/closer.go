package util

import "sync"

// Closer 是「关闭一次 + 等待所有 worker 退出」的组合原语，消除手写
// `close(ch); wg.Wait()` 时常见的两类 bug：① 重复 close 同一 channel 导致 panic；
// ② Add/Done 与 close 的时序竞态（在 goroutine 真正进入 select 前就 close）。
// 典型用法：
//
//	c := NewCloser()
//	for ... { c.Add(1); go func(){ defer c.Done(); <-c.C(); /* 清理 */ }() }
//	... // 收到退出信号
//	c.Close() // 只生效一次；所有 <-c.C() 被唤醒
//	c.Wait()  // 等全部 worker 清理完毕
type Closer struct {
	once sync.Once
	ch   chan struct{}
	wg   sync.WaitGroup
}

// NewCloser 创建未关闭的 Closer。
func NewCloser() *Closer {
	return &Closer{ch: make(chan struct{})}
}

// C 返回关闭信号 channel，worker 用 `select { case <-c.C(): ... }` 监听退出。
func (c *Closer) C() <-chan struct{} {
	return c.ch
}

// Add 登记一个待退出的 worker（须在启动 goroutine 前调用，遵循 WaitGroup 约定）。
func (c *Closer) Add(n int) {
	c.wg.Add(n)
}

// Done 标记一个 worker 已退出（通常 defer 调用）。
func (c *Closer) Done() {
	c.wg.Done()
}

// Close 关闭信号 channel（仅生效一次，重复调用安全），唤醒所有监听者。
func (c *Closer) Close() {
	c.once.Do(func() {
		close(c.ch)
	})
}

// Wait 阻塞直至所有 Done 被调用（所有 worker 退出）。
func (c *Closer) Wait() {
	c.wg.Wait()
}
