// semaphore.go —— 有界信号量（限制并发资源数）
//
// 用于限制热点路径的并发度（如同时回源连接数、并发迁移数）。
// 支持权重获取与上下文取消；零值不可用，须用 NewSemaphore 构造。
package util

import "context"

// Semaphore 是有界信号量。内部用带缓冲 channel 实现，容量为许可上限。
type Semaphore struct {
	ch chan struct{}
}

// NewSemaphore 构造容量为 n 的信号量（n<1 视为 1）。
func NewSemaphore(n int) *Semaphore {
	if n < 1 {
		n = 1
	}
	return &Semaphore{ch: make(chan struct{}, n)}
}

// Acquire 获取一个许可（权重 1）。ctx 取消时立即返回 ctx.Err()。
func (s *Semaphore) Acquire(ctx context.Context) error {
	return s.AcquireWeighted(ctx, 1)
}

// AcquireWeighted 获取 w 个许可。ctx 取消时回滚已获取的部分并返回 ctx.Err()，
// 不会出现「取到一半」的中间态。
func (s *Semaphore) AcquireWeighted(ctx context.Context, w int) error {
	if w < 1 {
		w = 1
	}
	for i := 0; i < w; i++ {
		select {
		case s.ch <- struct{}{}:
		case <-ctx.Done():
			for j := 0; j < i; j++ { // 回滚已获取的许可
				<-s.ch
			}
			return ctx.Err()
		}
	}
	return nil
}

// Release 释放一个许可（权重 1）。
func (s *Semaphore) Release() { s.ReleaseWeighted(1) }

// ReleaseWeighted 释放 w 个许可，不会超过容量上限（多释放在此处被忽略）。
func (s *Semaphore) ReleaseWeighted(w int) {
	if w < 1 {
		w = 1
	}
	for i := 0; i < w; i++ {
		select {
		case <-s.ch:
		default:
		}
	}
}

// Cap 返回信号量容量。
func (s *Semaphore) Cap() int { return cap(s.ch) }

// InUse 返回当前已获取的许可数（并发快照，仅用于观测，不保证强一致）。
func (s *Semaphore) InUse() int { return len(s.ch) }

// TryAcquire 非阻塞获取一个许可：成功返回 true，已满时立即返回 false（不等待、不阻塞）。
// 用于"满即拒"语义（如网关并发上限超限直接 429）。
func (s *Semaphore) TryAcquire() bool { return s.TryAcquireWeighted(1) }

// TryAcquireWeighted 非阻塞获取 w 个许可：足够则全部获取并返回 true，
// 不足（或并发竞争下刚好满）时立即返回 false 且不会残留部分获取（整体回滚）。
func (s *Semaphore) TryAcquireWeighted(w int) bool {
	if w < 1 {
		w = 1
	}
	// 先探测容量是否足够，避免无谓的部分获取。
	if len(s.ch)+w > cap(s.ch) {
		return false
	}
	for i := 0; i < w; i++ {
		select {
		case s.ch <- struct{}{}:
		default:
			for j := 0; j < i; j++ { // 并发竞争致刚好满，回滚已获取部分
				<-s.ch
			}
			return false
		}
	}
	return true
}
