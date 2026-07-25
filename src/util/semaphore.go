// semaphore.go —— 有界信号量（限制并发资源数）
//
// 用于限制热点路径的并发度（如同时回源连接数、并发迁移数）。
// 支持权重获取与上下文取消；零值不可用，须用 NewSemaphore 构造。
package util

import (
	"context"
	"sync/atomic"
)

// Semaphore 是有界信号量。内部用带缓冲 channel 作许可令牌桶（令牌在桶中=已获取），
// 用原子计数 inuse 维护「当前已获取数」的并发安全快照，避免读取 channel 的
// len/cap（Go 中 len(ch)/cap(ch) 不持有 channel 锁，与并发 send/recv 存在数据竞争）。
type Semaphore struct {
	ch    chan struct{}
	n     int
	inuse int64 // atomic：当前已获取许可数，仅用于观测，避免读取 channel len 引发数据竞争
}

// NewSemaphore 构造容量为 n 的信号量（n<1 视为 1）。
func NewSemaphore(n int) *Semaphore {
	if n < 1 {
		n = 1
	}
	return &Semaphore{ch: make(chan struct{}, n), n: n}
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
			atomic.AddInt64(&s.inuse, 1)
		case <-ctx.Done():
			for j := 0; j < i; j++ { // 回滚已获取的许可
				<-s.ch
				atomic.AddInt64(&s.inuse, -1)
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
			atomic.AddInt64(&s.inuse, -1)
		default:
		}
	}
}

// Cap 返回信号量容量。
func (s *Semaphore) Cap() int { return s.n }

// InUse 返回当前已获取的许可数（并发安全快照，仅用于观测，不保证强一致）。
// 改用原子计数而非 len(ch)，彻底消除「观测读取」与「许可变更」之间的数据竞争。
func (s *Semaphore) InUse() int { return int(atomic.LoadInt64(&s.inuse)) }

// TryAcquire 非阻塞获取一个许可：成功返回 true，已满时立即返回 false（不等待、不阻塞）。
// 用于"满即拒"语义（如网关并发上限超限直接 429）。
func (s *Semaphore) TryAcquire() bool { return s.TryAcquireWeighted(1) }

// TryAcquireWeighted 非阻塞获取 w 个许可：足够则全部获取并返回 true，
// 不足（或并发竞争下刚好满）时立即返回 false 且不会残留部分获取（整体回滚）。
// 仅依赖 select 的非阻塞 send 探测满桶，不读取 channel 的 len/cap，无数据竞争。
func (s *Semaphore) TryAcquireWeighted(w int) bool {
	if w < 1 {
		w = 1
	}
	for i := 0; i < w; i++ {
		select {
		case s.ch <- struct{}{}:
			atomic.AddInt64(&s.inuse, 1)
		default:
			for j := 0; j < i; j++ { // 并发竞争致刚好满，回滚已获取部分
				<-s.ch
				atomic.AddInt64(&s.inuse, -1)
			}
			return false
		}
	}
	return true
}
