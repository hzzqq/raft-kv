package util

import (
	"context"
	"errors"
	"sync"
)

// ErrPoolStopped 表示向已停止的 WorkerPool 提交任务。
var ErrPoolStopped = errors.New("worker pool already stopped")

// ErrPoolFull 表示任务缓冲区已满且采用非阻塞提交（TrySubmit）。
var ErrPoolFull = errors.New("worker pool task buffer full")

// WorkerPool 是固定 N 个常驻 worker 的任务池：任务经有界 channel 派发，
// worker 阻塞消费直至池被关闭。相比「每次任务起一个 goroutine」，它限制并发度、
// 复用 goroutine、并天然提供「停止后等所有在途任务跑完」的语义，适用于
// 批量回源、迁移搬运、指标采集等「并发有上限 + 优雅退出」场景。
//
// 停止语义：StopAndWait 只关闭 stopCh 而非 tasks 通道。worker 收到 stopCh 后
// 先排空缓冲区中的在途任务再退出。这样 Submit/TrySubmit/SubmitCtx 永远不需要
// 向「已关闭的通道」发送（否则会 panic），且无需在发送期间长期持锁，避免
// 「缓冲满 + 停止」并发下的死锁。
type WorkerPool struct {
	mu      sync.Mutex
	tasks   chan func()
	stopCh  chan struct{}
	stopped bool
	wg      sync.WaitGroup
}

// NewWorkerPool 创建并启动 n 个 worker（n<1 时按 1 处理）。
func NewWorkerPool(n int) *WorkerPool {
	if n < 1 {
		n = 1
	}
	p := &WorkerPool{
		tasks:  make(chan func(), 1024),
		stopCh: make(chan struct{}),
	}
	p.wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer p.wg.Done()
			for {
				select {
				case fn, ok := <-p.tasks:
					if !ok {
						return
					}
					fn()
				case <-p.stopCh:
					// 优雅退出前先排空缓冲区的在途任务，保证「已提交必执行」。
					for {
						select {
						case fn, ok := <-p.tasks:
							if !ok {
								return
							}
							fn()
						default:
							return
						}
					}
				}
			}
		}()
	}
	return p
}

// Submit 阻塞提交一个任务（缓冲区有空位即入队，满则等待空闲）。
// 池已停止时返回 ErrPoolStopped（任务不被执行）。发送前已释放锁，故不会与
// StopAndWait 互相死锁。
func (p *WorkerPool) Submit(fn func()) error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return ErrPoolStopped
	}
	p.mu.Unlock()
	p.tasks <- fn
	return nil
}

// TrySubmit 非阻塞提交：缓冲区有空位立即入队返回 nil；缓冲区满返回 ErrPoolFull；
// 池已停止返回 ErrPoolStopped。调用方据此决定是否退避 / 直接执行 / 丢弃，
// 避免「缓冲满」时无限阻塞或退化为每任务起一个 goroutine 的内存爆炸。
func (p *WorkerPool) TrySubmit(fn func()) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return ErrPoolStopped
	}
	select {
	case p.tasks <- fn:
		return nil
	default:
		return ErrPoolFull
	}
}

// SubmitCtx 在 ctx 未取消且池未停止时提交；否则立即返回对应错误而不阻塞：
//   - ctx 已取消 / 超时 → ctx.Err()
//   - 池已停止         → ErrPoolStopped
//   - 缓冲满且 ctx 持续有效 → 阻塞至有空位（有界并发 + 背压），ctx 取消即退出
//
// tasks 通道永不关闭，故并发提交与停止不会因向已关闭通道发送而 panic。
func (p *WorkerPool) SubmitCtx(ctx context.Context, fn func()) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return ErrPoolStopped
	}
	p.mu.Unlock()
	select {
	case p.tasks <- fn:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.stopCh:
		return ErrPoolStopped
	}
}

// StopAndWait 关闭任务通道并等待所有在途任务执行完毕。重复调用安全（仅首次生效）。
func (p *WorkerPool) StopAndWait() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	close(p.stopCh)
	p.mu.Unlock()
	p.wg.Wait()
}
