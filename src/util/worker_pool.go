package util

import (
	"errors"
	"sync"
)

// ErrPoolStopped 表示向已停止的 WorkerPool 提交任务。
var ErrPoolStopped = errors.New("worker pool already stopped")

// WorkerPool 是固定 N 个常驻 worker 的任务池：任务经有界 channel 派发，
// worker 阻塞消费直至池被关闭。相比「每次任务起一个 goroutine」，它限制并发度、
// 复用 goroutine、并天然提供「停止后等所有在途任务跑完」的语义，适用于
// 批量回源、迁移搬运、指标采集等「并发有上限 + 优雅退出」场景。
type WorkerPool struct {
	mu      sync.Mutex
	tasks   chan func()
	stopped bool
	wg      sync.WaitGroup
}

// NewWorkerPool 创建并启动 n 个 worker（n<1 时按 1 处理）。
func NewWorkerPool(n int) *WorkerPool {
	if n < 1 {
		n = 1
	}
	p := &WorkerPool{tasks: make(chan func(), 1024)}
	p.wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer p.wg.Done()
			for fn := range p.tasks {
				fn()
			}
		}()
	}
	return p
}

// Submit 提交一个任务；池已停止时返回 ErrPoolStopped（任务不被执行）。
func (p *WorkerPool) Submit(fn func()) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return ErrPoolStopped
	}
	p.tasks <- fn
	return nil
}

// StopAndWait 关闭任务通道并等待所有在途任务执行完毕。重复调用安全（仅首次生效）。
func (p *WorkerPool) StopAndWait() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	close(p.tasks)
	p.mu.Unlock()
	p.wg.Wait()
}
