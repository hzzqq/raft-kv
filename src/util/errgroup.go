// errgroup.go —— 轻量并发任务组（出错即取消全组）
//
// 用于「部分失败即整体失败」的场景（如并行拉多分片、批量健康探测）：
// 多个任务并行，任一返回 error 即取消其余并通过共享 ctx 通知它们退出。
package util

import (
	"context"
	"sync"
)

// ErrGroup 是并发任务组。基于父 ctx 创建，父 ctx 取消也会取消全组。
type ErrGroup struct {
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	mu       sync.Mutex
	firstErr error
}

// WithErrGroup 基于父 ctx 创建任务组。
func WithErrGroup(ctx context.Context) *ErrGroup {
	ctx, cancel := context.WithCancel(ctx)
	return &ErrGroup{ctx: ctx, cancel: cancel}
}

// Go 启动一个任务；fn 接收可被取消的组内 ctx。首个非 nil 错误被记录，
// 并触发全组取消（其余任务应监听 ctx.Done() 及时退出）。
func (g *ErrGroup) Go(fn func(ctx context.Context) error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if err := fn(g.ctx); err != nil {
			g.mu.Lock()
			if g.firstErr == nil {
				g.firstErr = err
			}
			g.mu.Unlock()
			g.cancel() // 通知其余任务取消
		}
	}()
}

// Wait 等待全部任务结束，返回首个（若有）错误。
func (g *ErrGroup) Wait() error {
	g.wg.Wait()
	g.cancel()
	return g.firstErr
}

// Ctx 返回组内共享 ctx，供任务内部传递与被动取消。
func (g *ErrGroup) Ctx() context.Context { return g.ctx }
