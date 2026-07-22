package util

import (
	"context"
	"sync/atomic"
)

// BoundedQueue 有界阻塞队列：Push 在满时阻塞、Pop 在空时阻塞，二者均可被 ctx 取消/超时中断，
// 返回 ctx.Err() 而非永久挂起——带背压的生产者/消费者、任务派发等场景的标准原语。
// 底层用 buffered channel 实现，天然 FIFO 且并发安全；closed 后 Push 立即返回错误避免向已关闭 chan 写入 panic。
type BoundedQueue[T any] struct {
	ch     chan T
	closed atomic.Bool
}

// NewBoundedQueue 创建容量为 cap 的有界队列；cap<=0 退化为 1。
func NewBoundedQueue[T any](cap int) *BoundedQueue[T] {
	if cap <= 0 {
		cap = 1
	}
	return &BoundedQueue[T]{ch: make(chan T, cap)}
}

// Push 追加元素；队列满时阻塞直到有空位或 ctx 取消/超时。已被关闭时返回 ErrClosed。
func (q *BoundedQueue[T]) Push(ctx context.Context, v T) error {
	if q.closed.Load() {
		return ErrClosed
	}
	select {
	case q.ch <- v:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Pop 取出队首元素；队列空时阻塞直到有元素或 ctx 取消/超时。
func (q *BoundedQueue[T]) Pop(ctx context.Context) (T, error) {
	select {
	case v, ok := <-q.ch:
		if !ok {
			var zero T
			return zero, ErrClosed
		}
		return v, nil
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// Len 返回当前元素数。
func (q *BoundedQueue[T]) Len() int { return len(q.ch) }

// Cap 返回容量。
func (q *BoundedQueue[T]) Cap() int { return cap(q.ch) }

// Close 关闭队列；之后 Push 返回 ErrClosed，Pop 在排空后返回 ErrClosed（不 panic）。
func (q *BoundedQueue[T]) Close() {
	if q.closed.CompareAndSwap(false, true) {
		close(q.ch)
	}
}

// ErrClosed 表示队列已关闭。
var ErrClosed = &queueError{"queue closed"}

type queueError struct{ msg string }

func (e *queueError) Error() string { return e.msg }
