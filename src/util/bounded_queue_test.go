package util

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestBoundedQueueFIFO 验证：正常 Push/Pop 保序、Len/Cap 正确。
func TestBoundedQueueFIFO(t *testing.T) {
	q := NewBoundedQueue[int](3)
	if q.Cap() != 3 || q.Len() != 0 {
		t.Fatalf("期望 cap=3 len=0，实际 cap=%d len=%d", q.Cap(), q.Len())
	}
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		if err := q.Push(ctx, i); err != nil {
			t.Fatalf("Push %d 失败: %v", i, err)
		}
	}
	if q.Len() != 3 {
		t.Fatalf("期望 len=3，实际 %d", q.Len())
	}
	for i := 1; i <= 3; i++ {
		v, err := q.Pop(ctx)
		if err != nil {
			t.Fatalf("Pop 失败: %v", err)
		}
		if v != i {
			t.Fatalf("期望 FIFO 出 %d，实际 %d", i, v)
		}
	}
}

// TestBoundedQueuePushBlocked 验证：满队列 Push 阻塞，直至 ctx 超时返回错误。
func TestBoundedQueuePushBlocked(t *testing.T) {
	q := NewBoundedQueue[int](1)
	q.Push(context.Background(), 1) // 填满
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := q.Push(ctx, 2)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("期望超时错误，实际 %v", err)
	}
}

// TestBoundedQueuePopBlocked 验证：空队列 Pop 阻塞，直至 ctx 超时返回错误。
func TestBoundedQueuePopBlocked(t *testing.T) {
	q := NewBoundedQueue[int](2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := q.Pop(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("期望超时错误，实际 %v", err)
	}
}

// TestBoundedQueueClosed 验证：关闭后 Push/Pop 返回 ErrClosed。
func TestBoundedQueueClosed(t *testing.T) {
	q := NewBoundedQueue[int](2)
	q.Close()
	if err := q.Push(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Push 期望 ErrClosed，实际 %v", err)
	}
	_, err := q.Pop(context.Background())
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Pop 期望 ErrClosed，实际 %v", err)
	}
}

// TestBoundedQueueCloseDrains 验证：关闭后已入队元素仍可被 Pop 取出，之后才返回 ErrClosed。
func TestBoundedQueueCloseDrains(t *testing.T) {
	q := NewBoundedQueue[int](4)
	q.Push(context.Background(), 10)
	q.Push(context.Background(), 20)
	q.Close()
	v, err := q.Pop(context.Background())
	if err != nil || v != 10 {
		t.Fatalf("期望先 Pop 10，实际 %d/%v", v, err)
	}
	v, err = q.Pop(context.Background())
	if err != nil || v != 20 {
		t.Fatalf("期望再 Pop 20，实际 %d/%v", v, err)
	}
	_, err = q.Pop(context.Background())
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("排空后 Pop 期望 ErrClosed，实际 %v", err)
	}
}
