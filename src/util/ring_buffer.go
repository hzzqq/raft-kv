package util

import "sync"

// RingBuffer 固定容量的有界环形缓冲：写满后继续写覆盖最旧元素（采样语义），
// 适用于指标/事件的滑动采样、保留最近 N 条记录等场景。并发安全。
// 泛型化以避免装箱与类型断言；T 通常为指标点、日志条目或事件结构。
type RingBuffer[T any] struct {
	mu    sync.Mutex
	buf   []T
	size  int
	pos   int // 下一个写入位置
	count int // 当前元素数（<= size）
}

// NewRingBuffer 创建容量为 size 的环形缓冲；size<=0 时退化为 1。
func NewRingBuffer[T any](size int) *RingBuffer[T] {
	if size <= 0 {
		size = 1
	}
	return &RingBuffer[T]{
		buf:  make([]T, size),
		size: size,
	}
}

// Cap 返回容量。
func (r *RingBuffer[T]) Cap() int {
	return r.size
}

// Len 返回当前元素数。
func (r *RingBuffer[T]) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

// Add 追加一个元素；若已满则覆盖最旧元素。
func (r *RingBuffer[T]) Add(v T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf[r.pos] = v
	r.pos = (r.pos + 1) % r.size
	if r.count < r.size {
		r.count++
	}
}

// Items 返回缓冲中元素，从最旧到最新（不含占位零值以外的空洞）。
func (r *RingBuffer[T]) Items() []T {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]T, 0, r.count)
	if r.count < r.size {
		for i := 0; i < r.count; i++ {
			out = append(out, r.buf[i])
		}
		return out
	}
	// 已满：从 pos（最旧）顺序取到 pos-1（最新）。
	for i := 0; i < r.size; i++ {
		idx := (r.pos + i) % r.size
		out = append(out, r.buf[idx])
	}
	return out
}
