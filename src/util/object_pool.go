package util

import "sync"

// ObjectPool 泛型对象池：复用临时对象以降低 GC 压力，New 构造、Reset 在归还前清理。
// 在 sync.Pool 之上补上类型安全（免去 any 断言）与可选 Reset 钩子，避免调用方散落类型转换。
// 典型用途：高频分配的缓冲区（*bytes.Buffer）、临时结构体、序列化中间对象。
type ObjectPool[T any] struct {
	pool  sync.Pool
	reset func(T)
}

// NewObjectPool 创建对象池；newFn 在池空时构造新对象，reset 在 Put 归还前清理（可为 nil）。
func NewObjectPool[T any](newFn func() T, reset func(T)) *ObjectPool[T] {
	return &ObjectPool[T]{
		pool: sync.Pool{
			New: func() interface{} { return newFn() },
		},
		reset: reset,
	}
}

// Get 取出一个对象：优先复用池中对象，否则调用 newFn 构造。
func (p *ObjectPool[T]) Get() T {
	return p.pool.Get().(T)
}

// Put 归还对象；若提供了 reset 钩子则先清理再放回，避免脏状态被后续 Get 复用污染。
func (p *ObjectPool[T]) Put(v T) {
	if p.reset != nil {
		p.reset(v)
	}
	p.pool.Put(v)
}
