package util

import "sync/atomic"

// Atomic 是类型安全的原子值容器（底层 atomic.Value，编译期约束元素类型 T，
// 运行期保证读写的是同一具体类型）。相比裸 atomic.Value，调用方不再需要做
// 类型断言、也不再会因"先 Store 接口后 Load 成另一个具体类型"而 panic。
// 适用于配置热替换、状态机当前版本号、瞬时快照指针等"读多写少且需无锁"的场景。
type Atomic[T any] struct {
	v atomic.Value
}

// NewAtomic 创建并初始化一个原子值。
func NewAtomic[T any](init T) *Atomic[T] {
	a := &Atomic[T]{}
	a.v.Store(init)
	return a
}

// Load 返回当前值。
func (a *Atomic[T]) Load() T {
	return a.v.Load().(T)
}

// Store 写入新值（覆盖旧值）。
func (a *Atomic[T]) Store(val T) {
	a.v.Store(val)
}

// Swap 替换并返回旧值。
func (a *Atomic[T]) Swap(val T) T {
	return a.v.Swap(val).(T)
}

// CompareAndSwap 当当前值等于 old 时替换为 new，返回是否成功。
func (a *Atomic[T]) CompareAndSwap(old, new T) bool {
	return a.v.CompareAndSwap(old, new)
}
