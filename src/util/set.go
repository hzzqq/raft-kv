package util

// Set 是泛型集合（基于 map 实现），元素必须可比较（any comparable）。
// 提供并发不安全但简单的 Add/Has/Delete/Len/Items/Clear/Equal，用于去重、
// 成员判定、集合运算（差集/交集）等场景，替代手写 map[T]struct{} 样板。
type Set[T comparable] struct {
	m map[T]struct{}
}

// NewSet 创建空集；可选传入初始元素。
func NewSet[T comparable](items ...T) *Set[T] {
	s := &Set[T]{m: make(map[T]struct{}, len(items))}
	for _, it := range items {
		s.m[it] = struct{}{}
	}
	return s
}

// Add 加入元素（已存在则幂等）。
func (s *Set[T]) Add(v T) {
	s.m[v] = struct{}{}
}

// Has 判定元素是否在集合中。
func (s *Set[T]) Has(v T) bool {
	_, ok := s.m[v]
	return ok
}

// Delete 删除元素（不存在则幂等）。
func (s *Set[T]) Delete(v T) {
	delete(s.m, v)
}

// Len 返回元素个数。
func (s *Set[T]) Len() int {
	return len(s.m)
}

// Items 返回所有元素（无序切片，每次调用重新生成）。
func (s *Set[T]) Items() []T {
	out := make([]T, 0, len(s.m))
	for v := range s.m {
		out = append(out, v)
	}
	return out
}

// Clear 清空集合。
func (s *Set[T]) Clear() {
	s.m = make(map[T]struct{})
}

// Clone 返回集合的浅拷贝（元素为值/指针时按 Go 语义共享底层对象）。
func (s *Set[T]) Clone() *Set[T] {
	n := NewSet[T]()
	for v := range s.m {
		n.m[v] = struct{}{}
	}
	return n
}

// Equal 判断两集合是否含完全相同的一组元素（忽略顺序）。
func (s *Set[T]) Equal(o *Set[T]) bool {
	if s.Len() != o.Len() {
		return false
	}
	for v := range s.m {
		if !o.Has(v) {
			return false
		}
	}
	return true
}
