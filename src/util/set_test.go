package util

import (
	"sort"
	"testing"
)

func TestSet(t *testing.T) {
	s := NewSet(1, 2, 3)
	if s.Len() != 3 {
		t.Fatalf("期望 3 个元素，实际 %d", s.Len())
	}
	if !s.Has(2) || s.Has(9) {
		t.Fatal("Has 判定错误")
	}
	s.Add(2) // 幂等
	if s.Len() != 3 {
		t.Fatal("重复 Add 不应改变长度")
	}
	s.Delete(2)
	if s.Has(2) {
		t.Fatal("Delete 后应不存在")
	}
	// Items 无序但内容正确
	items := s.Items()
	sort.Ints(items)
	if len(items) != 2 || items[0] != 1 || items[1] != 3 {
		t.Fatalf("Items 内容不符：%v", items)
	}
	// Equal
	a := NewSet("x", "y")
	b := NewSet("y", "x")
	if !a.Equal(b) {
		t.Fatal("元素相同应判 Equal")
	}
	c := NewSet("x")
	if a.Equal(c) {
		t.Fatal("元素不同应判不等")
	}
	// Clone 隔离
	cl := a.Clone()
	cl.Add("z")
	if a.Has("z") {
		t.Fatal("Clone 与原集合未隔离")
	}
	// Clear
	s.Clear()
	if s.Len() != 0 {
		t.Fatal("Clear 后应为空")
	}
}
