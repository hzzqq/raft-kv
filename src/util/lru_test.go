// lru_test.go —— 验证 util.LRU（#81），纯结构白盒测试。
package util

import "testing"

func TestLRUBasic(t *testing.T) {
	c := NewLRU(2)
	if c.Len() != 0 {
		t.Fatalf("new LRU len = %d, want 0", c.Len())
	}
	if _, ok := c.Get("a"); ok {
		t.Fatal("Get on empty should miss")
	}
	c.Put("a", 1)
	c.Put("b", 2)
	if c.Len() != 2 {
		t.Fatalf("len = %d, want 2", c.Len())
	}
	if v, ok := c.Get("a"); !ok || v.(int) != 1 {
		t.Fatalf("Get(a) = %v,%v want 1,true", v, ok)
	}
}

func TestLRUEviction(t *testing.T) {
	c := NewLRU(2)
	c.Put("a", 1)
	c.Put("b", 2)
	// 访问 a 使其成为最近使用，b 成为最久未用。
	c.Get("a")
	// 插入 c：应淘汰 b（最久未用）。
	c.Put("c", 3)
	if c.Len() != 2 {
		t.Fatalf("len after eviction = %d, want 2", c.Len())
	}
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted (LRU)")
	}
	if v, ok := c.Get("a"); !ok || v.(int) != 1 {
		t.Fatalf("a should survive, got %v,%v", v, ok)
	}
	if v, ok := c.Get("c"); !ok || v.(int) != 3 {
		t.Fatalf("c should exist, got %v,%v", v, ok)
	}
}

func TestLRUUpdate(t *testing.T) {
	c := NewLRU(2)
	c.Put("a", 1)
	c.Put("a", 100) // 同 key 更新，不应增加长度
	if c.Len() != 1 {
		t.Fatalf("len after update = %d, want 1", c.Len())
	}
	if v, _ := c.Get("a"); v.(int) != 100 {
		t.Fatalf("updated value = %v, want 100", v)
	}
}

func TestLRUDelete(t *testing.T) {
	c := NewLRU(3)
	c.Put("a", 1)
	c.Put("b", 2)
	c.Delete("a")
	if c.Len() != 1 {
		t.Fatalf("len after delete = %d, want 1", c.Len())
	}
	if _, ok := c.Get("a"); ok {
		t.Fatal("a should be deleted")
	}
	c.Delete("missing") // 无操作，不应 panic
	if c.Len() != 1 {
		t.Fatalf("len after delete-missing = %d, want 1", c.Len())
	}
}

func TestLRUGetMovesToFront(t *testing.T) {
	c := NewLRU(2)
	c.Put("a", 1)
	c.Put("b", 2)
	// Get(a) 后 a 最近，b 最久。再 Put(c) 应淘汰 b。
	c.Get("a")
	c.Put("c", 3)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should be evicted after Get(a) then Put(c)")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should survive (was most recent)")
	}
}
