package util

import (
	"sync"
	"testing"
)

// TestRingBufferBasic 验证：未满时按写入顺序保留，Len 正确。
func TestRingBufferBasic(t *testing.T) {
	rb := NewRingBuffer[int](3)
	if rb.Cap() != 3 || rb.Len() != 0 {
		t.Fatalf("期望 cap=3 len=0，实际 cap=%d len=%d", rb.Cap(), rb.Len())
	}
	rb.Add(1)
	rb.Add(2)
	rb.Add(3)
	if rb.Len() != 3 {
		t.Fatalf("期望 len=3，实际 %d", rb.Len())
	}
	items := rb.Items()
	if len(items) != 3 || items[0] != 1 || items[1] != 2 || items[2] != 3 {
		t.Fatalf("期望 [1 2 3]，实际 %v", items)
	}
}

// TestRingBufferOverwrite 验证：写满后覆盖最旧元素，保持从旧到新顺序。
func TestRingBufferOverwrite(t *testing.T) {
	rb := NewRingBuffer[int](3)
	for i := 1; i <= 5; i++ {
		rb.Add(i)
	}
	if rb.Len() != 3 {
		t.Fatalf("满后 len 应为 3，实际 %d", rb.Len())
	}
	items := rb.Items()
	// 最旧 1、2 被覆盖，剩 [3 4 5]
	if len(items) != 3 || items[0] != 3 || items[1] != 4 || items[2] != 5 {
		t.Fatalf("期望 [3 4 5]，实际 %v", items)
	}
}

// TestRingBufferEmpty 验证：空缓冲 Items 返回空切片（非 nil）且 Len=0。
func TestRingBufferEmpty(t *testing.T) {
	rb := NewRingBuffer[int](2)
	items := rb.Items()
	if len(items) != 0 {
		t.Fatalf("空缓冲期望空切片，实际 %v", items)
	}
}

// TestRingBufferNonZero 验证：非整数类型（字符串）同样正确采样。
func TestRingBufferNonZero(t *testing.T) {
	rb := NewRingBuffer[string](2)
	rb.Add("a")
	rb.Add("b")
	rb.Add("c") // 覆盖 "a"
	items := rb.Items()
	if len(items) != 2 || items[0] != "b" || items[1] != "c" {
		t.Fatalf("期望 [b c]，实际 %v", items)
	}
}

// TestRingBufferConcurrent 验证：并发 Add 不破坏结构（无 panic/竞态）。
func TestRingBufferConcurrent(t *testing.T) {
	rb := NewRingBuffer[int](16)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			rb.Add(v)
		}(i)
	}
	wg.Wait()
	if rb.Len() > 16 {
		t.Fatalf("并发后 len 不应超过容量 16，实际 %d", rb.Len())
	}
	// Items 长度应等于 min(100, 16)=16。
	if len(rb.Items()) != 16 {
		t.Fatalf("Items 长度应为 16，实际 %d", len(rb.Items()))
	}
}
