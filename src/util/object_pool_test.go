package util

import (
	"bytes"
	"testing"
)

// TestObjectPoolBasic 验证：池空时 Get 走 newFn 构造，Put 后再 Get 复用（同 goroutine 私有槽），且 reset 清空脏状态。
func TestObjectPoolBasic(t *testing.T) {
	pool := NewObjectPool(func() *bytes.Buffer { return &bytes.Buffer{} }, func(b *bytes.Buffer) { b.Reset() })

	b1 := pool.Get()
	if b1 == nil {
		t.Fatalf("Get 应返回非 nil *bytes.Buffer")
	}
	b1.WriteString("hello")

	pool.Put(b1) // 归还前 reset 清空

	b2 := pool.Get() // 复用 b1，已被 reset
	if b2 == nil {
		t.Fatalf("二次 Get 应返回非 nil")
	}
	if b2.Len() != 0 {
		t.Fatalf("reset 应清空内容，实际 len=%d (%q)", b2.Len(), b2.String())
	}
}

// TestObjectPoolNewFn 验证：无对象可复用时走 newFn（计数确认）。
func TestObjectPoolNewFn(t *testing.T) {
	var constructed int
	pool := NewObjectPool(func() *bytes.Buffer {
		constructed++
		return &bytes.Buffer{}
	}, nil)

	// 连续 Get 两个（无归还），应各自 newFn 一次。
	a := pool.Get()
	b := pool.Get()
	if constructed != 2 {
		t.Fatalf("期望构造 2 次，实际 %d", constructed)
	}
	_ = a
	_ = b
	// 归还一个后 Get，应复用（构造次数不变）。
	pool.Put(a)
	pool.Get()
	if constructed != 2 {
		t.Fatalf("复用后构造次数应仍=2，实际 %d", constructed)
	}
}

// TestObjectPoolNilReset 验证：reset 为 nil 时不 panic，Put/Get 正常。
func TestObjectPoolNilReset(t *testing.T) {
	pool := NewObjectPool(func() *bytes.Buffer { return &bytes.Buffer{} }, nil)
	b := pool.Get()
	b.WriteString("dirty") // 不清理
	pool.Put(b)
	b2 := pool.Get()
	if b2 == nil {
		t.Fatalf("Get 应返回非 nil")
	}
	// 无 reset，内容可能残留（仅验证不 panic 且类型正确）。
	_ = b2
}
