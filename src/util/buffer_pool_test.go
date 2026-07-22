package util

import (
	"bytes"
	"testing"
)

func TestBufferPool(t *testing.T) {
	bp := NewBufferPool()
	buf := bp.Get()
	if buf.Len() != 0 {
		t.Fatal("Get 返回的 buffer 应为空")
	}
	buf.WriteString("hello")
	if buf.String() != "hello" {
		t.Fatalf("写入内容不符：%q", buf.String())
	}
	bp.Put(buf) // 归还并 Reset
	// 再次 Get 应得到空 buffer（可能复用同一对象）
	buf2 := bp.Get()
	if buf2.Len() != 0 {
		t.Fatalf("复用 buffer 应被 Reset，实际内容 %q", buf2.String())
	}
	buf2.WriteString("world")
	if buf2.String() != "world" {
		t.Fatal("第二次写入失败")
	}
	bp.Put(buf2)
	// Put nil 安全
	bp.Put(nil)
}

// 确保返回的确实是 *bytes.Buffer，可调用其方法。
func TestBufferPoolType(t *testing.T) {
	bp := NewBufferPool()
	var buf *bytes.Buffer = bp.Get()
	buf.WriteByte('x')
	bp.Put(buf)
}
