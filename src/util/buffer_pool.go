package util

import (
	"bytes"
	"sync"
)

// BufferPool 是 *bytes.Buffer 的对象池：缓解高频 JSON/IO 序列化场景下的
// 短生命周期 buffer 分配压力。Get 返回的 buffer 已 Reset（空内容），Put 会自动
// Reset 再归还，避免把上一次写入的脏数据泄漏到下一次使用方。
type BufferPool struct {
	p sync.Pool
}

// NewBufferPool 创建 buffer 池。
func NewBufferPool() *BufferPool {
	return &BufferPool{p: sync.Pool{New: func() interface{} { return &bytes.Buffer{} }}}
}

// Get 取一个已清空的 buffer。
func (b *BufferPool) Get() *bytes.Buffer {
	return b.p.Get().(*bytes.Buffer)
}

// Put 归还 buffer（先 Reset 再入池，杜绝脏数据复用）。
func (b *BufferPool) Put(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	buf.Reset()
	b.p.Put(buf)
}
