package util

import (
	"sync/atomic"
	"testing"
)

func TestWorkerPool(t *testing.T) {
	p := NewWorkerPool(4)
	var done int32
	const n = 100
	for i := 0; i < n; i++ {
		if err := p.Submit(func() { atomic.AddInt32(&done, 1) }); err != nil {
			t.Fatalf("Submit 不应报错：%v", err)
		}
	}
	p.StopAndWait()
	if atomic.LoadInt32(&done) != n {
		t.Fatalf("期望执行 %d 个任务，实际 %d", n, done)
	}
	// 停止后提交应报错
	if err := p.Submit(func() {}); err != ErrPoolStopped {
		t.Fatalf("停止后 Submit 应返回 ErrPoolStopped，实际 %v", err)
	}
	// 重复 StopAndWait 必须安全
	p.StopAndWait()
}

func TestWorkerPoolSingleWorker(t *testing.T) {
	p := NewWorkerPool(1)
	var val int32
	p.Submit(func() { atomic.StoreInt32(&val, 7) })
	p.StopAndWait()
	if atomic.LoadInt32(&val) != 7 {
		t.Fatal("单 worker 也应执行任务")
	}
}
