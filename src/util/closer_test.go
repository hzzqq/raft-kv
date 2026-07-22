package util

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestCloser(t *testing.T) {
	c := NewCloser()
	var exited int32
	for i := 0; i < 5; i++ {
		c.Add(1)
		go func() {
			defer c.Done()
			<-c.C()
			atomic.AddInt32(&exited, 1)
		}()
	}
	// 稍等确保 worker 都已阻塞在 <-c.C()（验证非时序竞态）
	time.Sleep(20 * time.Millisecond)
	c.Close()
	c.Wait()
	if atomic.LoadInt32(&exited) != 5 {
		t.Fatalf("期望 5 个 worker 全部退出，实际 %d", exited)
	}
	// 重复 Close 必须安全（不 panic）
	c.Close()
}

func TestCloserNoWorkers(t *testing.T) {
	c := NewCloser()
	c.Close()
	c.Wait() // 无 worker 时立即返回
}
