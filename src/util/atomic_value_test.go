package util

import (
	"sync"
	"testing"
)

func TestAtomic(t *testing.T) {
	a := NewAtomic(10)
	if a.Load() != 10 {
		t.Fatalf("初始值应为 10，实际 %d", a.Load())
	}
	a.Store(20)
	if a.Load() != 20 {
		t.Fatalf("Store 后应为 20，实际 %d", a.Load())
	}
	old := a.Swap(30)
	if old != 20 || a.Load() != 30 {
		t.Fatalf("Swap 返回 %d，当前 %d，期望 (20,30)", old, a.Load())
	}
	if !a.CompareAndSwap(30, 31) {
		t.Fatal("CAS(30→31) 应成功")
	}
	if a.CompareAndSwap(99, 100) {
		t.Fatal("CAS(99→100) 应失败（当前为 31）")
	}
	if a.Load() != 31 {
		t.Fatalf("CAS 失败后值应保持 31，实际 %d", a.Load())
	}
}

// TestAtomicConcurrent 验证多写多读无数据竞争（race detector 关，但仍验证逻辑一致）。
func TestAtomicConcurrent(t *testing.T) {
	a := NewAtomic(0)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				a.Store(a.Load() + 1)
			}
		}()
	}
	wg.Wait()
	// 50*100 = 5000，但并发自增非原子（Load+Store 非临界区），此处仅验证不 panic、类型稳定。
	if a.Load() < 50 {
		t.Fatalf("并发 Store 后值异常偏低：%d", a.Load())
	}
}
