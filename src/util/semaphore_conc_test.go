package util

import (
	"context"
	"runtime"
	"sync"
	"testing"
)

// TestSemaphoreConcurrentNoOvercommit 验证：在高并发 Acquire/Release 下，
// 任意时刻「同时持有」的许可数不超过容量（无超发），且最终无残留泄漏。
// 该用例同时是 len(ch) 数据竞争的回归测试——旧的 InUse()/TryAcquireWeighted
// 读取 channel len/cap，在 -race 下会与并发 send/recv 报 DATA RACE。
func TestSemaphoreConcurrentNoOvercommit(t *testing.T) {
	const n = 4
	s := NewSemaphore(n)

	var mu sync.Mutex
	held := 0
	maxHeld := 0

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 300; j++ {
				if err := s.Acquire(context.Background()); err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				mu.Lock()
				held++
				if held > maxHeld {
					maxHeld = held
				}
				if held > n {
					mu.Unlock()
					t.Errorf("OVERCOMMIT: held=%d > cap=%d", held, n)
					s.Release()
					return
				}
				mu.Unlock()

				// 模拟极短临界区，制造调度交错
				runtime.Gosched()

				mu.Lock()
				held--
				mu.Unlock()
				s.Release()

				// 并发观测 InUse()，旧实现会触发数据竞争
				_ = s.InUse()
			}
		}()
	}
	wg.Wait()

	if held != 0 {
		t.Fatalf("泄漏：结束时仍持有 %d 个许可", held)
	}
	if maxHeld > n {
		t.Fatalf("峰值持有 %d 超过容量 %d", maxHeld, n)
	}
	if s.InUse() != 0 {
		t.Fatalf("InUse()=%d，应为 0（无残留）", s.InUse())
	}
}

// TestSemaphoreTryAcquireWeightedConcurrent 验证：并发 TryAcquireWeighted 总量
// 不超过容量，且所有成功获取最终都能被释放回收（无令牌丢失）。
func TestSemaphoreTryAcquireWeightedConcurrent(t *testing.T) {
	const n = 3
	s := NewSemaphore(n)

	var mu sync.Mutex
	live := 0
	maxLive := 0
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 300; j++ {
				if !s.TryAcquireWeighted(1) {
					continue // 满桶，跳过
				}
				mu.Lock()
				live++
				if live > maxLive {
					maxLive = live
				}
				if live > n {
					mu.Unlock()
					t.Errorf("OVERCOMMIT via TryAcquire: live=%d > cap=%d", live, n)
					s.Release()
					return
				}
				mu.Unlock()

				runtime.Gosched()

				mu.Lock()
				live--
				mu.Unlock()
				s.Release()
			}
		}()
	}
	wg.Wait()

	if live != 0 {
		t.Fatalf("泄漏：结束时仍持有 %d 个许可", live)
	}
	if maxLive > n {
		t.Fatalf("峰值持有 %d 超过容量 %d", maxLive, n)
	}
	if s.InUse() != 0 {
		t.Fatalf("InUse()=%d，应为 0", s.InUse())
	}
}
