package util

import (
	"sync"
	"testing"
)

// TestOnceFlagInitial 验证：初始未触发。
func TestOnceFlagInitial(t *testing.T) {
	var f OnceFlag
	if f.Done() {
		t.Fatal("初始应未触发")
	}
}

// TestOnceFlagTriggerOnce 验证：仅首次 Trigger 返回 true，之后恒为 false。
func TestOnceFlagTriggerOnce(t *testing.T) {
	var f OnceFlag
	if !f.Trigger() {
		t.Fatal("首次 Trigger 应返回 true")
	}
	if f.Done() != true {
		t.Fatal("触发后 Done 应为 true")
	}
	for i := 0; i < 100; i++ {
		if f.Trigger() {
			t.Fatalf("第 %d 次重复 Trigger 不应返回 true", i+2)
		}
	}
	if !f.Done() {
		t.Fatal("重复触发后 Done 仍应为 true")
	}
}

// TestOnceFlagConcurrent 验证：多 goroutine 并发 Trigger，恰有一个返回 true。
func TestOnceFlagConcurrent(t *testing.T) {
	var f OnceFlag
	const n = 1000
	var mu sync.Mutex
	success := 0
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if f.Trigger() {
				mu.Lock()
				success++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if success != 1 {
		t.Fatalf("并发下应恰有 1 次 Trigger 成功，实际 %d", success)
	}
	if !f.Done() {
		t.Fatal("并发触发后 Done 应为 true")
	}
}
