package util

import (
	"sync"
	"testing"
	"time"
)

// TestSlidingWindowBasic 验证：100ms 窗口、上限 3，前 3 次放行后拒，滑窗后恢复。
func TestSlidingWindowBasic(t *testing.T) {
	l := NewSlidingWindowLimiter(100*time.Millisecond, 3)
	for i := 0; i < 3; i++ {
		if !l.Allow("k") {
			t.Fatalf("第 %d 次应放行", i+1)
		}
	}
	if l.Allow("k") {
		t.Fatal("第 4 次应被拒（窗口内达上限）")
	}
	if l.Allow("k") {
		t.Fatal("第 5 次仍应被拒")
	}
	time.Sleep(110 * time.Millisecond) // 滑动窗口滑过
	if !l.Allow("k") {
		t.Fatal("窗口滑过后应恢复放行")
	}
}

// TestSlidingWindowPerKey 验证：不同 key 各自独立计数。
func TestSlidingWindowPerKey(t *testing.T) {
	l := NewSlidingWindowLimiter(time.Second, 2)
	if !l.Allow("a") || !l.Allow("a") {
		t.Fatal("a 前 2 次应放行")
	}
	if l.Allow("a") {
		t.Fatal("a 第 3 次应被拒")
	}
	if !l.Allow("b") {
		t.Fatal("b 独立计数，首应放行")
	}
}

// TestSlidingWindowAllowN 验证：批量放行 n 笔的边界计算。
func TestSlidingWindowAllowN(t *testing.T) {
	l := NewSlidingWindowLimiter(time.Second, 5)
	if !l.AllowN("k", 3) {
		t.Fatal("AllowN(k,3) 应放行")
	}
	if l.AllowN("k", 3) {
		t.Fatal("再 AllowN(k,3) 应被拒（3+3>5）")
	}
	if !l.AllowN("k", 2) {
		t.Fatal("AllowN(k,2) 应放行（3+2=5 触顶）")
	}
	if l.Allow("k") {
		t.Fatal("再 Allow(k,1) 应被拒（已达 5）")
	}
}

// TestSlidingWindowConcurrent 验证：并发 Allow 不 panic、计数不超上限。
func TestSlidingWindowConcurrent(t *testing.T) {
	l := NewSlidingWindowLimiter(time.Second, 100)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.Allow("k")
		}()
	}
	wg.Wait()
	// 50 次并发放行，不超上限即不 panic 即可（具体计数因竞态不必精确）。
	if l.AllowN("k", 51) {
		t.Fatal("已达 50，再放 51 应被拒")
	}
}
