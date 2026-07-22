package util

import (
	"sync"
	"testing"
	"time"
)

// TestTokenBucketBurst 验证：满桶可连续取光，耗尽后拒绝，前进时钟补充后可再取。
func TestTokenBucketBurst(t *testing.T) {
	now := time.Unix(0, 0)
	tb := NewTokenBucket(1, 5)
	tb.now = func() time.Time { return now }
	tb.last = now

	for i := 0; i < 5; i++ {
		if !tb.Allow() {
			t.Fatalf("第 %d 次取令牌应成功（满桶）", i+1)
		}
	}
	if tb.Allow() {
		t.Fatalf("桶已空，第 6 次取令牌应失败")
	}
	// 前进 1 秒补充 1 令牌（rate=1/s）。
	now = now.Add(time.Second)
	if !tb.Allow() {
		t.Fatalf("补充 1 令牌后应可取")
	}
	if tb.Allow() {
		t.Fatalf("仅补充 1 令牌，第二次应失败")
	}
}

// TestTokenBucketAllowN 验证：AllowN 超过容量时拒绝；不消耗令牌。
func TestTokenBucketAllowN(t *testing.T) {
	now := time.Unix(0, 0)
	tb := NewTokenBucket(2, 5)
	tb.now = func() time.Time { return now }
	tb.last = now

	if !tb.AllowN(5) {
		t.Fatalf("AllowN(5) 应成功（满桶=容量）")
	}
	if tb.AllowN(1) {
		t.Fatalf("桶已空，AllowN(1) 应失败")
	}
	if tb.Available() != 0 {
		t.Fatalf("失败后令牌应为 0，实际 %.2f", tb.Available())
	}
	// 前进 2 秒补充 4 令牌（rate=2/s），仍不足以 AllowN(5)。
	now = now.Add(2 * time.Second)
	if tb.AllowN(5) {
		t.Fatalf("仅补充 4 令牌，AllowN(5) 应失败")
	}
	if !tb.AllowN(4) {
		t.Fatalf("补充 4 令牌，AllowN(4) 应成功")
	}
}

// TestTokenBucketRate 验证：长期速率由 rate 决定（不依赖突发）。
func TestTokenBucketRate(t *testing.T) {
	now := time.Unix(0, 0)
	tb := NewTokenBucket(10, 10) // 10/s，容量 10
	tb.now = func() time.Time { return now }
	tb.last = now

	// 取光。
	for i := 0; i < 10; i++ {
		if !tb.Allow() {
			t.Fatalf("初始满桶第 %d 次应成功", i+1)
		}
	}
	// 前进 0.5s 只补充 5 令牌。
	now = now.Add(500 * time.Millisecond)
	if got := tb.Available(); got < 4.9 || got > 5.1 {
		t.Fatalf("0.5s 后应有≈5 令牌，实际 %.2f", got)
	}
}

// TestTokenBucketConcurrent 验证：并发取令牌总数不超过容量上限（无超发）。
func TestTokenBucketConcurrent(t *testing.T) {
	tb := NewTokenBucket(1000, 50) // 高补充率，容量 50
	var ok int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tb.Allow() {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	// 初始只是满桶 50，最多 50 个 goroutine 能取到（补充远慢于瞬间并发）。
	if ok > 50 {
		t.Fatalf("并发不应超发：成功数 %d > 容量 50", ok)
	}
}
