package util

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSingleFlightDedup 验证：同一 key 的并发 Do 只执行一次 fn。
func TestSingleFlightDedup(t *testing.T) {
	g := NewGroup()
	var backend int64
	var wg sync.WaitGroup

	const n = 50
	results := make([]string, n)
	errs := make([]error, n)
	called := make([]bool, n)

	// 让 fn 稍微耗时，确保并发重叠。
	fn := func() (interface{}, error) {
		atomic.AddInt64(&backend, 1)
		time.Sleep(20 * time.Millisecond)
		return "v", nil
	}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, e, c := g.Do("same-key", fn)
			results[i], errs[i], called[i] = v.(string), e, c
		}(i)
	}
	wg.Wait()

	if got := atomic.LoadInt64(&backend); got != 1 {
		t.Fatalf("期望后端只被调用 1 次（击穿保护），实际 %d 次", got)
	}
	for i := 0; i < n; i++ {
		if results[i] != "v" || errs[i] != nil {
			t.Fatalf("result[%d]=%q err=%v，期望复用结果 v", i, results[i], errs[i])
		}
	}
	// 恰好一个 goroutine 实际执行了 fn。
	var trueCount int
	for _, c := range called {
		if c {
			trueCount++
		}
	}
	if trueCount != 1 {
		t.Fatalf("期望恰好 1 个 goroutine 实际执行 fn，实际 %d", trueCount)
	}
}

// TestSingleFlightDifferentKeys 验证：不同 key 各自独立执行 fn。
func TestSingleFlightDifferentKeys(t *testing.T) {
	g := NewGroup()
	var backend int64
	fn := func() (interface{}, error) {
		atomic.AddInt64(&backend, 1)
		return 1, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			_, _, _ = g.Do(k, fn)
		}(string(rune('a' + i)))
	}
	wg.Wait()
	if got := atomic.LoadInt64(&backend); got != 3 {
		t.Fatalf("期望 3 个不同 key 各执行 1 次，实际 %d", got)
	}
}

// TestSingleFlightCalledFlag 验证：called 标记正确区分回源与复用。
func TestSingleFlightCalledFlag(t *testing.T) {
	g := NewGroup()
	fn := func() (interface{}, error) { return "x", nil }

	_, _, c1 := g.Do("k", fn)
	if !c1 {
		t.Fatal("首个调用应 called=true（回源）")
	}
	_, _, c2 := g.Do("k", fn) // 此时上一波已结束，应开启新一波
	if !c2 {
		t.Fatal("上一波结束后新请求应 called=true（新一波回源）")
	}
}

// TestSingleFlightErrorPropagates 验证：fn 返回的错误被所有复用方共享。
func TestSingleFlightErrorPropagates(t *testing.T) {
	g := NewGroup()
	wantErr := errTest("boom")
	fn := func() (interface{}, error) { return nil, wantErr }

	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e, _ := g.Do("k", fn)
			errs[i] = e
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		if e != wantErr {
			t.Fatalf("errs[%d]=%v，期望共享错误 boom", i, e)
		}
	}
}

type errTest string

func (e errTest) Error() string { return string(e) }
