// label_vec_test.go —— 免锁读缓存并发正确性（cycle #119）。
//
// 验证：多 goroutine 并发 WithLabelValues（同标签命中读路径 + 异标签写路径）下，
// 子序列指针稳定、计数无丢失、无 panic。注：完整数据竞争检测需 -race（本环境无 gcc，
// 由 labelVec 的 atomic.Value 读快照 + mutex 写保证无竞争）。
package metrics

import (
	"sync"
	"testing"
)

func TestLabelVecConcurrentCounter(t *testing.T) {
	v := NewCounterVec("id")
	const goroutines = 32
	const per = 1000
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		// 每个 goroutine 用「相同标签」命中读缓存，压力测试免锁热路径。
		go func() {
			defer wg.Done()
			for j := 0; j < per; j++ {
				v.WithLabelValues("same").Inc()
			}
		}()
	}
	wg.Wait()
	if got := v.WithLabelValues("same").Value(); got != goroutines*per {
		t.Fatalf("counter = %d, want %d", got, goroutines*per)
	}
}

func TestLabelVecConcurrentDistinctKeys(t *testing.T) {
	v := NewGaugeVec("id")
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			v.WithLabelValues(string(rune('a'+i%26))+string(rune(i))).Set(1)
		}()
	}
	wg.Wait()
	// 全部标签组合都应存在，且指针在生命周期内稳定（命中读缓存返回同一实例）。
	if got := len(v.Keys()); got != n {
		t.Fatalf("distinct keys = %d, want %d", got, n)
	}
	first := v.WithLabelValues("a" + string(rune(0)))
	if v.WithLabelValues("a"+string(rune(0))) != first {
		t.Fatal("same label set returned different instances (pointer stability broken)")
	}
}
