// lru_bench_test.go —— LRU 热点路径基准（cycle #116）。
//
// 注意：util.LRU 是无锁的单线程数据结构（按设计），故基准串行执行；
// 与并发安全的 Counter/Histogram/Semaphore 不同，不应跨 goroutine 共享。运行：
//   go test -run='^$' -bench='BenchmarkLRU' -benchmem ./src/util
package util

import (
	"testing"
)

func BenchmarkLRUGet(b *testing.B) {
	c := NewLRU(1024)
	for i := 0; i < 1024; i++ {
		c.Put(string(rune('a'+i%26))+string(rune(i)), i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Get(string(rune('a'+i%26)) + string(rune(i)))
	}
}

func BenchmarkLRUPut(b *testing.B) {
	c := NewLRU(1024)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Put(string(rune('a'+i%26))+string(rune(i)), i)
	}
}
