// semaphore_bench_test.go —— 信号量并发获取/释放基准（cycle #116）。
//
// 网关与 kvcli 批量扇出都依赖 util.Semaphore 做有界并发。运行：
//   go test -run='^$' -bench='BenchmarkSemaphore' -benchmem ./src/util
package util

import (
	"context"
	"testing"
)

func BenchmarkSemaphoreAcquireRelease(b *testing.B) {
	s := NewSemaphore(64)
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = s.Acquire(ctx)
			s.Release()
		}
	})
}
