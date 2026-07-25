// metrics_bench_test.go —— 指标库热点路径基准（cycle #116）。
//
// 这些基准建立「每次请求/每条日志都会触碰」的写入路径基线，供后续优化
// （标签向量免锁读缓存，cycle #119）做 A/B 对比。运行：
//   go test -run='^$' -bench='Benchmark(Metrics|Counter|Histogram|Gauge)' -benchmem ./src/metrics
package metrics

import (
	"testing"
)

func BenchmarkCounterInc(b *testing.B) {
	c := &Counter{}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc()
		}
	})
}

func BenchmarkGaugeSet(b *testing.B) {
	g := &Gauge{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		g.Set(float64(i))
	}
}

func BenchmarkHistogramRecord(b *testing.B) {
	h := NewHistogram()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h.Record(1.5)
		}
	})
}

// BenchmarkGaugeVecWithLabelValues 模拟网关每请求取「已存在」标签序列的代价：
// 所有 goroutine 都命中同一组 label，衡量高频 WithLabelValues 的加锁开销。
func BenchmarkGaugeVecWithLabelValues(b *testing.B) {
	v := NewGaugeVec("method", "code")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v.WithLabelValues("GET", "200").Set(1)
		}
	})
}

// BenchmarkCounterVecWithLabelValues 同上的 counter 向量版本（cycle #117 新增原语）。
func BenchmarkCounterVecWithLabelValues(b *testing.B) {
	v := NewCounterVec("method", "code")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v.WithLabelValues("GET", "200").Inc()
		}
	})
}
