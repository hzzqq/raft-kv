package shardkv

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"raftkv/src/metrics"
)

// BenchmarkSKVPutGet 测量单 group 下单线程 Put 吞吐（ops/sec）。
// 用法：go test ./src/shardkv/ -run XXX -bench BenchmarkSKVPutGet -benchtime 3s
func BenchmarkSKVPutGet(b *testing.B) {
	cfg := makeSKVConfig(b, 1, 3, 3, 0)
	defer cfg.cleanup()
	cfg.joinGroup(0)
	cfg.waitGroupConfig(0, 0, 1)
	ck := cfg.makeClerk()
	Metrics.Reset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ck.Put(fmt.Sprintf("bkey-%d", i%200), fmt.Sprintf("bval-%d", i))
	}
	b.StopTimer()
	Metrics.Reset()
}

// BenchmarkSKVGet 测量单 group 下单线程 Get 吞吐（ops/sec）。
func BenchmarkSKVGet(b *testing.B) {
	cfg := makeSKVConfig(b, 1, 3, 3, 0)
	defer cfg.cleanup()
	cfg.joinGroup(0)
	cfg.waitGroupConfig(0, 0, 1)
	ck := cfg.makeClerk()
	for i := 0; i < 200; i++ {
		ck.Put(fmt.Sprintf("bkey-%d", i), fmt.Sprintf("bval-%d", i))
	}
	Metrics.Reset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ck.Get(fmt.Sprintf("bkey-%d", i%200))
	}
	b.StopTimer()
	Metrics.Reset()
}

// TestSKVBenchmark 用并发客户端在固定时长内压测，打印吞吐与延迟分位数，
// 作为后续效率优化（如 ReadIndex 读取、Clerk 缓存）的可量化基线。
// 与 shardkv.Metrics 打通：直接读取进程级指标快照。
func TestSKVBenchmark(t *testing.T) {
	const nGroups = 2
	cfg := makeSKVConfig(t, nGroups, 3, 3, 0)
	defer cfg.cleanup()
	for g := 0; g < nGroups; g++ {
		cfg.joinGroup(g)
	}
	cfg.waitGroupConfig(0, 0, nGroups)

	Metrics.Reset()

	const nClients = 8
	const duration = 5 * time.Second
	var ops int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(duration)
	for c := 0; c < nClients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			local := cfg.makeClerk()
			i := 0
			for time.Now().Before(deadline) {
				k := fmt.Sprintf("bk-%d-%d", c, i%100)
				local.Put(k, fmt.Sprintf("v%d", i))
				atomic.AddInt64(&ops, 1)
				i++
			}
		}(c)
	}
	wg.Wait()
	elapsed := duration.Seconds()
	rate := float64(ops) / elapsed

	snap := Metrics.Snapshot()
	t.Logf("=== ShardKV benchmark ===")
	t.Logf("clients=%d duration=%.1fs ops=%d rate=%.1f ops/sec", nClients, elapsed, ops, rate)
	if h, ok := snap["histograms"].(map[string]metrics.HistSnapshot); ok {
		if lat, ok2 := h["op_latency_ms"]; ok2 {
			t.Logf("op_latency_ms: count=%d p50=%.2f p95=%.2f p99=%.2f", lat.Count, lat.P50, lat.P95, lat.P99)
		}
	}
	if c, ok := snap["counters"].(map[string]int64); ok {
		t.Logf("counters: ops_total=%d ops_errors=%d entries_applied=%d", c["ops_total"], c["ops_errors"], c["entries_applied"])
	}
	// 健全性：仅需确认集群确实在对外服务即可。注意：当前基线吞吐偏低，
	// 主因是 Clerk 每次操作都向 ShardMaster 查询配置（见 shardkv.go Clerk.refresh），
	// 该低效将在后续「Clerk 配置缓存」优化中显著改善——本测试即为该优化的量化基线。
	// 阈值取 10（满负载/资源争用下基线会波动，10 次已足以证明集群在对外服务，
	// 避免全量套件顺序运行时的偶发误判）。
	if ops < 10 {
		t.Fatalf("benchmark produced too few ops (%d); cluster likely not serving", ops)
	}
}
