package main

import (
	"testing"
	"time"
)

// TestProcGaugesRegistered 验证进程级观测 gauge 已注册且返回合理值：
// uptime 应 > 0，goroutines 应 >= 1。便于 /metrics scrape 直接发现
// 进程重启与 goroutine 泄漏（R6 可验证收益）。
func TestProcGaugesRegistered(t *testing.T) {
	// 幂等重注册：即便其它单测执行 Metrics.Reset() 清空了 funcGauges，这里也能恢复，
	// 避免本断言因指标隔离被误伤。
	ensureProcGauges()
	// Windows 下 time.Now() 分辨率约 1ms：若不前进一个 tick，start 与采样可能落在
	// 同一 tick 内导致 time.Since 恰好为 0。sleep 越过 tick，使 uptime 稳定为正
	//（生产环境 init 在进程启动、远早于读取，无此问题）。
	time.Sleep(2 * time.Millisecond)
	snap := Metrics.Snapshot()
	gauges, ok := snap["gauges"].(map[string]float64)
	if !ok {
		t.Fatalf("gauges 缺失于 snapshot")
	}
	up, ok := gauges["gw_uptime_seconds"]
	if !ok || up <= 0 {
		t.Fatalf("gw_uptime_seconds = %v, want > 0", gauges["gw_uptime_seconds"])
	}
	g, ok := gauges["gw_goroutines"]
	if !ok || g < 1 {
		t.Fatalf("gw_goroutines = %v, want >= 1", gauges["gw_goroutines"])
	}
}
