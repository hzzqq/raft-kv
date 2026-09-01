// scaling_test.go —— 场景 C 的性能回归护栏（轻量版）。
//
// 完整曲线（1/2/3/5 组，每档 3s）在 runPerf 里产出；这里只跑「单组 vs 双组」的
// 最小对照，用短窗口把耗时压到数秒，CI 每次都能挡住两类回归：
//
//  1) 心跳节流回归：若有人 revert 了 Start() 的 kickCh 唤醒，所有配置都会跌回
//     ~100 ops/sec，单组吞吐会跌破地板线；
//  2) 扩展性失效：双组吞吐未达单组的 1.3x，说明分片并行提交没生效。
//
// 不在 -short 下跑（需要起真实内存集群）。
package main

import (
	"testing"
	"time"
)

// bestOf 同一配置连跑 n 次，取稳态吞吐最高的一轮作为该配置的代表样本。
// 目的是滤掉单次运行里 GC/调度抖动带来的偶发低谷——门禁要挡的是『真回归』，
// 不是『机器忙了一下』。真实回归（吞吐跌回 ~100、扩展比≈1.0x）在任何一轮都会出现，
// 故取最优不会漏掉回归，只消除误红。
func bestOf(nGroups int, window time.Duration, n int) perfResult {
	best := runOneConfig(nGroups, window)
	for i := 1; i < n; i++ {
		r := runOneConfig(nGroups, window)
		if r.OpsPerSec > best.OpsPerSec {
			best = r
		}
	}
	return best
}

func TestScalingRatio(t *testing.T) {
	if testing.Short() {
		t.Skip("scaling 回归需起内存集群，非 -short")
	}
	const floor = 200.0       // 单组吞吐地板线：低于此基本可判定心跳节流回归
	const window = 1200 * time.Millisecond
	const repeats = 3 // 每配置跑 3 次取最优，抗单次抖动

	r1 := bestOf(1, window, repeats)
	t.Logf("1 组(best-of-%d): %.1f ops/sec (p50=%.1fms, err=%d)", repeats, r1.OpsPerSec, r1.P50ms, r1.Errors)
	if r1.Errors > 0 {
		t.Fatalf("单组出现 %d 个错误（集群未就绪/死锁）", r1.Errors)
	}
	if r1.OpsPerSec < floor {
		t.Fatalf("单组吞吐 %.1f < 地板线 %.0f：疑似 Start() 心跳节流被 revert（kickCh 唤醒丢失）",
			r1.OpsPerSec, floor)
	}

	r2 := bestOf(2, window, repeats)
	t.Logf("2 组(best-of-%d): %.1f ops/sec (p50=%.1fms, err=%d)", repeats, r2.OpsPerSec, r2.P50ms, r2.Errors)
	if r2.Errors > 0 {
		t.Fatalf("双组出现 %d 个错误", r2.Errors)
	}
	if r2.OpsPerSec < r1.OpsPerSec*1.3 {
		t.Fatalf("2 组 %.1f 未达单组 %.1f 的 1.3x（扩展失效：分片并行提交未生效？）",
			r2.OpsPerSec, r1.OpsPerSec)
	}
}
