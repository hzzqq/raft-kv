package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestStartNeverBlocksUnderContention 是 I152 心跳解节流（kickCh）的回归护栏。
//
// Start() 在追加日志后会 `select { case kickCh <- struct{}{}: default: }` 非阻塞地
// 唤醒一次复制。这个 select/default 结构一旦被误改成阻塞发送（`kickCh <- ...` 无 default），
// 由于 kickCh 缓冲仅为 1、且唯一消费者是 ~110ms 一次的心跳计时器，Start() 会被串行化到
// 心跳周期上限 ≈ 9 ops/sec——写路径吞吐会悄然崩回场景 C 修复前 ~100 ops/sec 的量级。
//
// 本测试用多 goroutine 高频并发 Start()，断言并发吞吐远高于心跳周期上限（地板 50 ops/sec，
// 留足余量区分「非阻塞（数百~数千/s）」与「阻塞（≈9/s）」），把「误改默认分支」这类回归
// 变成 CI 可挡的硬失败。
func TestStartNeverBlocksUnderContention(t *testing.T) {
	cfg := makeConfig(t, 3)
	defer cfg.cleanup()

	leader, _ := cfg.checkOneLeader()
	rf := cfg.rafts[leader]

	const workers = 8
	const dur = 300 * time.Millisecond
	const minThroughput = 50.0 // ops/sec 地板：远低于非阻塞真实吞吐，但远高于阻塞时的心跳周期上限 ≈9/s

	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			local := 0
			for time.Now().Before(start.Add(dur)) {
				if _, _, ok := rf.Start(fmt.Sprintf("kick-%d-%d", w, local)); ok {
					local++
				}
			}
			mu.Lock()
			total += local
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	tp := float64(total) / elapsed.Seconds()
	if tp < minThroughput {
		t.Fatalf("Start() 并发吞吐 %.0f ops/sec < 地板 %.0f：疑似 kickCh 被误改成阻塞发送（心跳周期上限≈9/s）",
			tp, minThroughput)
	}
	t.Logf("Start() 并发 %.0f ops/sec（%d workers, 总提交 %d, 耗时 %v）—— kickCh 非阻塞确认",
		tp, workers, total, elapsed.Round(time.Millisecond))
}
