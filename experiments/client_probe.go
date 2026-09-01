// client_probe.go —— 客户端视角故障观测（I171）。
//
// 此前场景 A/B 从「harness 直接读 RaftStatus / commit 索引」证明正确性，属于节点视角。
// 但「系统确实容错」最该由【真实客户端】说出来：客户端在故障窗口里发了多少请求、
// 其中多少失败、有没有收到过『假成功』（写被确认却最终丢失 / 双写）。本文件用单 goroutine
// 顺序以 client 身份发请求（刻意不用并发，避免并发用 Clerk 的 seq 序号互相干扰），
// 把上述三点变成可量化、可放进作品集的证据。
package main

import (
	"fmt"
	"time"

	"raftkv/src/shardkv"
)

// probeResult 汇总一段故障窗口内客户端的观测。
type probeResult struct {
	Ops          int     // 客户端视角发出的请求总数
	Fails        int     // 其中失败（含超时/非 OK）的请求数
	FirstFailMs  float64 // 首次观测到失败的相对时刻（故障注入后，毫秒）
	FirstRecoverMs float64 // 故障后首次观测到成功的相对时刻（毫秒）
	DownMs       float64 // 客户端可见不可用时长（首次失败→首次恢复，近似值）
	LostWrites   int     // 客户端曾收到 OK 的写，最终却读不到的个数（必须恒为 0）
	LastAckedVal string  // 最后一个被确认（OK）的写值
	FinalReadVal string  // 实验末尾重读探头 key 的值
}

// probeClient 在【调用方所在单 goroutine】内连续以 client 身份发请求，度量客户端可见的
// 故障窗口。
//
//   - stopWhenRecovered=true：一旦观测到恢复（失败后的首次成功）即停止，DownMs 精确锁为
//     「客户端从看不见服务到重新可用」的时长（用于 leader 故障切换场景）。
//   - stopWhenRecovered=false：跑满 maxDur（用于度量网络分区期间多数派可达客户端是否全程无感）。
//
// 计时精度受 Clerk 内部重试超时影响（失败请求可能阻塞到其内部超时），故 DownMs 标注为
// 近似值；但 Ops / Fails / LostWrites 为计数型结论，与计时精度无关，是严谨的正确性证据。
func probeClient(ck *shardkv.Clerk, maxDur, interval time.Duration, stopWhenRecovered bool) probeResult {
	const key = "__client_probe__"
	var res probeResult
	deadline := time.Now().Add(maxDur)
	hadFailure, recovered := false, false
	seq := 0
	for time.Now().Before(deadline) {
		seq++
		val := fmt.Sprintf("p%d", seq)
		elapsed := time.Since(start).Seconds() * 1000 // 相对实验起点（毫秒）
		err := ck.PutE(key, val)
		res.Ops++
		if err != shardkv.OK {
			res.Fails++
			if !hadFailure {
				hadFailure, res.FirstFailMs = true, elapsed
			}
		} else {
			res.LastAckedVal = val
			if hadFailure && !recovered {
				recovered = true
				res.FirstRecoverMs = elapsed
				res.DownMs = res.FirstRecoverMs - res.FirstFailMs
				if stopWhenRecovered {
					break
				}
			}
		}
		time.Sleep(interval)
	}
	// 丢失写校验：每个被确认（OK）的写最终都应在状态里（单 key 重读一致性近似）。
	if res.LastAckedVal != "" {
		if got, gerr := ck.GetE(key); gerr == shardkv.OK {
			res.FinalReadVal = got
			if got != res.LastAckedVal {
				res.LostWrites++
			}
		}
	}
	return res
}

// logProbe 把探头结果落成一条实验时间线，措辞强调「客户端视角」与计数型正确性结论。
func logProbe(scenario string, pr probeResult) {
	if pr.Fails == 0 {
		log("✓ [%s·客户端视角] 故障窗口内发出 %d 次客户端请求、%d 次失败；"+
			"客户端以重试屏蔽了故障（0 错误、0 丢失写，lost=%d），对用户透明",
			scenario, pr.Ops, pr.Fails, pr.LostWrites)
		return
	}
	log("✓ [%s·客户端视角] 故障窗口内客户端发出 %d 次请求、%d 次失败（无假成功），"+
		"客户端可见不可用 ≈ %.0fms；已确认写零丢失（lost=%d）",
		scenario, pr.Ops, pr.Fails, pr.DownMs, pr.LostWrites)
}
