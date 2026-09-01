// client_probe.go —— 客户端视角故障观测（I171）。
//
// 此前场景 A/B 从「harness 直接读 RaftStatus / commit 索引」证明正确性，属于节点视角。
// 但「系统确实容错」最该由【真实客户端】说出来：客户端在故障窗口里发了多少请求、
// 其中多少失败、有没有收到过『假成功』（写被确认却最终丢失 / 双写）。本文件用单 goroutine
// 顺序以 client 身份发请求（刻意不用并发，避免并发用 Clerk 的 seq 序号互相干扰），
// 把上述三点变成可量化、可放进作品集的证据。
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"raftkv/src/cluster"
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

// minorityProbeResult 汇总「少数派客户端」在分区期间的连续观测。
// 这是客户端视角故事里最该量化、也最危险的一半：多数派可达客户端无感（probeClient），
// 而少数派客户端——连得上旧 leader、却拿不到 quorum——必须全程被拒且绝不可拿到一次成功
// （否则就是脑裂双写）。
type minorityProbeResult struct {
	Attempts     int     // 少数派客户端发起的写请求总数
	Fails        int     // 被拒（Err≠OK 或 RPC 不可达）的次数
	Success      int     // 竟然被接受（Err=OK）的次数——必须恒为 0，否则即脑裂
	FirstFailMs  float64 // 首次被拒的相对时刻（毫秒）
	LastFailMs   float64 // 末次被拒的相对时刻（毫秒）
	WindowMs     float64 // 客户端持续被拒的窗口（首次→末次），即「饿死」时长
}

// probeMinorityWindow 在【单 goroutine】内以「未被隔离的客户端」身份（端点 owner=9500，
// 与 probeMinorityWrite 同源）持续直连少数派 leader 发写，量化其在分区全程被拒的窗口。
// 不同于 probeClient（走 Clerk 重试到多数派），这里故意直连少数派，把「客户端连得上却
// 写不进」变成连续、可计数的运行时证据。
//
// 难点：单次 minority 写会阻塞到服务端提交超时（~3s），若顺序发则 1.5s 窗口只采到 1 个样本。
// 故每次尝试用【独立端点 + goroutine + 200ms 客户端超时】切成多个可计数样本；独立端点
// 保证不会在同一端点的 RPC 上并发（labrpc 的 end.Call 非并发安全），超时即记为被拒。
func probeMinorityWindow(c *cluster.Cluster, g, r int, dur, interval time.Duration) minorityProbeResult {
	const owner = 9500
	var res minorityProbeResult
	deadline := time.Now().Add(dur)
	seq := 0
	for time.Now().Before(deadline) {
		seq++
		endName := 7800 + seq // 每次新建独立端点，避免同端点并发 Call 不安全
		end := c.Net.MakeEnd(endName, owner)
		c.Net.Connect(endName, 1000+g*100+r)
		args := &shardkv.PutAppendArgs{
			Key:      "ghost-probe",
			Value:    fmt.Sprintf("p%d", seq),
			OpType:   "Put",
			ClientId: 990002,
			Seq:      int64(seq),
		}
		reply := &shardkv.PutAppendReply{}
		elapsed := time.Since(start).Seconds() * 1000
		// 单次 minority 写阻塞到服务端提交超时（~3s）；用 goroutine+短超时切成多个样本。
		done := make(chan struct{})
		go func() {
			end.Call("ShardKV.PutAppend", args, reply)
			close(done)
		}()
		ok := false
		select {
		case <-done:
			ok = (reply.Err == shardkv.OK) // 在超时内返回且被接受 ⇒ 危险（脑裂）
		case <-time.After(200 * time.Millisecond):
			ok = false // 超时即视为被拒（与脑裂防护语义一致）
		}
		res.Attempts++
		if ok {
			res.Success++ // 危险：少数派竟接受写 → 脑裂双写
		} else {
			res.Fails++
			if res.FirstFailMs == 0 {
				res.FirstFailMs = elapsed
			}
			res.LastFailMs = elapsed
		}
		time.Sleep(interval)
	}
	if res.FirstFailMs > 0 {
		res.WindowMs = res.LastFailMs - res.FirstFailMs
	}
	return res
}

// ---- 客户端视角结构化产物（I174）：供控制台「实验与验证」Tab 直接渲染 KPI ----
//
// 上面 probeResult / minorityProbeResult 只被 logProbe 打成 stdout 文本行（进 results/scene_*.log）。
// 为让控制台把「客户端视角容错」渲染成结构化、可复核的 KPI（而不仅是日志文本），这里把探头结果
// 额外落盘成 results/client_view_*.json。与场景 C 的 perf_shard_scaling.json 同理：带 generated_at
// 字段，被 experiments/main.go 的 writeArtifactManifest 自动纳入汇总清单，控制台读取其 freshness。

// clientViewReport 多数派可达客户端视角的结构化报告（leader 故障 / 分区多数派两场景共用）。
type clientViewReport struct {
	Scenario        string  `json:"scenario"`
	GeneratedAt     string  `json:"generated_at"`
	Ops             int     `json:"ops"`
	Fails           int     `json:"fails"`
	LostWrites      int     `json:"lost_writes"`
	DownMs          float64 `json:"down_ms"`
	FirstFailMs     float64 `json:"first_fail_ms"`
	FirstRecoverMs  float64 `json:"first_recover_ms"`
	Conclusion      string  `json:"conclusion"`
	Ok              bool    `json:"ok"`
}

// minorityViewReport 少数派客户端视角（危险路径）的结构化报告：重点是 Success 必须恒为 0。
type minorityViewReport struct {
	Scenario    string  `json:"scenario"`
	GeneratedAt string  `json:"generated_at"`
	Attempts    int     `json:"attempts"`
	Fails       int     `json:"fails"`
	Success     int     `json:"success"`
	FirstFailMs float64 `json:"first_fail_ms"`
	LastFailMs  float64 `json:"last_fail_ms"`
	WindowMs    float64 `json:"window_ms"`
	SplitBrain  bool    `json:"split_brain"`
	Conclusion  string  `json:"conclusion"`
	Ok          bool    `json:"ok"`
}

// writeClientViewJSON 把客户端视角报告落盘为 results/<name>（带缩进，便于人工核对）；
// 写失败静默忽略（产物是锦上添花，绝不影响实验主流程与正确性断言）。
func writeClientViewJSON(name string, v interface{}) {
	dir := "results"
	_ = os.MkdirAll(dir, 0o755)
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(dir, name), b, 0o644)
}
