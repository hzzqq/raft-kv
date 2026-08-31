// perf.go —— 场景 C：分片扩展性对比曲线。
//
// 回答一个此前无人回答的问题：ShardKV 把 10 个分片摊到更多 group 之后，吞吐到底
// 涨不涨？此前 bench 只有一个孤立数字（~16.6 ops/sec，单 group、单客户端），既看不出
// 扩展性，也无法判断瓶颈在哪。这里对 1/2/3/5 组各跑一轮固定时长的并发读写，
// 输出吞吐 + 延迟分位数 + ASCII 柱状图，并落盘 JSON/SVG 供报告与作品集引用。
//
// 结论的可信度前提（诚实声明）：内存 labrpc 传输，无真实网络与磁盘，绝对值只在
// 同机同参数下可比；有价值的是「随 group 数变化的相对趋势」。
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"raftkv/src/cluster"
	"raftkv/src/shardkv"
)

// perfResult 是单个配置（group 数）跑完一轮后的测量结果。
type perfResult struct {
	Groups    int     `json:"groups"`
	Replicas  int     `json:"replicas"`
	Clients   int     `json:"clients"`
	Seconds   float64 `json:"seconds"`
	Ops       int     `json:"ops"`
	OpsPerSec float64 `json:"ops_per_sec"`
	P50ms     float64 `json:"p50_ms"`
	P95ms     float64 `json:"p95_ms"`
	Errors    int     `json:"errors"`
}

const (
	perfReplicas = 3
	perfClients  = 12
	perfWindow   = 3 * time.Second
)

// runOneConfig 在 nGroups 个 group 上跑一轮并发写读，返回测量结果。
// 每个客户端独占键前缀，键经 key2shard 摊到 10 个分片上，因此 group 越多、
// 能并行提交的 Raft 组越多——这正是要测的扩展性。
func runOneConfig(nGroups int) perfResult {
	c := cluster.StartCluster(nGroups, perfReplicas, 3, -1)
	defer c.Cleanup()
	bootstrap(c, nGroups)
	// 等再平衡（Join 触发的分片迁移）彻底落定，避免把迁移期的冻结算进吞吐。
	c.WaitAllConfigs(nGroups)
	warm := c.Clerk()
	for i := 0; i < 10; i++ { // 预热：每个分片至少摸一次，触发完迁移
		warm.Put(fmt.Sprintf("warm-%d", i), "w")
	}

	var (
		mu        sync.Mutex
		lats      []time.Duration
		ops, errs int
		wg        sync.WaitGroup
	)
	deadline := time.Now().Add(perfWindow)
	t0 := time.Now()
	for cid := 0; cid < perfClients; cid++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			ck := c.Clerk()
			localLat := make([]time.Duration, 0, 128)
			localOps, localErr := 0, 0
			for i := 0; time.Now().Before(deadline); i++ {
				k := fmt.Sprintf("bench-%d-%d", cid, i)
				s := time.Now()
				if err := ck.PutE(k, "v"); err != shardkv.OK {
					localErr++
					continue
				}
				localLat = append(localLat, time.Since(s))
				localOps++
			}
			mu.Lock()
			lats = append(lats, localLat...)
			ops += localOps
			errs += localErr
			mu.Unlock()
		}(cid)
	}
	wg.Wait()
	elapsed := time.Since(t0).Seconds()

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(p float64) float64 {
		if len(lats) == 0 {
			return 0
		}
		i := int(float64(len(lats)-1) * p)
		return float64(lats[i].Microseconds()) / 1000
	}
	return perfResult{
		Groups: nGroups, Replicas: perfReplicas, Clients: perfClients,
		Seconds: elapsed, Ops: ops, OpsPerSec: float64(ops) / elapsed,
		P50ms: pct(0.50), P95ms: pct(0.95), Errors: errs,
	}
}

// bar 生成一行等宽 ASCII 柱（用于终端里直接看出扩展趋势）。
func bar(v, max float64, width int) string {
	if max <= 0 {
		return ""
	}
	n := int(v / max * float64(width))
	out := make([]rune, 0, width)
	for i := 0; i < width; i++ {
		if i < n {
			out = append(out, '#')
		} else {
			out = append(out, '.')
		}
	}
	return string(out)
}

// writeSVG 把结果画成一张独立柱状图（无外部依赖，可直接嵌进报告/README）。
func writeSVG(path string, rs []perfResult) error {
	const w, h, pad = 560, 300, 46
	maxV := 0.0
	for _, r := range rs {
		if r.OpsPerSec > maxV {
			maxV = r.OpsPerSec
		}
	}
	bw := (w - 2*pad) / len(rs)
	s := fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d">`, w, h, w, h)
	s += `<rect width="100%" height="100%" fill="#ffffff"/>`
	s += fmt.Sprintf(`<text x="%d" y="24" font-family="sans-serif" font-size="15" fill="#111">ShardKV 分片扩展性：吞吐 vs group 数（%d 并发客户端 / 每组 %d 副本）</text>`,
		pad-16, perfClients, perfReplicas)
	// 坐标轴
	s += fmt.Sprintf(`<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#999"/>`, pad, h-pad, w-pad/2, h-pad)
	s += fmt.Sprintf(`<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#999"/>`, pad, 40, pad, h-pad)
	for i, r := range rs {
		bh := 0
		if maxV > 0 {
			bh = int(r.OpsPerSec / maxV * float64(h-pad-60))
		}
		x := pad + i*bw + bw/6
		y := h - pad - bh
		s += fmt.Sprintf(`<rect x="%d" y="%d" width="%d" height="%d" fill="#2f6fdb"/>`, x, y, bw*2/3, bh)
		s += fmt.Sprintf(`<text x="%d" y="%d" font-family="sans-serif" font-size="12" fill="#111" text-anchor="middle">%.1f</text>`,
			x+bw/3, y-6, r.OpsPerSec)
		s += fmt.Sprintf(`<text x="%d" y="%d" font-family="sans-serif" font-size="12" fill="#333" text-anchor="middle">%d 组</text>`,
			x+bw/3, h-pad+18, r.Groups)
	}
	s += fmt.Sprintf(`<text x="%d" y="%d" font-family="sans-serif" font-size="11" fill="#666">纵轴：ops/sec（内存 labrpc，绝对值仅同机可比；看趋势）</text>`,
		pad, h-12)
	s += `</svg>`
	return os.WriteFile(path, []byte(s), 0o644)
}

func runPerf() {
	groupsList := []int{1, 2, 3, 5}
	resetClock()
	log("场景 C：分片扩展性对比（每组 %d 副本，%d 并发客户端，每档 %.0fs）",
		perfReplicas, perfClients, perfWindow.Seconds())

	results := make([]perfResult, 0, len(groupsList))
	for _, g := range groupsList {
		log("跑 %d 组…", g)
		r := runOneConfig(g)
		log("  %d 组: %.1f ops/sec (ops=%d, p50=%.1fms, p95=%.1fms, err=%d)",
			r.Groups, r.OpsPerSec, r.Ops, r.P50ms, r.P95ms, r.Errors)
		results = append(results, r)
	}

	maxV := 0.0
	for _, r := range results {
		if r.OpsPerSec > maxV {
			maxV = r.OpsPerSec
		}
	}
	fmt.Println()
	fmt.Println("组数  吞吐(ops/sec)  p50(ms)  p95(ms)   相对1组   ")
	base := results[0].OpsPerSec
	for _, r := range results {
		speedup := 0.0
		if base > 0 {
			speedup = r.OpsPerSec / base
		}
		fmt.Printf("%3d   %11.1f  %7.1f  %7.1f   %5.2fx   %s\n",
			r.Groups, r.OpsPerSec, r.P50ms, r.P95ms, speedup, bar(r.OpsPerSec, maxV, 28))
	}
	fmt.Println()

	dir := "results"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log("✗ 建结果目录失败: %v", err)
		return
	}
	jf := filepath.Join(dir, "perf_shard_scaling.json")
	blob, _ := json.MarshalIndent(map[string]interface{}{
		"generated_at": time.Now().Format(time.RFC3339),
		"transport":    "in-memory labrpc (no real network/disk)",
		"window_sec":   perfWindow.Seconds(),
		"results":      results,
	}, "", "  ")
	if err := os.WriteFile(jf, blob, 0o644); err != nil {
		log("✗ 写 JSON 失败: %v", err)
		return
	}
	sf := filepath.Join(dir, "perf_shard_scaling.svg")
	if err := writeSVG(sf, results); err != nil {
		log("✗ 写 SVG 失败: %v", err)
		return
	}
	log("✓ 结果已落盘: %s / %s", jf, sf)

	// 结论判定用明确阈值，避免把 1.00x 的噪声说成"扩展有效"。
	top := results[len(results)-1]
	speedup := 0.0
	if base > 0 {
		speedup = top.OpsPerSec / base
	}
	switch {
	case speedup >= 1.5:
		log("场景 C 结论：分片带来有效横向扩展（%d 组 = 单组的 %.2fx），瓶颈成功从单个 Raft 组摊开",
			top.Groups, speedup)
	case speedup >= 1.15:
		log("场景 C 结论：分片有部分增益（%.2fx）但未线性，仍有共享瓶颈（单机 CPU / ShardMaster 查询 / 客户端并发不足）",
			speedup)
	default:
		log("场景 C 结论：分片未带来净增益（%.2fx）——瓶颈不在 Raft 组数。先查单次提交延迟是否被固定周期钉住（当前 p50=%.1fms，心跳间隔 110ms）",
			speedup, top.P50ms)
	}
}
