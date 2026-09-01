// migration.go —— 场景 D：多组（n_groups>1）分片迁移故障。
//
// 这是 ShardKV 最硬的路径：分片在组间迁移（rebalance）期间，系统仍必须保证
// 「已确认写零丢失、无脑裂双写」。此前 experiments 三个场景都只用单组（n_groups=1），
// 跨组迁移这个真实正确性盲区从未被任何演示 / 门禁覆盖。本场景：
//  1. 起 2 组 × 3 副本，预热 20 个 key（散落各分片）；
//  2. 注入组合故障：kill 第 1 组一个副本（剩 2/3 仍 quorum），并在 Churn 中段对第 0 组
//     注入真网络分裂 [0] | [1,2]（多数派 [1,2] 仍 quorum，避免停滞）——形成
//     「跨组 rebalance + 一组一副本崩溃 + 一组一副本网络分区」三重并发故障；
//  3. 并发客户端在 Churn 跨组漂移（40 轮、每 120ms 迁一个分片到另一组）期间持续写，
//     只记录每次被确认（OK）的写；
//  4. 迁移结束后等配置全推进 + 数据沉降，读回每个 key，断言：
//     - 被确认写过的 key 最终值 == 最后一次确认值（迁移零丢失写）；
//     - 从未被确认写过的 key 最终值 == 预热初值（初值也零丢失）。
// 落盘 results/client_view_migration.json，复用 I174 结构化产物 + I175 -assert 门禁模式。
package main

import (
	"fmt"
	"sync"
	"time"

	"raftkv/src/cluster"
	"raftkv/src/shardkv"
)

func runMigration() {
	const nG, nR, nSM, mrs = 2, 3, 3, 0
	const nKeys = 20
	resetClock()
	log("场景 D：多组（%d）分片迁移故障（跨组 rebalance 期间零丢失写 + 无脑裂）", nG)
	c := cluster.StartCluster(nG, nR, nSM, mrs)
	defer c.Cleanup()
	bootstrap(c, nG)
	ck := c.Clerk()

	// 预热：20 个 key 散落各分片，初值 init-<i>。
	initVal := make(map[string]string, nKeys)
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("m%d", i)
		v := fmt.Sprintf("init-%d", i)
		ck.Put(k, v)
		initVal[k] = v
	}
	base := c.Configs()
	log("初始配置数=%d；预热 %d key 完成，开始注入故障 + 跨组迁移", base[len(base)-1].Num, nKeys)

	// 故障注入：kill 第 1 组一个副本（模拟迁移期间副本崩溃）。
	// 该组剩 2/3 仍满足 quorum，迁移继续进行——这是「迁移 + 故障并发」的真实硬路径。
	c.KVs[1][nR-1].Kill()
	log("已 kill 第 1 组副本 g1-%d（该组剩 %d/%d 仍 quorum）", nR-1, nR-1, nR)

	// 并发客户端：Churn 期间持续以真实 client 身份写各分片；只记录被确认的写。
	var (
		mu     sync.Mutex
		lastAck = make(map[string]string)
		ops    int
		fails  int
	)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		seq := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			seq++
			k := fmt.Sprintf("m%d", seq%nKeys)
			val := fmt.Sprintf("v%d", seq)
			mu.Lock()
			ops++
			mu.Unlock()
			if err := ck.PutE(k, val); err == shardkv.OK {
				mu.Lock()
				lastAck[k] = val
				mu.Unlock()
			} else {
				mu.Lock()
				fails++
				mu.Unlock()
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// 触发跨组 churn：每 120ms 把下一号分片迁到另一组，制造可控的多组并发迁移。
	// 为模拟「迁移期间叠加网络分区」的最硬组合故障，把单次 churn 拆成三段，中段注入
	// 第 0 组的真网络分裂（[0] 与 [1,2] 不互通，多数派 [1,2] 仍 quorum，迁移不停滞），
	// 形成「跨组 rebalance + 第 1 组一副本崩溃 + 第 0 组一副本网络分区」三重并发故障。
	c.Churn(15, 120*time.Millisecond, 1)
	log("前段 churn 完成；注入第 0 组网络分区 [0] | [1,2]（多数派 [1,2] 仍 quorum）")
	partStart := time.Now()
	c.PartitionKV(0, []int{0}, []int{1, 2})
	c.Churn(15, 120*time.Millisecond, 1)
	c.PartitionKV(0) // 愈合第 0 组网络分区，恢复全连通
	partitionMs := time.Since(partStart).Milliseconds()
	log("中段 churn（分区活跃 ~%dms）完成；已愈合第 0 组网络分区", partitionMs)
	c.Churn(10, 120*time.Millisecond, 1)
	close(stop)
	wg.Wait()

	// 等所有 group 配置推进到最新，确保迁移收敛（配置应用 ≠ 数据完全搬运，故再给沉降窗口）。
	latest := c.Configs()
	latestNum := latest[len(latest)-1].Num
	c.WaitAllConfigs(latestNum)
	log("迁移完成，最终配置数=%d；等待分片数据沉降…", latestNum)
	time.Sleep(1500 * time.Millisecond)

	// 断言：每个 key 最终读回必须 == 最后一次确认写（或初值，若从未被确认写）。
	mu.Lock()
	acked := make(map[string]string, len(lastAck))
	for k, v := range lastAck {
		acked[k] = v
	}
	totalOps, totalFails := ops, fails
	mu.Unlock()

	lost := 0
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("m%d", i)
		want := initVal[k]
		if v, ok := acked[k]; ok {
			want = v
		}
		if got, ok := getStable(ck, k); !ok {
			log("✗ [migration] 迁移后读 %s 持续失败（视为丢失写）", k)
			lost++
		} else if got != want {
			log("✗ [migration] 迁移后 %s 值漂移: want=%q got=%q（已确认写丢失！）", k, want, got)
			lost++
		}
	}
	if lost == 0 {
		log("✓ [migration] 跨组迁移 + 副本崩溃 + 网络分区三重故障期间：所有被确认写零丢失（%d 请求 / %d 失败 / 分区窗口 %dms），无脑裂双写",
			totalOps, totalFails, partitionMs)
	}

	// I174/I175：落盘结构化报告，供控制台渲染 + -assert 门禁强制不变量。
	// DownMs 这里记为网络分区窗口（三重并发故障中唯一「非 quorum 保留」的中断段），
	// 与 leader/partition 场景的停机口径一致，便于控制台横向对比硬故障强度。
	writeClientViewJSON("client_view_migration.json", clientViewReport{
		Scenario:    "migration",
		GeneratedAt: time.Now().Format(time.RFC3339),
		Ops:         totalOps,
		Fails:       totalFails,
		LostWrites:  lost,
		DownMs:      float64(partitionMs),
		Conclusion:  fmt.Sprintf("跨组迁移+副本崩溃+网络分区：%d 请求/%d 失败/%dms 分区窗口，丢失写=%d（ok=%v）", totalOps, totalFails, partitionMs, lost, lost == 0),
		Ok:          lost == 0,
	})

	if lost > 0 {
		log("场景 D 结论：⚠ 迁移期间出现 %d 处丢失写（系统 bug 或本次并发边界触发）", lost)
		return
	}
	log("场景 D 结论：2 组 ×3 副本在「跨组 rebalance + 一组一副本崩溃 + 一组一副本网络分区」三重并发故障下，已确认写全部存活、无脑裂双写——ShardKV 最硬路径的正确性被客户端视角实证。")
}

// getStable 轮询读一个 key，给分片迁移数据沉降留窗口：迁移刚完成、分片数据尚未
// 完全搬运到新 owner 时，读可能短暂失败（ErrWrongGroup / 超时）。最多重试 ~5s，
// 期间配置与数据应已收敛；仍读不到才视为丢失写（避免把迁移时延误判为数据丢失）。
func getStable(ck *shardkv.Clerk, k string) (string, bool) {
	for attempt := 0; attempt < 50; attempt++ {
		if v, err := ck.GetE(k); err == shardkv.OK {
			return v, true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", false
}
