# 性能基准与基线（T-148，2026-09-19 落档）

> ⚠️ **数字绑定机器环境，跨机器不可直接比较**。本页全部基线为下述开发机实测
> （`go test -bench`，labrpc 内存网络口径）。性能对比必须在**同一台机器、同一口径**
> 下做改动前后对照，方法见 §4；机器差异、后台负载、CPU 调度都会造成两位数百分比漂移。

## 1. 基准资产与运行命令

基准按「三层对照」组织，用于定位开销大头（网关管线 → 共识全栈 → 内存地板）：

| 层 | 文件 | 基准 | 口径 |
|---|---|---|---|
| 网关数据面（cluster-free 微基准） | `src/gateway/gateway_bench_test.go` | `GatewayGetCacheHit` / `GatewayGetCacheHitGzip` / `GatewayGetCacheMiss` / `GatewayGetCacheMissGzip` / `GatewayGetETag304` / `GatewayPutPath` | 直接驱动 `s.Wrap` 管线（无 TCP），缓存+ETag 生产双开；每请求新建 Recorder 属固定 harness 开销（各基准同口径） |
| 端到端集群 | `src/gateway/gateway_cluster_bench_test.go` | `GatewayClusterPut` / `GatewayClusterGet` | 1×3 进程内集群（labrpc）+ loopback HTTP 真实往返；PUT 生产口径（含写失效护栏），GET 关缓存/ETag 隔离共识读路径 |
| 对照地板 | 同上 | `MemoryMapPut` | 互斥锁 map 直写，无共识无 HTTP |
| KV 层（既有） | `src/shardkv/bench_test.go` | `SKVPutGet` / `SKVGet` | shardkv Clerk 直连（无 HTTP 层；Clerk 每操作查询 ShardMaster 配置，口径偏慢，见源码注记） |
| 共识层（既有） | `src/raft/bench_test.go` | `RaftAgree` | 3 副本日志达成一致 |
| 微观测件（既有） | `src/metrics` / `src/util` / `src/transport` 各 `*_bench_test.go` | `CounterInc` 等 11 个 | 纯函数级 |

运行命令（benchtime 单基准 1s 级即可复现表列量级）：

```bash
# 全量（≈2 分钟；含拉集群的端到端基准）
go test ./src/... -run '^$' -bench . -benchtime 1s

# 仅网关数据面微基准
go test ./src/gateway/ -run '^$' -bench 'BenchmarkGatewayGet|BenchmarkGatewayPut' -benchtime 1s

# 仅端到端 + 地板
go test ./src/gateway/ -run '^$' -bench 'BenchmarkGatewayCluster|BenchmarkMemoryMap' -benchtime 1s

# 对比用：改动前后各跑一次多轮
go test ./src/<pkg>/ -run '^$' -bench <名称> -benchtime 1s -count 5 > bench-after.txt
```

注：`make bench` / `make bench-update` 跑同一套基准并经 `scripts/check_bench_regression.py`
与 `scripts/bench-baseline.json` 比对（`-benchtime=1x` 快扫口径，数字仅供门禁，不作基线）。

## 2. 基线机器环境

| 项 | 值 |
|---|---|
| CPU | AMD Ryzen 7 7840HS（8 核 16 线程） |
| 内存 | 23.7 GB |
| OS | Windows 10 专业版（10.0.19045） |
| Go | go1.22.5 windows/amd64 |
| 网络口径 | labrpc 内存 RPC + loopback HTTP（非真实 TCP 传输层） |
| 落档日期 | 2026-09-19 |

## 3. 基线数字（`-benchtime 1s` 单次运行）

### 3.1 网关数据面微基准（cluster-free，响应体 ≈504B 文本）

| 基准 | ns/op | B/op | allocs/op | 解读 |
|---|---:|---:|---:|---|
| `GatewayGetCacheHit` | 5,648 | 8,811 | 50 | 缓存命中回放，热路径最快整包口径 |
| `GatewayGetCacheHitGzip` | 6,333 | 8,783 | 54 | gzip 变体键回放（回放预压缩字节，不重压缩） |
| `GatewayGetCacheMiss` | 8,194 | 12,340 | 69 | miss = 命中 + SHA256 ETag + 落缓存（+45% ns） |
| `GatewayGetCacheMissGzip` | 290,431 | 1,643,422 | 115 | **压缩税 ≈35×**，见 §3.4 优化机会 |
| `GatewayGetETag304` | 5,265 | 7,374 | 47 | 304 短路最快（无 body 回写、不占信号量），较整包命中省 ~7% ns 与 ~16% 内存 |
| `GatewayPutPath` | 6,535 | 8,055 | 52 | PUT 全管线 + 写失效护栏（双变体键构造 + 双锁对 + invalGen 递增） |

### 3.2 端到端与对照层

| 基准 | ns/op | B/op | allocs/op | 解读 |
|---|---:|---:|---:|---|
| `GatewayClusterPut` | 2,678,078 | 4,611,064 | 687 | HTTP 全栈 + 3 副本共识提交，单客户端串行 |
| `GatewayClusterGet` | 173,326 | 880,266 | 209 | 共识读快路径（读守卫免日志提交），较 PUT 快 ~15× |
| `MemoryMapPut` | 24.1 | 1 | 0 | 内存地板 |
| `SKVPutGet`（shardkv Clerk 直连） | 6,473,598 | — | — | 含每操作 ShardMaster 配置查询（源码注记的已知低效） |
| `SKVGet`（Clerk 直连） | 6,595 | — | — | 配置已缓存在 Clerk 侧会话时的读口径 |
| `RaftAgree`（3 副本） | 295,454,500 | — | — | 单次 agreement 含多副本等待，n 仅 4，量级参考 |

**放大倍数**：`GatewayClusterPut` / `MemoryMapPut` ≈ **111,000×**（共识 + 持久化序列化 +
HTTP 全栈 vs 内存直写）；读路径（免提交）与写路径相差一个多数量级——后续优化优先级
应先看写路径的持久化与序列化成本。

### 3.3 既有微基准快照（同机同时刻，供后续对照）

| 基准 | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `CounterInc` | 9.4 | 0 | 0 |
| `GaugeSet` | 1.7 | 0 | 0 |
| `HistogramRecord` | 70.0 | 145 | 0 |
| `GaugeVecWithLabelValues` | 20.6 | 8 | 1 |
| `CounterVecWithLabelValues` | 20.5 | 8 | 1 |
| `LRUGet` | 22.0 | 0 | 0 |
| `LRUPut` | 50.2 | 19 | 2 |
| `SemaphoreAcquireRelease` | 203.3 | 0 | 0 |
| `GobCodecMarshal` | 1,286 | 1,856 | 25 |
| `GobCodecUnmarshal` | 9,345 | 7,512 | 186 |
| `GobCodecRoundTrip` | 25,872 | 18,497 | 422 |

### 3.4 基线即发现的优化机会

1. **gzip 压缩税（明确优化方向）**：`GatewayGetCacheMissGzip` 290µs / 1.63MB allocs 每
   请求——wrap 对每个 gzip 请求新建两个 `gzip.Writer`（响应压缩 + 缓存落存重压缩，
   见 `gateway.go` wrap 内 `gzip.NewWriter` 两处），无池化复用。`sync.Pool` 复用
   压缩器是量化收益最直接的第一刀。
2. **Clerk 配置查询**：`SKVPutGet` 慢于 `GatewayClusterPut`（同为单客户端 PUT 共识）
   即源于此既有已知低效（见 `src/shardkv/bench_test.go` 注记），优化时以二者差值为准绳。
3. **kvcli 不单列 RTT 基准（决策）**：HTTP 往返口径已由 ClusterPut/ClusterGet 端到端
   覆盖，kvcli 仅叠加 JSON 编解码与重试/缓存逻辑（正确性单测已覆盖）；单独微基准
   无网络语义，量化价值低，暂不建设。

## 4. 对比方法（如何判断「性能回退」）

1. **同机同口径**：改动前后各跑 `go test -bench <名称> -benchtime 1s -count 5`，
   输出到文件后用 [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat)
   对比（`go install golang.org/x/perf/cmd/benchstat@latest`）；未装 benchstat 时
   手工对比 `-count 5` 各轮的**中位数**。
2. **噪声口径（本机 -count=3 实测）**：微基准同时程轮次波动 ≤7%，跨进程可到 ~12%
   （如 CacheMiss 8.2k→9.2k ns/op）；端到端集群基准波动 ~4%。据此：
   - **<10%**：视为噪声，不下结论；
   - **10–20%**：加长 benchtime（2–3s）+ 增多 count 复测后再判；
   - **>20% 且可复现**：判定真回退，回到改动找原因。
3. **优先看 allocs/op 与 B/op**：分配计数比 ns/op 稳定一个量级（本基线各基准轮次间
   allocs 几乎不变）；allocs 翻倍几乎必然是真实回退。
4. 快扫门禁（`make bench`，`-benchtime=1x`）的数字与 §3 表不可比，仅用于回归比对流程。

## 5. 防回归门禁决策（为何不接硬阈值）

`scripts/check_bench_regression.py` 的机制是：解析 `go test -bench` 文本输出，与仓库
提交的 `scripts/bench-baseline.json` 按**基准名**比对，任一基准 ns/op 超阈值（默认 10%）
即失败；不在基线中的新基准仅报告不门禁。经评估，本轮**不把任何基准写入基线 JSON**
（该文件自 cycle 135 随检查器落库以来即为空，处于 report-only 模式，本轮维持），理由：

1. **绝对 ns/op 绑定采集机器**：`compare()` 按仓库提交的绝对数值判定，换机器、换
   CI runner、换负载环境必然大面积误报——对本仓「无固定专用 CI 机器」的现状是
   纯 flaky 门禁；
2. **端到端集群基准抖动更大**（选举/GC/调度），10% 硬阈值尤其不可靠；
3. 本轮新增基准全部是此类跨机器敏感的端到端/管线口径，恰属最不宜硬门禁的一类。

**替代方案即本页**：§2 环境快照 + §3 基线数字 + §4 同机对比方法与判断线，把
「性能不静默回退」的判断依据落到文档而非 flaky 断言。`make bench` 保留观测能力
（空基线时仅打印当前数字，不阻断）。若未来出现固定性能 CI 机器，可在该机器上用
`make bench-update` 重建基线后再启用硬门禁——机制已就绪，只欠稳定环境。
