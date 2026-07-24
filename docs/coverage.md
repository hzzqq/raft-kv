# 测试覆盖率报告（coverage）

> 由 `make cover`（即 `go test ./... -coverprofile=cover.out -covermode=atomic`）生成。
> `cover.out` 是本地产物（已被 `.gitignore` 的 `*.out` 忽略），本文记录最近一次全量运行的**数值快照**。

## 总覆盖率

```
total:  (statements)  74.2%
```

> 说明：6.824 起始代码（尤其 `src/raft` 的选举/快照边界分支、`src/shardkv` 的迁移异常路径）存在大量防御性分支与难以触发的边界，是覆盖率未达更高的主要来源；核心读写与迁移主链路均已覆盖。

## 分包覆盖率

| 包 | 覆盖率 | 说明 |
|----|-------:|------|
| `src/cluster` | 93.2% | 可复用 in-process 集群 harness，路径少且全被测试/演示/网关覆盖 |
| `src/demo` | 93.0% | 全栈演示，两条路径（Clerk + HTTP 网关）都被 `TestRunDemo` 跑通 |
| `src/kvraft` | 84.4% | Lab 3 单组 KV，主链路 + 快照路径覆盖良好 |
| `src/metrics` | 84.7% | 零依赖指标库，Counter/Histogram/Registry 均有单测 |
| `src/shardmaster` | 76.5% | 配置服务，Join/Leave/Move/Query 主链路覆盖 |
| `src/raft` | 77.8% | 共识核心，选举/复制/快照主链路覆盖；少数边界分支（脑裂恢复、快照截断）未触发 |
| `src/shardkv` | 66.5% | 数据面最复杂：分片路由 + 迁移状态机 + ReadIndex；部分迁移异常/冻结路径（见 `lab4-shardkv-design.md §7`）未被测试覆盖 |
| `src/gateway` | 66.7% | HTTP 网关，读写/健康检查/指标/调试端点均覆盖；错误分支（504/503 映射）有 `TestGatewayFailFast` |
| `src/kvcli` | 54.1% | HTTP 客户端 + CLI；`bench` 子命令与错误路径覆盖较弱（CLI 参数解析分支多） |

> 时效性说明：上表数值为「可观测性/韧性收口（#212–#226）」**之前**的一次全量快照。
> 该推送为 `util.WorkerPool`(#217)、`kvcli` 批量扇出(#218)、`raft`/`shardkv` 健康快照(#219–#220)、
> 网关 `/debug/raft`(#221)/`raft_min_health_score`(#222)、`kvraft` 状态机可观测(#223) 等新增了
> 大量 **cluster-free 单测**（见下），故当前真实覆盖率应高于上表。重新生成请用 `make cover`。

## 近期新增 cluster-free 单测（#212 起，不触发进程内 Raft 选举，规避 Windows `time.Now()` 分辨率 flaky）

| 测试文件 | 覆盖的增量能力 |
|----|------|
| `src/util/worker_pool_test.go` | `TrySubmit`(非阻塞) / `SubmitCtx`(ctx 取消) / 停止无 panic |
| `src/kvcli/client_batch_concurrency_test.go` | `MGet`/`MSet` 有界并发 + ctx 取消不挂死 |
| `src/raft/raft_status_test.go` · `raft_metrics_test.go` | `Raft.Status()` 只读快照 · 共识层计数 |
| `src/diagnostics/diagnostics_selfcheck_test.go` | `RaftCheck` 不变量自检（健康 / commit 越界 / apply 越界 / leader 无租约） |
| `src/shardkv/raft_status_test.go` | `ShardKV.RaftStatus()` 透出底层共识健康 |
| `src/gateway/gateway_debug_raft_test.go` · `gateway_raft_health_metric_test.go` · `gateway_proc_metrics_test.go` · `gateway_concurrency_test.go` | `/debug/raft` 汇聚 · `raft_min_health_score` · 进程级 gauge · 并发信号量 |
| `src/shardmaster/shardmaster_metrics_test.go` · `src/transport/transport_metrics_test.go` | 控制面 / 框架层指标埋点 |
| `src/kvraft/kvraft_status_test.go` | `KVServer.Status()` + GC 计数（**注：该文件尚未提交，待 go 工具链验证后入册**） |

## 如何复现

```bash
# 本地（无需 gcc）：
make cover
# 或等价：
go test ./... -count=1 -timeout 900s -coverprofile=cover.out -covermode=atomic
go tool cover -func=cover.out | tail -1

# HTML 报告（按函数级着色，定位未覆盖行）：
go tool cover -html=cover.out
```

CI 的 `coverage` job 也会跑同样的命令并把 `cover.out` 作为 artifact 上传，可在 Actions 页面下载查看。

## 提升方向（供后续迭代）

1. `src/kvcli`（54.1%）最薄弱：补 `bench` 子命令的单测与 CLI 错误路径，预计可拉到 70%+。
2. `src/shardkv`（66.5%）：针对 `pendingIn/pendingOut` 冻结的异常路径补「恢复后自愈」用例（与 §7 根因修复配套），可同时提升覆盖率与鲁棒性。
3. `src/raft`（77.8%）：补快照截断 / 日志压缩边界的单测。
