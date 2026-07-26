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

> 时效性说明：上表数值为较早一次全量快照（早于 #227–#234 的可观测/文档收口）。
> #212–#226 推送为 `util.WorkerPool`(#217)、`kvcli` 批量扇出(#218)、`raft`/`shardkv` 健康快照(#219–#220)、
> 网关 `/debug/raft`(#221)/`raft_min_health_score`(#222)、`kvraft` 状态机可观测(#223) 新增了大量
> **cluster-free 单测**；#227–#234 又追加 `kvraft` 状态机可观测收口(#228，含 `kvraft_status_test.go`
> 已于 #228 提交 `8e93ef7`)、`shardkv` 迁移积压 gauge(#229)、`diagnostics.ShardCheck` 数据面自检(#230)
> 以及文档同步(#231–#234，含新增 `docs/observability.md` 统一指标总览）。故当前真实覆盖率应高于上表。
> 重新生成请用 `make cover`。

## 免 Go 自检门禁（scripts/）

仓库在自驱迭代（cycle 68 起）沉淀了一套**不依赖 Go 工具链**的纯 Python 静态校验器，
统一由 [`scripts/check_all.py`](../scripts/check_all.py) 编排、CI `docs-links` job 与 `make docs`
复跑，并由 `scripts/pre-commit.sh`（`make hooks` 安装）在提交前阻断漂移。它本身就是
本覆盖率快照**之外**的工程化收口，由 3 个 meta 校验器守护自身一致性：

| 校验器 | 守护维度 |
|---------|----------|
| `check_md_links.py` | 全仓 Markdown 内部链接 / 锚点可解析 |
| `check_docs_endpoints.py` | 网关 18 端点 + kvcli 4 CLI 子命令 与文档一致 |
| `check_metrics_docs.py` | 指标注册名（51 个）与文档一致 |
| `check_api_docs.py` | `kvcli.Client` 32 方法 + `util` 16 类型 与文档一致 |
| `gen_changelog.py --verify` | `CHANGELOG.md` 与迭代日志同步 |
| `check_state_integrity.py` | 自驱开发日志（state.json）完整性 |
| `check_doc_inventory.py` | 校验器套件自身接线一致性（meta） |
| `check_coverage_doc.py` | 本文件（coverage.md）与校验器清单一致（meta） |
| `check_go_patterns.py` | `ioutil.` / 非测试 `time.After` 反模式 |
| `check_godoc.py` | 导出 `type` / 包级 `func` / 对外可见 `method` 必须具备 `//` 文档注释（go doc 可见性） |
| `check_test_coverage.py` | 免 Go 测试纪律护栏：包级测试缺口 + 导出符号未引用提示（软提示） |
| `check_hooks_installed.py` | 提交门禁钩子安装状态：`.git/hooks/pre-commit` 须存在/可执行/与源一致（防门禁静默失效） |
| `gen_test_coverage.py --verify` | 校验本文件「模块↔测试」自动生成表与实际一致（防 drift） |

> 数值覆盖率快照仅为「信息性」参考；上述门禁才是本仓**持续保证**代码/文档不漂移的机制。
> 本机交互 shell 默认无 `go`（且托管 Go 无 gcc，故 `go test -race` 仅能在 CI 侧跑），
> 故这类免 Go 检查器是本地验证的主力；Go 侧 `vet/test/race/cover` 见 CI 各 job。


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
| `src/kvraft/kvraft_status_test.go` | `KVServer.Status()` + GC 计数（已于 #228 提交 `8e93ef7`） |

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

## 相关文档

- [`docs/observability.md`](observability.md) —— 指标目录与各子系统单测的对应关系（含 `raft_*` / `kv_*` / `shardkv_*` / `sm_*` / `gateway_*` 指标来源），提交覆盖前后均可据此核对可观测性是否随单测同步增长。
- [`docs/architecture.md`](architecture.md) · [`docs/usage.md`](usage.md) · [`docs/runbook.md`](runbook.md) —— 架构 / 使用 / 运维排障。

## 模块 ↔ 测试映射（自动生成）

<!-- test-coverage-table:start -->

_本表由 `scripts/check_test_coverage.py` 自动生成（免 Go 扫描），成对标记之间的内容请勿手工编辑，改代码后运行 `make test-cov` 刷新。_

| 模块 (包) | 源码文件 | 测试文件 | 有测试 | 高信号未覆盖导出符号 |
|-----------|---------:|--------:|:------:|---------------------:|
| `cluster` | 1 | 1 | ✅ | ConfigNum |
| `demo` | 1 | 4 | ✅ | — |
| `diagnostics` | 2 | 3 | ✅ | — |
| `gateway` | 6 | 18 | ✅ | Flush, LoadGatewayConfig, SetCORS, SetHTTPServer, GroupStatus, GroupView, RaftStatusView |
| `kvcli` | 16 | 25 | ✅ | MSetCtx, BatchResult, BenchResult, MDelResult, MSetResult |
| `kvraft` | 1 | 4 | ✅ | OpResult |
| `metrics` | 4 | 8 | ✅ | Desc, SetDesc |
| `raft` | 3 | 9 | ✅ | Call, CondInstallSnapshot, RaftStateSize, Send, String, RpcMsg |
| `shardkv` | 15 | 23 | ✅ | GetE, PutE, MigrationPlan, MigrationStep |
| `shardmaster` | 14 | 17 | ✅ | String, ConfigDelta, PlanResult |
| `statusfmt` | 1 | 4 | ✅ | — |
| `transport` | 2 | 13 | ✅ | Target, ClientConn, ClientStats, JSONCodec, ErrClosed, ErrMethodNotFound |
| `util` | 24 | 28 | ✅ | ExpBackoff, MarshalJSON, CbState |
| `version` | 1 | 1 | ✅ | — |

> 汇总：14 个包，0 个无测试；未引用导出符号 func=19 / type=17 / var=2 / const=0。「未覆盖符号」为软提示，可能含被间接覆盖的结果/视图类型。

<!-- test-coverage-table:end -->
