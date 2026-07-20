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
