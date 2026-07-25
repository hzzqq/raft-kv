# 端到端演示（`src/demo`）

[`src/demo/main.go`](../src/demo/main.go) 是 raft-kv 的**开箱即跑**端到端样例：在进程内用可复用的
[`cluster`](../src/cluster/cluster.go) 包拉起一个多副本组 ShardKV 集群，分两段验证「集群 → HTTP → 客户端」
全栈链路。它也是 CI `demo` job（`go run ./src/demo`）的固定冒烟用例，用于防止进程内集群 / 网关 /
分片迁移 / 指标链路被回归破坏。

> 注意：演示依赖**内存 labrpc 网络**（与单测同一套），因此集群是进程内的；生产部署需替换为真实网络
> 传输层（gRPC / TCP，见 [`src/transport`](../src/transport/transport.go)）。

## 运行方式

```bash
# 方式一：直接用托管 Go 工具链运行源码
go run ./src/demo

# 方式二：仓库根目录已预编译好的二进制（Windows / Linux 通用）
./demo            # Linux / Git Bash
start demo        # 或直接双击（Windows）
```

环境变量：

| 变量 | 作用 |
|------|------|
| `RAFT_KV_DEMO_QUIET` | 设置后跳过启动诊断与结果回显，仅退出码反映成败（CI 静默模式） |

退出码：`0` = 两段演示均通过；非 `0` = 任一路径失败（HTTP 监听错误等）。

## 演示流程

### 1) 启动诊断（cluster-free）

`main` 先调用 `CollectStartupReport()` 采集运行期环境（Go 版本 / OS / Arch / CPU / GOMAXPROCS / CWD）
并做两项轻量自检：**临时目录可写**、**分片数常量 `shardmaster.NShards > 0`**。两项均不启动集群、不发网络，
纯本地能力探测，便于在真正拉起内存集群前暴露环境问题。报告由 `FormatStartupReport()` 渲染为
`[demo-diag] ...` 多行文本。

### 2) 进程内 KV 路径

`StartCluster(2, 3, 3, 0)` 启动 **2 个副本组、每组 3 副本** 的集群，经 `Clerk` 直接做：

- `Put("hello","world")` / `Get` → 验证基础读写；
- `Append("hello","!")` / `Get` → 验证追加；
- `Move(hello 所在分片, group1)` + 等待配置收敛 → **跨 group 分片迁移**，再 `Get` 验证数据随分片迁移且可读。

演示期间通过 `metrics.StartPeriodicReporter` 每 400ms 把 [`shardkv.Metrics`](../src/shardkv/shardkv.go)
快照 dump 到 stderr，展示周期性可观测能力。

### 3) 全栈 HTTP 路径

以本进程集群的 `Clerk` 构造一个与 [`src/gateway`](../src/gateway/gateway.go) 语义一致的 HTTP handler
（复用 [`demoGatewayHandler`](../src/demo/main.go)），监听 `127.0.0.1:0`（随机端口），验证：

| 端点 | 方法 | 验证点 |
|------|------|--------|
| `/kv/{key}` | GET | 经 HTTP 读回 `dkey` |
| `/kv/{key}` | PUT | 经 HTTP 写入 `dkey=dval` |
| `/kv/{key}/append` | POST | 经 HTTP 追加 `-http` |
| `/healthz` | GET | 启动后轮询探活（`waitHealth` 指数退避，上限 ~4s） |
| `/metrics` | GET | 返回 `shardkv.Metrics.Snapshot()` 的 JSON，`counters` 非空 |

请求全程带 `context` 超时（整体 15s、单请求 5s），并在退出时 `srv.Shutdown` 优雅关闭等待在途请求完成。

## 验证结论

`RunDemo()` 返回结构化摘要字符串，断言两类事实：

- **进程内路径**：`Put/Get`、`Append/Get`、迁移后 `Get` 三处读回值符合预期；
- **全栈路径**：HTTP `put` 成功、`get` 读回 `dval`、追加后读回 `dval2`，且 `/metrics` 解析为合法 JSON
  （含 `counters`）。

任一不符即说明「集群复制 / 分片迁移 / 网关路由 / 指标暴露」某一环被回归，CI 会直接失败。

## 与文档体系的关联

- 集群底座：[`src/cluster`](../src/cluster/cluster.go)（被 demo / gateway / kvcli 复用）
- HTTP 网关生产形态：[网关用法](../docs/usage.md)
- 指标语义与 Prometheus 暴露：见[可观测性手册](../docs/observability.md)与[排障 runbook](../docs/runbook.md)
- 分片迁移健康与卡滞排查：见[runbook §7](../docs/runbook.md)
