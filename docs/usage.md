# 使用指南：全栈组件（cluster / demo / gateway / kvcli / metrics）

本仓库在 Lab 4 的 `raft` / `shardmaster` / `shardkv` 核心之上，额外提供了一组
「可直接跑起来」的上层组件，用于演示与压测整条链路：

```
cluster 包 ──► demo（端到端演示）
     │
     └──────► gateway（HTTP REST 网关，自带进程内集群）
                  │
                  └──► kvcli（HTTP 客户端 / 压测工具）
```

> 说明：gateway / demo 自带一个**进程内** ShardKV 集群（复用实验用的内存
> `labrpc` 网络），因此是「自包含演示」。生产部署应由网关连接一组独立部署、
> 走真实网络传输（gRPC / TCP）的 ShardKV 节点，而非本文件的进程内集群。

---

## 1. cluster 包（`src/cluster`）

把 `shardkv_test.go` 里「内存 labrpc 网络 + ShardMaster 集群 + 多 replica group」
的搭建逻辑抽成独立、可 import 的包，供 demo / gateway / kvcli 上层组件复用。

```go
c := cluster.StartCluster(2, 3, 3, 0) // nGroups, nReplicas, nSM, maxraftstate
defer c.Cleanup()

ck := c.Clerk()
c.Join(0); c.WaitConfig(0, 0, 1)   // 加入第 0 组（gid=1），等其配置推进到 v1
c.Join(1); c.WaitConfig(1, 0, 2)

ck.Put("hello", "world")
ck.Append("hello", "!")            // -> "world!"
c.Move(key2shard("hello"), 1)     // 把分片跨组迁移到 gid=2
c.WaitConfig(0, 0, 3); c.WaitConfig(1, 0, 3)
time.Sleep(500 * time.Millisecond) // 等迁移完成
ck.Get("hello")                    // -> "world!"（数据随分片迁移）
```

主要 API：

| 方法 | 作用 |
|------|------|
| `StartCluster(nGroups, nReplicas, nSM, maxraftstate)` | 启动完整内存集群 |
| `Clerk()` | 返回绑定到本集群 ShardMaster 的 `shardkv.Clerk` |
| `Join(g)` / `Leave(g)` / `Move(shard, g)` | 配置变更（组下标，内部 +1 作 gid） |
| `ConfigNum(g, r)` / `WaitConfig(g, r, num)` | 读取 / 轮询某副本生效配置版本 |
| `Cleanup()` | 关闭所有节点并回收 goroutine |

---

## 2. demo（`src/demo`）

一次性演示「进程内 KV 路径」与「全栈 HTTP 路径」两段：

```bash
go run ./src/demo
# 或：make demo   （先构建二进制再运行）
```

输出示例（摘要）：

```
demo result: inproc Put/Get="world" Append/Get="world!" after-move Get="world!" |
            http put=true get="dval" append get="dval-http" metrics-ok=true
```

- **进程内路径**：直接用 `Clerk` 做 Put/Get/Append + 跨 group 分片迁移。
- **全栈 HTTP 路径**：以本进程集群的 `Clerk` 起一个真正的 HTTP 网关，客户端经
  HTTP 做 Put/Get/Append，并拉取 `/metrics`，证明 `cluster → HTTP → client` 全栈打通。

---

## 3. gateway（`src/gateway`，HTTP REST 网关）

自带进程内集群（默认 2 个 group），把 ShardKV 暴露成 REST 接口：

```bash
go run ./src/gateway                 # 监听 :8080
go run ./src/gateway :9090           # 自定义地址
# 或：make build-binaries && ./bin/gateway
```

| 方法 & 路径 | 作用 |
|------|------|
| `PUT /kv/{key}` | 写入 `key = body` |
| `GET /kv/{key}` | 读取 `key` 当前值 |
| `POST /kv/{key}/append` | 把 body 追加到 `key` 当前值之后 |
| `GET /healthz` | 健康检查（200） |
| `GET /metrics` | 返回 `shardkv.Metrics` 的 JSON 快照（counters + 直方图分位数） |
| `GET /status` | 集群健康总览（JSON `ClusterStatus`）：每 group leader/config/持有/待收/待迁/孤儿中转计数 + 卡滞秒数 + 整体 `healthy` 标志（卡滞 >2s 判冻结），供监控/告警轮询 |
| `GET /debug/migrate` | 纯文本迁移进度（每 group leader 副本的 pendingIn/pendingOut/incoming 分布 + 集群最新 config 号），供线下排障 |
| `POST /debug/migrate-plan` | 配置变更 **dry-run** 预览：提交 `current` 配置 + `PlanOp`（Join/Leave/Move），返回目标配置/结构错误/演进错误/迁移步骤（`shardmaster.Plan` 在内存模拟，不触碰 Raft）。运维提交前安全评估迁移代价与风险 |

`Handler()` 返回 `http.Handler`，便于用 `httptest` 做单测而无需绑定端口。

**可观测响应头（经 `wrap` 统一注入，所有路由自动受益）**：

- `X-Process-Time`：服务端处理耗时（TTFB 口径，毫秒，三位小数），用于定位慢路径。
- `X-Response-Size`：响应体「线上」字节数（gzip 开启时为压缩后字节）；直方图指标 `gateway_response_bytes`。
- `X-Request-Size`：入站请求体声明大小（`Content-Length`）；分块上传（`-1`）跳过以避免无意义负值，便于识别超大请求。

**并发与容量**：`max_concurrent`（YAML，默认 64）限制网关在途请求数，超出返回 `429 too many concurrent requests`（满即拒，不阻塞在途请求）；实时占用见 `gateway_concurrent_in_use` 指标。实现基于 `util.Semaphore`，与 `kvcli` 的并发上限复用同一原语。

**构建信息自报**：`/debug/version`（及 `version.String()`/`Short()`）未注入 `-ldflags` 时，自动从 `runtime/debug.ReadBuildInfo()` 补全 `commit`/`build_time`，便于排障定位构建来源（`version.LoadFromBuildInfo()` 在启动期调用一次）。

---

## 4. kvcli（`src/kvcli`，HTTP 客户端 / 压测工具）

```bash
go run ./src/kvcli [-addr http://localhost:8080] get KEY
go run ./src/kvcli [-addr http://localhost:8080] put KEY VALUE
go run ./src/kvcli [-addr http://localhost:8080] append KEY VALUE

# 端到端压测（默认 mixed，1000 次，8 并发）
go run ./src/kvcli [-addr http://localhost:8080] bench
go run ./src/kvcli [-addr http://localhost:8080] bench get 2000 16
go run ./src/kvcli [-addr http://localhost:8080] bench put 500 4
# 语法：bench [op=get|put|mixed] [ops] [workers]
```

`bench` 启动 `workers` 个并发客户端，共执行 `ops` 次指定操作，报告吞吐
（`ops/sec`）与延迟分位数（`p50/p95/p99`，毫秒）。每个 worker 操作独立 key 命名空间，
保证 `mixed`/`get` 下读到的都是本 worker 写入的数据。客户端对非 200 响应会返回错误
（不会静默返回空串）。

作为 Go 库使用时，`Client` 还提供：

- `MGet(keys)` / `MSet(pairs)`：并发批量读/写，单 key 失败互不阻断（结果里 `Errors` 归集失败项）。
- `SetMaxConcurrent(n int)`：限制 `MGet`/`MSet` 内部并发回源 goroutine 数（默认 `0`=不限制，保留历史语义；非 0 时仅该数量请求同时在途，复用 `util.Semaphore`，ctx 取消时立即退出不挂死），防止超大批量一次性拉起成千上万 goroutine 打爆客户端/后端。

---

## 5. 可观测性（metrics，`src/metrics`）

零依赖、并发安全的轻量指标库：计数用原子操作、直方图用有界环形缓冲，热路径开销可忽略。
已在 `raft` / `kvraft` / `shardkv` 三个热路径接入（纯增量原子操作，不改变任何行为）：

- `shardkv.Metrics`：`op_latency_ms`、`ops_total`、`ops_errors`、`entries_applied`、
  `snapshots_installed`、`snapshots_taken`
- `kvraft.Metrics`：`op_latency_ms`、`ops_total`、`ops_errors`、`entries_applied`、`snapshots_installed`
- `raft.Metrics`：`leader_changes`、`log_applied`、`snapshots_installed`

用法：

```go
reg := metrics.NewRegistry()
reg.Counter("ops_total").Inc()
reg.Histogram("op_latency_ms").Record(12.5)
snap := reg.Snapshot() // map[string]interface{}，可直接 json 序列化
// {"counters": {"ops_total": 1}, "histograms": {"op_latency_ms": {"count":1,"p50":12.5,...}}}
reg.Reset()            // 跨用例重置，避免进程级指标累积
```

网关的 `GET /metrics` 直接序列化 `shardkv.Metrics.Snapshot()`，便于接入外部监控。
新增 `shard_migration_ms` 直方图记录分片入站迁移端到端耗时（从 `pendingInSince` 起算，
到 `applyInstallShard` 落盘或 `applyNewConfig` 消费 incoming 为止）。

线上排障（端点速查、症状对照、迁移卡滞 SOP）见 [`docs/runbook.md`](runbook.md)。

---

## 6. 一键构建 / 冒烟

`start.sh`（Git Bash / WSL / Linux）与 `start.bat`（Windows）会依次执行：
`go build ./...` → `go vet ./...` → `go test ./src/shardkv/...` → 构建
`gateway`/`kvcli`/`demo` 二进制（`bin/`）→ 运行 `demo` 作为全栈冒烟。

```bash
./start.sh        # 或双击 start.bat
make              # build + vet + shardkv test
make demo         # 构建二进制并运行全栈 demo
```

排障 / 观测子命令：

```bash
./start.sh migrate   # 实时迁移进度（对接 /debug/migrate，一眼看清再平衡是否卡住）
./start.sh status    # 集群健康总览（对接 /status，JSON 经 statusfmt 渲染为可读表格）
```

`status` 子命令把 `/status` 的 JSON 输出渲染为表格（未安装 jq/python 也能用）；
statusfmt 也可独立使用，支持机器可读模式与探活退出码：

```bash
curl -s localhost:8080/status | go run ./src/statusfmt          # 可读表格 + 健康/均衡评分摘要
curl -s localhost:8080/status | go run ./src/statusfmt -json    # JSON 评分报告（health_score/balance_score 等）
# 退出码：0=健康或透传；2=集群 STALLED（可直接接 CI/巡检脚本判活）
```

`migrate` 子命令直接打印 `/debug/migrate` 文本，pendingIn/pendingOut 有残留且 stall>0
即配置冻结风险信号。
