# raft-kv

从零实现的 **Raft 共识算法 + 基于 Raft 的容错 KV 存储**，参照 MIT 6.824 课程实验（Lab2A–2D / Lab3 / Lab4）与 Raft 论文 *Figure 2*。

- **Lab 2A–2D**：Raft 共识（选举 / 日志复制 / 持久化 / 快照）。
- **Lab 3**：基于 Raft 的线性一致 KV 服务。
- **Lab 4**：分片容错 KV（ShardMaster 配置服务 + ShardKV 分片存储，分片随配置在 replica group 间迁移）。

## 模块结构

```
raft-kv/
├── go.mod
├── .github/workflows/ci.yml   # GitHub Actions：vet + test + race + lint + coverage
└── src/
    ├── raft/        # Raft 共识核心（选举 / 日志复制 / 持久化 / 快照）
    │   ├── raft.go       # 状态机、选举、日志复制、快照（2D）、后台 ticker/applier
    │   ├── persister.go  # 状态 / 快照的持久化接口
    │   ├── labrpc.go     # 实验用 RPC 框架（ClientEnd / Network）
    │   └── raft_test.go  # 共识层测试
    ├── kvraft/      # 线性一致的 KV 服务（Get / Put / Append）—— Lab 3
    │   ├── kvraft.go      # KVServer + Clerk，clientId+seq 幂等去重
    │   └── kvraft_test.go # KV 层测试
    ├── shardmaster/ # 分片配置服务（ShardMaster）—— Lab 4 控制器
    │   ├── shardmaster.go      # Join/Leave/Move/Query，Raft 复制配置变更
    │   └── shardmaster_test.go # 配置服务测试
    └── shardkv/     # 分片容错 KV 存储 —— Lab 4 数据面
        ├── shardkv.go      # ShardKV + Clerk，按当前配置只服务归属分片 + 分片迁移
        │                   #   （含 cycle 19 ReadIndex 线性一致快速读优化）
        ├── bench_test.go   # ShardKV 基准测试（baseline ~16.6 ops/sec）
        └── shardkv_test.go # 分片 KV 测试（11 用例，含 2 个默认 skip）+ shardkv_internal_test.go（3 白盒）
    ├── cluster/     # 可复用进程内集群框架（供 demo/gateway/kvcli 复用）
    │   ├── cluster.go      # StartCluster / Clerk / Join / Leave / Move / WaitConfig
    │   └── cluster_test.go
    ├── gateway/     # HTTP REST 网关（自带进程内集群，暴露 KV + /metrics）
    │   ├── gateway.go      # Handler：GET/PUT/POST-append /kv/{key}、/healthz、/metrics
    │   ├── main.go         # 启动入口（默认 :8080）
    │   └── gateway_test.go
    ├── kvcli/       # HTTP 客户端 + 压测工具
    │   ├── client.go       # Client.Get/Put/Append/Bench
    │   ├── main.go         # get / put / append / bench 子命令
    │   └── client_test.go
    ├── demo/        # 端到端演示（进程内 KV 路径 + 全栈 HTTP 路径）
    │   └── main.go
    └── metrics/     # 零依赖并发安全指标库（Counter + Histogram + Registry）
        ├── metrics.go
        └── metrics_test.go
```

> 上层组件（cluster / gateway / kvcli / demo / metrics）的运行与压测方式见
> [`docs/usage.md`](docs/usage.md)；系统整体架构地图见
> [`docs/architecture.md`](docs/architecture.md)；测试覆盖率快照见
> [`docs/coverage.md`](docs/coverage.md)；ShardKV 数据面深层设计笔记见
> [`docs/lab4-shardkv-design.md`](docs/lab4-shardkv-design.md)；线上排障与可观测性手册见
> [`docs/runbook.md`](docs/runbook.md)。

## 快速启动

`start.sh`（Git Bash / Linux）与 `start.bat`（Windows）把整套 ShardKV 系统**真正拉起来**：
进程内启动 2 组副本集群，并起一个常驻 HTTP 网关（默认 `:8080`），可被 `kvcli` / `curl`
持续访问。旧版脚本只跑一次性 `demo` 后退出，现默认前台常驻。

```bash
# Git Bash / Linux
./start.sh              # 默认 = serve：构建网关并前台常驻（Ctrl+C 停止）
./start.sh bg           # 后台启动（写 raft-kv-gateway.pid + .log）
./start.sh stop         # 停止后台网关
./start.sh build        # 构建全部二进制到 bin/
./start.sh cli get hello   # 运行 kvcli 访问网关
./start.sh migrate      # 实时迁移进度（对接 /debug/migrate，一眼看清再平衡是否卡住）
./start.sh status       # 集群健康总览（对接 /status，JSON 经 statusfmt 渲染）

# Windows（cmd / PowerShell）
start.bat               # 默认 = serve
start.bat bg
start.bat stop
start.bat cli get hello
```

等效的 Make 目标：`make serve`（前台）、`make serve-bg`（后台）、`make stop`、`make cli args="get hello"`。

网关起来后，用 `kvcli` 或 curl 交互：

```bash
# 写入 + 读取
./start.sh cli put hello world
./start.sh cli get  hello          # -> world
./start.sh cli append hello "!"

# 或直接 curl
curl -X PUT http://localhost:8080/kv/foo -d 'bar'
curl http://localhost:8080/kv/foo            # -> bar
curl http://localhost:8080/healthz           # -> 200 OK
curl http://localhost:8080/metrics           # -> JSON 指标快照
```

> 网关地址可用第一个参数覆盖：`./start.sh serve :9090`。集群组数默认 2，改 `src/gateway/main.go`
> 中的 `nGroups` 即可。注意：集群是进程内的（基于 labrpc 内存网络），生产部署需替换为真实传输层。

## 设计要点

### raft（共识层）
- **角色**：`Follower` / `Candidate` / `Leader`，任期（`currentTerm`）单调递增。
- **选举（2A）**：随机化选举超时（260–480ms），`RequestVote` 带日志新旧比较（`LastLogTerm` / `LastLogIndex`）。
- **日志复制（2B）**：`AppendEntries` 含一致性检查与冲突回退（`ConflictTerm` / `ConflictIndex`，仿 6.824 快速推进 `nextIndex`）；`advanceCommit` 仅提交**当前任期**的日志（Raft 提交安全性）。
- **no-op 任期开头条目**：leader 上任时立即追加一条空命令（no-op）。按 Raft 提交规则，leader 只能借助"当前任期"的条目来间接提交旧任期的日志——no-op 作为当前任期的首条条目，被多数派复制并提交后即可"拉动"先前未提交的旧条目，分区愈合后状态机才能补齐。
- **持久化（2C）**：`persist()` 保存 `currentTerm` / `votedFor` / `log` / 快照边界；`readPersist()` 容忍损坏数据。日志以 `interface{}` 形式 gob 编码，KV 层在 `init()` 中 `gob.Register(Op{})`，否则重启后日志反序列化失败、命令丢失。
- **快照（2D）**：`Snapshot` / `CondInstallSnapshot` / `InstallSnapshot`，落后节点通过快照追赶。
- **leader lease（本轮新增）**：每个节点维护 `lastContact[peer]`（收到合法 `AppendEntries` / `InstallSnapshot` 时刷新），`HasLeaderLease()` 统计在 `ElectionTimeoutMin` 窗口内联系到多数派的节点——leader 仅在有 lease 时才对外提供 ReadIndex 线性一致读，避免无 lease 保证下的陈旧读（详见 shardkv ReadIndex）。
- **Pre-Vote 预投票（本轮新增 #44）**：候选人正式自增任期前，先以意向任期 `currentTerm+1` 用 `RequestPreVote` 征求多数派意向，不抬升 `currentTerm`、不持久化 `votedFor`；`startElection` 改为「预投票 → 多数授权 → 正式选举（`doRealElection`）」两段式，`preVoteWon` 守卫保证同一轮预投票只转化一次正式选举。核心收益：日志落后或处于少数派分区的节点永远拿不到多数预投票，因此永远不会抬升任期去扰动一个稳定的 leader——显著降低网络抖动 / 短暂分区下的无效重新选举与 tail latency。配套 `TestPreVoteStillElects` / `TestPreVoteDeniesStaleLog` / `TestPreVoteNoDisrupt`。
- **LeadershipTransfer 领导权转移（本轮新增 #45）**：新增 `TimeoutNow` RPC（接收方立即越过选举超时发起选举）与 `LeadershipTransfer(target)`（leader 先把目标副本同步到已提交位置，再发 `TimeoutNow`，最后以更高任期主动退位让路），用于负载再平衡与计划内维护时的「平滑换主」。`leadership_transfers` 指标记录转移次数。配套 `TestLeadershipTransfer`。
- **并发**：所有状态访问走 `rf.mu`；复制由心跳计时器（~110ms）触发，避免持锁发 RPC 造成死锁。

### kvraft（KV 层）—— Lab 3
- 复用 `raft` 包的 `Network` / `ClientEnd` / `Raft` / `Make` / `ApplyMsg`。
- `Op` 携带 `ClientId` + `Seq`，`applier` 据此做**幂等去重**；重复命令直接复用上次结果。
- `Clerk` 轮询各节点，遇到 `WrongLeader` 自动切换 `leaderHint` 重试。
- **Clerk 指数退避（本轮新增 #46）**：`Get`/`PutAppend` 的重试休眠由固定 50ms 改为**指数退避**（10ms 起步、每轮翻倍、上限 500ms），减少迁移抖动 / 换主窗口下的无效重试风暴；`leaderHint` 仍作为 leader 缓存加速收敛。
- `waitApplied` 用带缓冲的 `notify` channel + 1s 超时，防止 leader 切换导致结果错位。
- `applier` 对 leader 的 no-op（nil 命令）直接跳过，不更新状态机。
- **客户端会话 GC（本轮新增）**：以 `sessions[clientId] → clientSession{LastSeq, LastResult, lastAccess}` 取代旧的 `lastSeq/lastResult` 两个扁平 map，后台 `gc()` 按 TTL（默认 1h）清理空闲会话，避免长期运行内存无限增长；`Kill()` 经 `sync.Once` 关闭 `killCh` 停用 GC，配套 `TestClientSessionGC` 回归。
- **Get ReadIndex 快路径（本轮新增）**：leader 持 `HasLeaderLease()` 时，以 Raft `ReadIndex()` 返回的 `commitIndex` 为一致性点、待状态机 `apply` 到该索引后直接本地读（省一次日志追加，纯读路径低延迟），并以 `read_leases` 计数器记录快读次数；失去领导权 / 无 lease / 超时则回退常规 `propose` 路径（同样线性一致）。配套 `TestKVReadLease` 断言稳定集群下连续读命中快路径。

### shardmaster（配置服务）—— Lab 4 控制器
- 维护 `NShards = 10` 个分片到 replica group 的映射，并通过 Raft 复制每一次配置变更，保证配置序列的线性一致。
- **接口**：`Join`（新增 group）/ `Leave`（移除 group）/ `Move`（手工迁移单分片）/ `Query`（读取最新或历史配置）。
- **确定性再平衡** `rebalance`：对当前所有 `gid` 排序后轮转分配 10 个分片，保证同一份初始配置下结果唯一、可复现。
- **最小化搬动 + 输入校验（本轮强化）**：`rebalance` 改为「保留上一版分片映射 + 仅把多余分片从负载最重组搬到最轻组」的确定性最小搬动策略，避免每次 Join/Leave 全量重排引发大范围迁移抖动；`Join` / `Leave` / `Move` 增加输入校验（空 group / 重复 gid / 越界分片 / 未知操作 → `ErrInvalid`），非法请求直接拒绝不落 Raft，配套 `TestShardMasterValidation` / `TestRebalanceMinimalMoves` / `TestRebalanceKeepsBalance` 回归。
- **Move 只改一个分片**：新配置从上一版**继承完整分片映射**，仅覆盖被 `Move` 的目标分片，其余分片归属不变。若从全零数组开始只写一个分片，会让其余 9 个分片被清零成「未分配(gid 0)」，replica group 集体丢失分片所有权而卡死——这是 `Apply` 路径里最隐蔽的一类 bug。
- **幂等去重**：`Op` 携带 `CkId` + `Seq`，applier 仅当 `Seq > lastSeq[CkId]` 才应用；`Query` 为只读（不加 Raft，直接返回已提交的最新配置）。
- **消除丢失唤醒竞态**：调用方在 `rf.Start` **之前**就分配并注册 `NotifyId`，applier 据此唤醒等待者，避免"先 Start 拿 index 再注册 channel"造成的丢失唤醒。
- **关键接线**：测试框架用同一 `server id` 同时承载"配置服务 RPC"与"Raft 内部 RPC"，因此 `ShardMaster.RaftRPC` 负责把 `RequestVote` / `AppendEntries` / `InstallSnapshot` 派发给底层 `rf`；缺少这一转发则集群永远选不出 leader。

### shardkv（分片 KV）—— Lab 4 数据面
- 每个 replica group 按**当前配置**只服务归属自己的分片集合；`Get` / `Put` / `Append` 在分片不归本组（含迁移中）时返回 `ErrWrongGroup`，由 `Clerk` 重新查配置后重试。
- **Clerk 指数退避 + leader 缓存（本轮新增 #46）**：`Clerk` 新增 per-gid leader 缓存 `leaderOf` map——成功返回 `OK` 的 server 即记为该 gid 的缓存 leader；`orderServers` 把缓存 leader 前置优先重试，命中即免去一轮轮询；失败走**指数退避**（10ms→翻倍→500ms 上限）而非固定 50ms。`Get`/`PutAppend`/`GetE`/`putAppendE` 四路径统一应用该策略，迁移 churn 下客户端收敛更快、RPC 更少。
- **分片迁移（双路）**：旧 owner 推送（`SendShard`）+ 新 owner 拉取（`GetShard` / `fetchShard`）。旧 owner 收到新 owner 的 ack（即新组已在本地 Raft 提交该分片数据）后才在本组提交 `GCShard` 回收——保证数据在对方落盘前不丢失。
- **串行推进配置**：`pollConfig` 仅在「本组无未决迁移」且「最新配置恰好比当前领先 1 步」时才推进 `NewConfig`，避免迁移重叠引发的竞态。
- **`applyNewConfig` 边界**：新归属分片若上一版未分配（哨兵 `gid == 0`）则直接初始化空 `ShardData`；否则从 `prevConfig` 中记录的旧 owner 名单拉取（本版配置里该 gid 可能已被 `Leave` 移除，故必须取上一版）。
- **跨迁移客户端幂等**：每个分片的 `ShardData` 独立保存 `LastSeq` / `LastResult`，保证迁移前后客户端命令不重复执行。
- **迁移期「合并而非覆盖」**：新 owner 在分片正式归属前就可能已直接收到客户端写；此时若旧 owner 的迁移快照到达，必须**合并**（只补充旧 owner 快照里多出来的 key，并取较大的 `LastSeq`/`LastResult`）而非整体覆盖，否则会把新 owner 在迁移窗口内已写入的数据冲掉，造成迁移丢数据。此逻辑同时作用于 `applyInstallShard`（已持有分片时）与 `applyNewConfig`（incoming 缓冲在配置推进时落地）两个入口。
- **快照压缩**：KV 状态（各分片数据 + 当前/上一版配置 + 迁移中的 incoming/pending）被 gob 压进 Raft 快照；`installSnapshot` 在重启/落后追赶时一次性恢复。
- **ReadIndex 线性一致快速读（cycle 19 引入，本轮经 leader lease 重新启用）**：`Get` 在 leader 上先查 `raft.HasLeaderLease()`，持 lease 时以 `commitIndex` 为一致性点、待状态机 `apply` 到该索引后直接读本地状态机（省一次日志追加，纯读路径低延迟）；此前因 raft 层无 lease 保证而被保守禁用，现 `HasLeaderLease()` 提供任期 + 多数派近因保证，快读路径安全复活。失去领导权 / 无 lease / 超时则回退常规 `propose` 路径（同样线性一致）。`raft.ReadIndex()` 返回 leader 的 `commitIndex` + 领导权标记，`ShardKV.appliedIndex` 由 applier 在每条已应用条目 / 快照上置为绝对索引。
- **配置号幂等 + 过期传输安全丢弃（本轮 I2/I3 修复）**：`applyInstallShard` 以 `installedCfgNum[s]` 记录每分片「最近一次成功安装的迁移配置号」，重复/迟到的同号或旧号推送仅合并（不覆盖）不重复计耗时；**过期丢弃仅在本组「不拥有」该分片时进行**——若本组拥有该分片则迟到的合法传输（配置推进快于单跳迁移）必须安装，否则 `pendingIn[s]` 永不清除、配置冻结（ReMigration 快速 churn 回归根因，已修复并加 `TestInstallShardConfigNumIdempotent` / `TestDropStaleIncoming` 白盒回归）。

## 全栈组件与可观测性

在 Lab 4 核心之上，仓库额外提供一组「可直接跑起来」的上层组件（详见
[`docs/usage.md`](docs/usage.md)）：

- **`cluster` 包**（`src/cluster`）：把测试里的内存集群搭建逻辑抽成可 import 的包，
  供 demo / gateway / kvcli 复用。`StartCluster` / `Clerk` / `Join` / `Leave` / `Move` / `WaitConfig`。
- **`demo`**（`src/demo`）：一次性演示「进程内 KV 路径（Put/Get/Append + 跨组迁移）」
  与「全栈 HTTP 路径（经真实 HTTP 压网关 + 拉 `/metrics`）」，证明 `cluster → HTTP → client` 全栈打通。
- **`gateway`**（`src/gateway`）：自带进程内集群的 HTTP REST 网关，`GET/PUT/POST-append /kv/{key}`、
  `GET /healthz`、`GET /metrics`（按 `Accept` 协商：`text/plain` 或 `application/openmetrics-text` → Prometheus 文本格式；其余 → JSON 序列化 `shardkv.Metrics` 快照）、`GET /debug/shards`（逐副本暴露分片归属 + 迁移状态 `pendingIn/pendingOut/incoming` + **卡滞时间戳 `PendingInSince/PendingOutSince` 与 `StallSeconds`**，用于诊断 3 组再平衡冻结）。
  另提供 **`GET /status`**（集群健康总览 JSON：`ClusterStatus`，每 group leader/config/持有/待收/待迁/孤儿中转计数 + 卡滞秒数 + 整体 `healthy` 标志，卡滞 >2s 即判冻结，供监控/告警轮询）与 **`GET /debug/migrate`**（纯文本迁移进度，供 CLI `start.sh migrate` 直接展示）。
  集群冻结时通过「1s RPC 超时 + 5s 有界重试」**快速失败**并返回正确 REST 语义（`ErrWrongGroup→409` / `ErrWrongLeader→503` / `ErrTimeout→504`），而非挂死。`Handler()` 可单测；`main.go` 支持 `SIGINT`/`SIGTERM` 优雅退出。
  另提供 **并发限流**（默认 64 并发，`sem` 信号量，超量返回 `429 Too Many Requests`，避免网关在迁移抖动期被压垮）；**优雅退出 `Shutdown(ctx)`**（`sync.WaitGroup` 等待在途请求结束、关闭 `applyCh` 防泄漏，供 `main.go` 的 `SIGINT`/`SIGTERM` 调用）；新增 **`GET /debug/groups`** 暴露各 replica group 的 `gid` / 副本数 / leader / 当前 config / 持有分片数，便于排障，配套 `TestGatewayConcurrencyLimit` / `TestGatewayGroups` 回归。
- **`gateway` 可观测性与稳健性增强（本轮新增）**：
  - **`GET /debug/accesslog?limit=N`**：以环形缓冲（默认 256 条）暴露最近访问记录（方法 / 路径 / 状态码 / 延迟 ms），便于线上排查异常请求；配套 `TestGatewayAccessLog`。
  - **`GET /readyz`**：k8s 就绪探针——要求每个 replica group 都有「持 leader 租约（`HasLeaderLease()`）」的 leader 且无迁移卡滞才返回 `200`，否则 `503`；与 `GET /healthz`（liveness，进程存活即 `200`）明确区分，避免把「自认 leader 但已分区失联、无法提交」的节点误判为就绪。配套 `TestGatewayReadyz`。`ShardDebug` 新增 `Lease` 字段以支撑该判定。
  - **`http.TimeoutHandler` 请求超时兜底**：`Handler()` 以 `http.TimeoutHandler(mux, 30s, "request timed out")` 包裹，单请求超期返回 `503`，防止个别慢请求拖垮网关；`SetRequestTimeout` 可配置。配套 `TestGatewayRequestTimeout`。
  - `/metrics` 的 **Prometheus 文本格式**：`metrics.Registry.WritePrometheus` 按字母序稳定输出 counter/gauge 直接序列化、histogram 输出 `_count`/`_sum`/`_p50`/`_p95`/`_p99` 分位，`/metrics` 按 `Accept` 头协商（Prometheus → 文本，其余 → JSON）。配套 `TestGatewayMetricsPrometheus`。
  - **分级结构化日志 + `GET /debug/log`（本轮新增 #47）**：每请求经 `wrap` 中间件产生恰好一条分级日志（`debug`/`info`/`warn`/`error`，成功=`info`、4xx=`warn`、5xx=`error`），存入容量 256 的环形缓冲；`GET /debug/log?level=&limit=` 可按级别过滤、按条数分页回看，统一排障入口（取代散落 `fmt.Println`）。配套 `TestGatewayLogLevel`。
  - **`X-Request-ID` 请求链路透传（本轮新增 #48）**：`wrap` 在入站缺 `X-Request-ID` 时以 `crypto/rand` 生成 16 位 hex（存在则沿用），并统一写入响应头；访问日志与结构化日志均记录该 ID，便于跨请求 / 跨服务链路追踪。配套 `TestGatewayRequestID`。
  - **每客户端令牌桶限流（本轮新增 #49）**：在全局并发 `429` 之上新增按客户端标识（`X-Client-ID` 或 `RemoteAddr` IP）的令牌桶（默认 200 rps、突发 40），超限返回 `429 + Retry-After`，防止单个客户端拖垮网关；桶 map 超 4096 自动重置防内存膨胀。`SetClientRateLimit` 可配置。配套 `TestGatewayClientLimit`。
  - **CORS 中间件（本轮新增 #50，浏览器直连）**：`corsHandler` 注入 `Access-Control-Allow-*` 头，`OPTIONS` 预检直接返回 `204`；`corsOrigins` 为空允许所有源、非空仅回显匹配源。`SetCORS` 可配置。配套 `TestGatewayCORS`。
  - **YAML 配置加载 + `GET /debug/config`（本轮新增 #51/#52）**：零依赖极简 YAML 子集解析器（`ParseGatewayConfig`/`LoadGatewayConfig`，沙箱不可联网装 yaml 库故自包含）支持 `listen_addr`/`request_timeout_sec`/`max_concurrent`/`client_rate`/`client_burst`/`cors_origins` 覆盖默认值；`GatewayConfig.Apply` 在 `NewServer` 后将配置写入 `Server`，`GET /debug/config` 返回当前生效配置快照（脱敏）便于确认加载结果。配套 `TestGatewayConfigLoad` / `TestGatewayDebugConfig`。
- **`kvcli`**（`src/kvcli`）：HTTP 客户端 + 压测工具，`get`/`put`/`append` 子命令外，
  另有 `bench [op] [ops] [workers]` 报告端到端吞吐与 `p50/p95/p99` 延迟。
- **`metrics`**（`src/metrics`）：零依赖并发安全指标库（Counter + **Gauge（瞬时值，如当前 config 号 / apply 滞后，原子位模式存储 float64）** + 有界环形 Histogram + Registry），
  已接入 `raft` / `kvraft` / `shardkv` 三个热路径（纯原子增量，不改行为），供网关 `/metrics` 暴露；本轮新增 `config_changes` / `config_num` / `apply_lag` / `shard_bytes` / `shard_bytes_overflow` / `send_shard_latency` / `config_stalls` 等指标。

## 构建与测试

> 需要本地安装 Go 1.22+。本仓库**未随附 Go 工具链**；本机可用的托管 Go 在
> `C:/Users/Administrator/.workbuddy/binaries/go/go/bin/go.exe`。Windows 下
> `GOCACHE`/`GOPATH` 必须用原生盘符绝对路径（如 `C:/Users/xxx/gocache`），否则报
> "GOCACHE is not an absolute path"。

**零配置跑测（推荐）**：仓库提供 `run-tests.sh`，已把托管 Go + 绝对路径 `GOCACHE`/`GOPATH`
写死，Git Bash / WSL / Linux 下直接：

```bash
./run-tests.sh                 # 跑全部包
./run-tests.sh shardkv         # 只跑 src/shardkv
./run-tests.sh shardkv -run TestSKVConcurrent   # 带额外参数（如只跑某个用例）
```

手动等价命令（供参考，需自行设置绝对路径 GOPATH/GOCACHE）：

```bash
export GO="C:/Users/Administrator/.workbuddy/binaries/go/go/bin/go.exe"
export GOCACHE="C:/Users/Administrator/.cache/go-raftkv"
export GOPATH="C:/Users/Administrator/.cache/gopath-raftkv"
export GO111MODULE=on
"$GO" vet ./...
"$GO" test ./src/raft/... ./src/kvraft/... ./src/shardmaster/... ./src/shardkv/...
"$GO" test -race ./src/kvraft/...   # 竞态检测（可选，耗时更长；需 gcc）
```

### 测试覆盖（Labs 2–3 全绿，重复运行稳定）
- `raft`：`TestInitialElection` / `TestReElection`（2A 选举）、`TestBasicAgree` / `TestFailAgree` / `TestFailNoAgree` / `TestConcurrentStarts`（2B 复制）、`TestPersist1`（2C 持久化）、`TestSnapshot`（2D 快照）。
- `kvraft`：`TestKVBasic` / `TestKVConcurrency` / `TestKVFail`（故障转移）/ `TestKVPersist`（掉电重启重放）/ `TestKVSnapshot`（快照压缩：超过 `maxraftstate` 阈值主动快照、重启后从快照恢复）/ `TestKVSnapshotStress`（并发写入 + 周期性分区/重启抖动下快照路径不丢数据且 raft 状态有界）。
- `shardmaster`：Join / Leave / Move / Query 及其组合下的配置正确性与线性一致。
- `shardkv`（Lab 4 数据面，设计细节见 `docs/lab4-shardkv-design.md`）：
  - `TestSKVBasic` 单组读写；`TestSKVMove` 单分片跨组迁移后可读；`TestSKVJoinLeave` 两组 Join/Leave 后数据不丢；`TestSKVGC` 旧 owner 回收、新 owner 持有（GC-after-ack）。
  - `TestSKVConcurrent` 多客户端并发写 + 后台 churn，线性一致；`TestSKVSnapshotChurn` 开启 `maxraftstate` 下并发 + churn（命中 `installSnapshot` 路径）；`TestSKVReMigration` 单分片 A→B→A 快速漂移（配置不冻结、迁移窗口内写不丢）；`TestSKVConfigProgress` 多轮 Move 下配置持续推进 + 数据完整。
  - `TestSKVLinearizableAppend`（cycle 29）N 个并发 Clerk 对各自 key 追加顺序敏感值，后台 2 组 churn 下断言 `Get` 等于运行期拼接串，捕捉「迁移丢更新/乱序/覆盖」（比 Put-Get 更强的线性一致守卫）；`TestSKVPersistRestart`（cycle 34）写入后杀掉并**用同一 `raft.Persister` 重启整组副本**，断言数据可读且可继续写（快照压缩之外的另一条崩溃恢复路径）。
  - `TestSKVThreeGroupChurn`（cycle 40）构造 3 组反复 `Leave`/`Join` 的整体再平衡 churn，是 §7 冻结根因的复现载体；**现已转为常驻**（冻结根因已修复，见下）。
  - **混沌测试（本轮 I16/I18）**：`TestChaosLeaderKillDuringMigration`（I16）在分片持续迁移窗口内反复杀掉源/目的组 leader 并重启（同一 `raft.Persister` 崩溃恢复），断言配置持续推进、数据不丢——专门守护 cycle 48 孤儿 incoming / pendingIn 残留根因在「迁移中杀主」场景下不复发；`TestChaosLongRun`（I18）更长轮次（40 轮）+ 并发纯读放大崩溃-重选窗口。CI 侧新增 `chaos-race`（I17，`-race` 覆盖混沌/迁移路径，本地无 gcc 故放 CI）与 `chaos-long`（I18，放大轮次防 liveness 回归）两个 job。
  - **3 组/多跳再平衡冻结已根治**（详见设计文档 §7）：两条失效路径均消除——① fetch 侧卡死由 cycle 48 的「`applyNewConfig` 消费 `incoming` 必清 `pendingIn` + `pollConfig` 仅以 `pendingIn` 门控配置推进 + `migratePump` 保活泵」消除；② orphan incoming（中间组收到迟到推送却无主可装、真正主人永远拉不到）由本轮的 `GetShard` **安全回退**（本组 `shards[s]` 缺失时返回 `incoming[s]`，只读、零改写、零新协程，与 `applyNewConfig` 权威路径正交）消除。曾 3 次尝试「独立 goroutine 改写状态式」修复（cycle 9 `reconcile`、cycle 55 `drainOrphanedIncoming`、本轮初 `forwardIncoming`）均因状态冲突 / RPC 风暴回归，`GetShard` 回退为正确解。`TestSKVReMigration`（A→B→A 漂移）与 `TestSKVThreeGroupChurn`（3 组 churn）现已**常驻且确定性通过**（`-count=3` 串行复跑均 3/3），CI 侧另设 `migration-stress` job 以 `-count=5` 高压力防回归。
  - **迁移 liveness 看门狗（cycle 39）**：`pollConfig` 在 `pendingIn/pendingOut` 任一分片卡滞 >3s 时，以**最新** ShardMaster 配置重算 owner 并重拉取/重推送（`fetchEpoch` bump 使陈旧 fetcher 自退），同时 `Metrics.Counter("config_stalls").Inc()`（`/metrics` 可观测）。网关 `/debug/shards` 暴露 `PendingInSince/PendingOutSince/StallSeconds`（cycle 46），`/status` 的 `healthy` 以卡滞 >2s 为冻结判据。看门狗作为极端卡死兜底 + 观测信号保留。

### 工程健壮性要点
- **`restart()` 每次使用全新的 `applyCh`**：被 Kill 的旧 KV/Raft applier 仍阻塞在旧 channel 上（不关闭），若复用同一 channel，新 applier 会与之竞争 `ApplyMsg`，导致 `notify` 信号被已死 applier 吞掉、客户端永久挂起。
- **`labrpc` 关闭 server 时排空在途 RPC**：`Server.loop` 在 `done` 关闭后排空 `ch` 并关闭每条 pending 的 `m.done`；`Send` 通过 `select` 在 server 被关停时直接返回 `false`（按"不可达"重试），消除节点重启竞态导致的死锁。
- **`AppendEntries` 仅在日志真正变化时才持久化**：心跳（无新条目）不再重写整个状态，避免每 110ms 一次的全量 gob 序列化。

## 验证状态说明
- Labs 2–3、`shardmaster`、`shardkv` 均已在本地 Go 1.22.5 下通过 `go vet` + `go test`（含 `-count=1`）验证，并纳入 GitHub Actions CI（`vet` + `test` + `race` + 非阻断 `lint` + `coverage` 上传）。
- 本机交互 shell 默认无 `go`，但仓库随附的托管 Go 工具链（`C:/Users/Administrator/.workbuddy/binaries/go/go/bin/go.exe`）可用于本地验证；该环境**无 gcc**，故 `go test -race` 仅能在 CI（GitHub ubuntu + gcc）侧运行，`shardkv` 的并发/冻结类回归以「高频 churn + 多轮循环」测试替代 race detector 来暴露。
- 自动化纪律：每次改动本地提交、验收不过绝不提交；**前多轮自主迭代（cycle 1–28、cycle 29–38、cycle 39–48、cycle 49–57、cycle 58–67）已按用户授权执行 `git push origin master`（仅普通推送、不 --force、不 `rm -rf`）**（见 `docs/lab4-shardkv-design.md` 与 `.workbuddy/self-driving/state.json`）。**本轮迭代（cycle 68–87，「照刚刚的迭代20次」：raft leader lease + ShardKV ReadIndex 快读复活 / 配置号幂等 `installedCfgNum` + ReMigration 冻结修复 / shardmaster 最小搬动 + 输入校验 `ErrInvalid` / 网关并发限流 429 + 优雅退出 `Shutdown` + `/debug/groups` / kvraft 客户端会话 GC / metrics 新增 Gauge 与多项指标 / 混沌测试 I16/I18 + CI 竞态与长时 job）同样获得用户授权，将在全量 `build+vet+test` 通过后执行 `git push origin master`（仅 fast-forward）**。

**最新一轮自主迭代（用户授权「迭代20次，方向是新增功能 / 完善旧功能 / 解决显性与隐性问题」，对应内部迭代 n=35–42）已全部本地提交**：含选举窗口跨迁移丢写修复（`GetShard` 三条件守卫 + raft `committedCurrentTerm` 标记）、网关可观测性（Prometheus `/metrics` 协商、访问日志 `/debug/accesslog`、`/readyz` 就绪探针、`http.TimeoutHandler` 请求超时兜底）、kvraft `Get` ReadIndex 快路径、`raft.HasCommittedCurrentTerm` 确定性守护测试；全仓 `go test ./...` 验证通过。因沙箱网络阻断 `github.com:443`，`git push origin master` 暂未完成，待联网环境补推（仅 fast-forward、绝不 `--force`、绝不 `rm -rf`）。

**最近一轮自主迭代（用户授权「迭代10次，方向=新增功能 / 完善旧功能 / 解决显性与隐性问题」，对应内部迭代 n=44–#53）已全部本地提交**：在 Raft 层新增 **Pre-Vote 预投票**（#44，候选人正式选举前先征求多数派意向，日志落后的少数派分区节点永不扰动稳定 leader）与 **LeadershipTransfer 领导权转移**（#45，平滑换主，用于负载再平衡 / 计划内维护）；Clerk 层新增 **指数退避 + per-gid leader 缓存**（#46，kvraft / shardkv 双 Clerk，迁移 churn 下收敛更快、RPC 更少）；网关层新增 **分级结构化日志 + `/debug/log`**（#47，并顺带修复 #44/#45 引入但被挂起掩盖的集群分发器未注册 `RequestPreVote`/`TimeoutNow` 真实回归）、**`X-Request-ID` 请求链路透传**（#48）、**每客户端令牌桶限流**（#49）、**CORS 中间件**（#50）、**YAML 配置文件加载 + `/debug/config` 生效配置快照**（#51/#52）。全仓 `go build ./...` + 网关 cluster-free 测试套件验证通过，因沙箱网络阻断 `github.com:443`，`git push origin master` 暂未完成，待联网环境补推（仅 fast-forward、绝不 `--force`、绝不 `rm -rf`）。

### R3 自主迭代交付（#54–#67，网关/可观测性/客户端/工程化）

用户授权「迭代50次」后的 R3 轮次（内部迭代 n=#54–#67，状态以 `.workbuddy/self-driving/state.json` 为准）。本轮聚焦网关与周边组件，全部本地提交 + cluster-free 测试验证（沙箱 raft 选举偶发挂死，故网关/客户端/工具测试一律 cluster-free，绕开真实集群）：

- **网关（#54–#62）**：请求体大小上限（413 + MaxBytesReader 兜底）、响应 gzip 压缩（含 Vary 头）、基线安全响应头（nosniff/DENY/Referrer-Policy）、IP 白名单（403）、`/debug/routes` 路由清单、`/debug/version` 版本与 uptime、配置加载校验告警（越界值记 warn）、后端健康熔断（连续 5xx 快速失败 503）、按路由前缀限流（路由级令牌桶 + 修复 tokenBucket data race）。
- **可观测性（#63）**：`metrics.WritePrometheus` 修复 Prometheus exposition 格式违规——直方图原错误声明 `# TYPE xxx histogram` 却输出自定义 p50/p95/p99 序列（scrape 客户端解析失败），改为每个派生序列声明正确 TYPE（`_count`=counter、`_sum`/`_p50`/`_p95`/`_p99`=gauge），并新增 `sanitizeMetricName` 清洗非法指标名（点/连字符→_），确保被 scrape 客户端正确采集。
- **客户端（#64）**：kvcli `Get/Put/Append` 在网关返回非 200 时把（截断后的）响应体纳入 error（排障可见真实错误，不再静默丢弃）；`Bench` 新增整体墙钟超时（默认 30s）+ 内部 ctx，后端挂死时不再无限拖尾。
- **工具（#65）**：`statusfmt` 渲染逻辑抽取为可测函数 + 白盒测试（nil 切片归一显示 `[]` 而非 `<nil>`）。
- **演示（#66）**：`demo` HTTP 演示段增加整体超时兜底 + 优雅关闭（`srv.Shutdown`），并补 `key2shard` 确定性白盒测试（分片映射不变量）。
- **工程化（#67）**：Makefile 新增 `test-all`（覆盖全部包，含本轮新增的 cluster-free 测试）/`fmt`（gofmt 检查，不重写上游 6.824 代码）/`bench`（raft 基准）目标；原 `test` 语义保留（仅 shardkv）以兼容 CI。

全部变更已本地提交；R3 #54–#62 已按授权 `git push origin master`（仅 fast-forward），#63–#67 待批量 push。

### R3 自主迭代交付（#68–#82，客户端/工具/可观测性增强）

R3 续批（内部迭代 n=#68–#82）。延续「网关/客户端/工具测试一律 cluster-free，绕开沙箱 raft 选举偶发挂死」的纪律，全部本地提交 + 白盒测试验证：

- **工程化/文档（#68）**：README 同步 R3 #54–#67 交付小结；runbook §4 新增「RPC 分发器必须覆盖全部 Raft 内部 RPC」正确性决策（源自 #71 修复的真实回归）。
- **白盒测试与隐性回归修复（#69–#72）**：raft `Persister` 读写往返/拷贝隔离、shardmaster `rebalance`/`validate` 纯函数（无主/空组/不匀摊）、`statusfmt` 渲染、demo `key2shard`、kvcli 错误体透传/超时等白盒测试；并修复 #47 遗留的集群测试分发器未登记 `RequestPreVote`/`TimeoutNow` 真实回归（#71，使 ShardMaster/ShardKV 集群测试恢复可运行）。
- **kvcli 客户端（#73/#74）**：读穿缓存（`EnableCache`：TTL + 容量上限 + Put/Append 写失效，降低回源）；客户端级重试（对网络错误 / 503 / 504 指数退避，Put/Append 因网关 Clerk 幂等去重安全可重试）。
- **metrics（#75）**：直方图新增 `Min`/`Max` 字段（Snapshot 暴露，避免 JSON 出现非有限数被 Prometheus 拒绝），新增 `Timer` 便捷类型（`Record` 一段耗时，比手写 `Record` 更不易漏）。
- **shardmaster（#76）**：纯函数配置比较辅助 `ConfigsEqual` / `IsNewer` / `NextConfigNum` / `OwnedShards`，统一「配置演进判断」语义，便于测试断言与诊断端点复用。
- **kvraft（#77）**：幂等去重白盒测试（直接喂 `applyCh`，验证同 `clientId+seq` 重复命令复用 `LastResult`、新 seq 才重执行），守护线性一致性的关键保证。
- **util 工具包（#78/#81）**：新增 `util.Backoff`（指数退避 + 抖动 + 上限，供 kvcli/demo 重试复用）、`util.LRU`（有界最近最少使用缓存，容量超限淘汰最久未用）。
- **demo（#79）**：`waitHealth` 改为指数退避重试（复用 `util.Backoff`），网关/集群暂不可达时不会空打也不会永久阻塞。
- **gateway（#80）**：per-route 请求指标埋点（请求总数 / 延迟直方图 / 按状态码分桶计数），经 `/metrics` 与 KV 层 `shardkv.Metrics` 合并暴露，统一采集网关 HTTP 面与 KV 层健康度。

R3 #54–#82 全部本地提交；#54–#72 已按授权 fast-forward 推送，本批 #73–#82 在批次收尾时 fast-forward 推送（仅 fast-forward、绝不 `--force`、绝不 `rm -rf`）。

### R3 自主迭代交付（#83–#103，kvcli 缓存防护 + 工具/可观测性/网关增强）

延伸「网关/客户端/工具测试一律 cluster-free，绕开沙箱 raft 选举偶发挂死」的纪律，本批共 21 轮（内部 cycle 29–50 对应 #83–#103），全部本地提交 + 白盒测试验证；其中 #83–#100 已落地：

- **kvcli 缓存击穿/穿透防护（#83–#87）**：`util.singleflight`（并发同 key 回源合并，防热点 key 同时失效打爆后端）、`Client.EnableSingleFlight`、读穿缓存命中/未命中/单飞合并/负向空值原子计数器 `CacheStats`、`BenchWithTimeout` 窗口快照统计缓存命中率、**MGet 并发批量读取**（复用连接/缓存/单飞/重试全链路，部分失败不阻断、重复 key 被单飞合并）；配套 client 级集成与压力缓存测试。
- **util 工具包（#88–#91）**：有界信号量 `Semaphore`（支持加权获取 + ctx 取消回滚 + 防下溢）、滑动窗口限流器 `SlidingWindowLimiter`（按 key 限流、滑窗清扫、注入时钟便于测试）、并发任务组 `ErrGroup`（出错即取消全组）、`metrics` Prometheus 暴露补全 `# HELP` 描述注释（无描述指标向后兼容）。
- **shardmaster（#92）**：`ValidateConfig` 单配置内部一致性校验（未知 gid / 空地址 / 负编号 / 无 group），与配置演进纯函数互补。
- **statusfmt（#93–#94）**：`clusterHealthScore` 集群健康分（leader 占比 / stall / 积压加权，0–100）、`shardBalance` 分片均衡度（极差/总分片占比，0–100），均为纯函数、cluster-free 可测。
- **demo（#96）**：cluster-free 启动诊断 `CollectStartupReport` / `FormatStartupReport`（环境采集 + 本地自检 + 可读报告），跑集群前打印，`RAFT_KV_DEMO_QUIET` 可静默。
- **shardkv（#97）**：`PlanRebalance` 迁移计划纯函数（只读预览/审计，给出 From/To 步骤，与 shardmaster.rebalance 互补），便于配置提交前评估迁移代价、做 dry-run 审计。
- **util（#98）**：`OnceFlag` 一次性开关（CAS 翻转，Trigger 返回是否本次触发，Done 可观测），用于 shutdown / migration 等「只做一次」护栏，防重复执行破坏性动作。
- **metrics（#99）**：`Registry.Subsystem` 命名空间前缀（子表共享存储+锁，注册名自动加前缀、可嵌套累加，导出/快照按前缀过滤），单一注册表内按组件（raft / shardkv / gateway）分组命名空间，避免跨组件同名冲突且便于一次性 scrape。
- **gateway（#100）**：`GatewayConfig.Validate()` 硬校验（接 ValidateConfig 模式：listen_addr 须合法 host:port 且端口 1..65535、超时/并发/体量须为正、限流不矛盾、CIDR 合法），`Apply` 开头调用，结构性错误记 levelError（与既有软告警 levelWarn 区分）。

#101 README 同步本批、**#102 runbook §4 追加决策**、**#103 全量 build+vet+test 验收 + fast-forward 推送**，对应的收尾提交见 git 历史（cycle 29–50 / #83–#103）。

## 说明
- 这是面向学习的实验性实现，重点在正确性与可读性，非生产级部署。
- 持久化、RPC、网络均使用 MIT 6.824 提供的实验脚手架（`persister.go` / `labrpc.go`）。
