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
> [`docs/lab4-shardkv-design.md`](docs/lab4-shardkv-design.md)。

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
- **并发**：所有状态访问走 `rf.mu`；复制由心跳计时器（~110ms）触发，避免持锁发 RPC 造成死锁。

### kvraft（KV 层）—— Lab 3
- 复用 `raft` 包的 `Network` / `ClientEnd` / `Raft` / `Make` / `ApplyMsg`。
- `Op` 携带 `ClientId` + `Seq`，`applier` 据此做**幂等去重**；重复命令直接复用上次结果。
- `Clerk` 轮询各节点，遇到 `WrongLeader` 自动切换 `leaderHint` 重试。
- `waitApplied` 用带缓冲的 `notify` channel + 1s 超时，防止 leader 切换导致结果错位。
- `applier` 对 leader 的 no-op（nil 命令）直接跳过，不更新状态机。

### shardmaster（配置服务）—— Lab 4 控制器
- 维护 `NShards = 10` 个分片到 replica group 的映射，并通过 Raft 复制每一次配置变更，保证配置序列的线性一致。
- **接口**：`Join`（新增 group）/ `Leave`（移除 group）/ `Move`（手工迁移单分片）/ `Query`（读取最新或历史配置）。
- **确定性再平衡** `rebalance`：对当前所有 `gid` 排序后轮转分配 10 个分片，保证同一份初始配置下结果唯一、可复现。
- **Move 只改一个分片**：新配置从上一版**继承完整分片映射**，仅覆盖被 `Move` 的目标分片，其余分片归属不变。若从全零数组开始只写一个分片，会让其余 9 个分片被清零成「未分配(gid 0)」，replica group 集体丢失分片所有权而卡死——这是 `Apply` 路径里最隐蔽的一类 bug。
- **幂等去重**：`Op` 携带 `CkId` + `Seq`，applier 仅当 `Seq > lastSeq[CkId]` 才应用；`Query` 为只读（不加 Raft，直接返回已提交的最新配置）。
- **消除丢失唤醒竞态**：调用方在 `rf.Start` **之前**就分配并注册 `NotifyId`，applier 据此唤醒等待者，避免"先 Start 拿 index 再注册 channel"造成的丢失唤醒。
- **关键接线**：测试框架用同一 `server id` 同时承载"配置服务 RPC"与"Raft 内部 RPC"，因此 `ShardMaster.RaftRPC` 负责把 `RequestVote` / `AppendEntries` / `InstallSnapshot` 派发给底层 `rf`；缺少这一转发则集群永远选不出 leader。

### shardkv（分片 KV）—— Lab 4 数据面
- 每个 replica group 按**当前配置**只服务归属自己的分片集合；`Get` / `Put` / `Append` 在分片不归本组（含迁移中）时返回 `ErrWrongGroup`，由 `Clerk` 重新查配置后重试。
- **分片迁移（双路）**：旧 owner 推送（`SendShard`）+ 新 owner 拉取（`GetShard` / `fetchShard`）。旧 owner 收到新 owner 的 ack（即新组已在本地 Raft 提交该分片数据）后才在本组提交 `GCShard` 回收——保证数据在对方落盘前不丢失。
- **串行推进配置**：`pollConfig` 仅在「本组无未决迁移」且「最新配置恰好比当前领先 1 步」时才推进 `NewConfig`，避免迁移重叠引发的竞态。
- **`applyNewConfig` 边界**：新归属分片若上一版未分配（哨兵 `gid == 0`）则直接初始化空 `ShardData`；否则从 `prevConfig` 中记录的旧 owner 名单拉取（本版配置里该 gid 可能已被 `Leave` 移除，故必须取上一版）。
- **跨迁移客户端幂等**：每个分片的 `ShardData` 独立保存 `LastSeq` / `LastResult`，保证迁移前后客户端命令不重复执行。
- **迁移期「合并而非覆盖」**：新 owner 在分片正式归属前就可能已直接收到客户端写；此时若旧 owner 的迁移快照到达，必须**合并**（只补充旧 owner 快照里多出来的 key，并取较大的 `LastSeq`/`LastResult`）而非整体覆盖，否则会把新 owner 在迁移窗口内已写入的数据冲掉，造成迁移丢数据。此逻辑同时作用于 `applyInstallShard`（已持有分片时）与 `applyNewConfig`（incoming 缓冲在配置推进时落地）两个入口。
- **快照压缩**：KV 状态（各分片数据 + 当前/上一版配置 + 迁移中的 incoming/pending）被 gob 压进 Raft 快照；`installSnapshot` 在重启/落后追赶时一次性恢复。
- **ReadIndex 线性一致快速读（cycle 19）**：`Clerk`/`Get` 在 leader 上以 `commitIndex` 为一致性点，待本组状态机 `apply` 到该索引后直接读本地状态机，省去一次日志追加；等待期间失去领导权或超时则回退到常规 `propose` 路径（同样线性一致）。`raft.ReadIndex()` 返回 leader 的 `commitIndex` + 领导权标记，`ShardKV.appliedIndex` 由 applier 在每条已应用条目 / 快照上置为绝对索引。

## 全栈组件与可观测性

在 Lab 4 核心之上，仓库额外提供一组「可直接跑起来」的上层组件（详见
[`docs/usage.md`](docs/usage.md)）：

- **`cluster` 包**（`src/cluster`）：把测试里的内存集群搭建逻辑抽成可 import 的包，
  供 demo / gateway / kvcli 复用。`StartCluster` / `Clerk` / `Join` / `Leave` / `Move` / `WaitConfig`。
- **`demo`**（`src/demo`）：一次性演示「进程内 KV 路径（Put/Get/Append + 跨组迁移）」
  与「全栈 HTTP 路径（经真实 HTTP 压网关 + 拉 `/metrics`）」，证明 `cluster → HTTP → client` 全栈打通。
- **`gateway`**（`src/gateway`）：自带进程内集群的 HTTP REST 网关，`GET/PUT/POST-append /kv/{key}`、
  `GET /healthz`、`GET /metrics`（JSON 序列化 `shardkv.Metrics` 快照）、`GET /debug/shards`（逐副本暴露分片归属 + 迁移状态 `pendingIn/pendingOut/incoming` + **卡滞时间戳 `PendingInSince/PendingOutSince` 与 `StallSeconds`**，用于诊断 3 组再平衡冻结）。
  另提供 **`GET /status`**（集群健康总览 JSON：`ClusterStatus`，每 group leader/config/持有/待收/待迁/孤儿中转计数 + 卡滞秒数 + 整体 `healthy` 标志，卡滞 >2s 即判冻结，供监控/告警轮询）与 **`GET /debug/migrate`**（纯文本迁移进度，供 CLI `start.sh migrate` 直接展示）。
  集群冻结时通过「1s RPC 超时 + 5s 有界重试」**快速失败**并返回正确 REST 语义（`ErrWrongGroup→409` / `ErrWrongLeader→503` / `ErrTimeout→504`），而非挂死。`Handler()` 可单测；`main.go` 支持 `SIGINT`/`SIGTERM` 优雅退出。
- **`kvcli`**（`src/kvcli`）：HTTP 客户端 + 压测工具，`get`/`put`/`append` 子命令外，
  另有 `bench [op] [ops] [workers]` 报告端到端吞吐与 `p50/p95/p99` 延迟。
- **`metrics`**（`src/metrics`）：零依赖并发安全指标库（Counter + 有界环形 Histogram + Registry），
  已接入 `raft` / `kvraft` / `shardkv` 三个热路径（纯原子增量，不改行为），供网关 `/metrics` 暴露。

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
  - **3 组/多跳再平衡冻结已根治**（详见设计文档 §7）：两条失效路径均消除——① fetch 侧卡死由 cycle 48 的「`applyNewConfig` 消费 `incoming` 必清 `pendingIn` + `pollConfig` 仅以 `pendingIn` 门控配置推进 + `migratePump` 保活泵」消除；② orphan incoming（中间组收到迟到推送却无主可装、真正主人永远拉不到）由本轮的 `GetShard` **安全回退**（本组 `shards[s]` 缺失时返回 `incoming[s]`，只读、零改写、零新协程，与 `applyNewConfig` 权威路径正交）消除。曾 3 次尝试「独立 goroutine 改写状态式」修复（cycle 9 `reconcile`、cycle 55 `drainOrphanedIncoming`、本轮初 `forwardIncoming`）均因状态冲突 / RPC 风暴回归，`GetShard` 回退为正确解。`TestSKVReMigration`（A→B→A 漂移）与 `TestSKVThreeGroupChurn`（3 组 churn）现已**常驻且确定性通过**（`-count=3` 串行复跑均 3/3），CI 侧另设 `migration-stress` job 以 `-count=5` 高压力防回归。
  - **迁移 liveness 看门狗（cycle 39）**：`pollConfig` 在 `pendingIn/pendingOut` 任一分片卡滞 >3s 时，以**最新** ShardMaster 配置重算 owner 并重拉取/重推送（`fetchEpoch` bump 使陈旧 fetcher 自退），同时 `Metrics.Counter("config_stalls").Inc()`（`/metrics` 可观测）。网关 `/debug/shards` 暴露 `PendingInSince/PendingOutSince/StallSeconds`（cycle 46），`/status` 的 `healthy` 以卡滞 >2s 为冻结判据。看门狗作为极端卡死兜底 + 观测信号保留。

### 工程健壮性要点
- **`restart()` 每次使用全新的 `applyCh`**：被 Kill 的旧 KV/Raft applier 仍阻塞在旧 channel 上（不关闭），若复用同一 channel，新 applier 会与之竞争 `ApplyMsg`，导致 `notify` 信号被已死 applier 吞掉、客户端永久挂起。
- **`labrpc` 关闭 server 时排空在途 RPC**：`Server.loop` 在 `done` 关闭后排空 `ch` 并关闭每条 pending 的 `m.done`；`Send` 通过 `select` 在 server 被关停时直接返回 `false`（按"不可达"重试），消除节点重启竞态导致的死锁。
- **`AppendEntries` 仅在日志真正变化时才持久化**：心跳（无新条目）不再重写整个状态，避免每 110ms 一次的全量 gob 序列化。

## 验证状态说明
- Labs 2–3、`shardmaster`、`shardkv` 均已在本地 Go 1.22.5 下通过 `go vet` + `go test`（含 `-count=1`）验证，并纳入 GitHub Actions CI（`vet` + `test` + `race` + 非阻断 `lint` + `coverage` 上传）。
- 本机交互 shell 默认无 `go`，但仓库随附的托管 Go 工具链（`C:/Users/Administrator/.workbuddy/binaries/go/go/bin/go.exe`）可用于本地验证；该环境**无 gcc**，故 `go test -race` 仅能在 CI（GitHub ubuntu + gcc）侧运行，`shardkv` 的并发/冻结类回归以「高频 churn + 多轮循环」测试替代 race detector 来暴露。
- 自动化纪律：每次改动本地提交、验收不过绝不提交；**前多轮自主迭代（cycle 1–28、cycle 29–38、cycle 39–48）已按用户授权执行 `git push origin master`（仅普通推送、不 --force、不 `rm -rf`）**（见 `docs/lab4-shardkv-design.md` 与 `.workbuddy/self-driving/state.json`）。**本轮迭代（3 组/多跳冻结根治 + 网关可观测 + `migrate`/`status` CLI + `migration-stress` CI job + 本文档刷新）同样获得用户授权，将在全量 `build+vet+test` 通过后执行 `git push origin master`（仅 fast-forward）**。

## 说明
- 这是面向学习的实验性实现，重点在正确性与可读性，非生产级部署。
- 持久化、RPC、网络均使用 MIT 6.824 提供的实验脚手架（`persister.go` / `labrpc.go`）。
