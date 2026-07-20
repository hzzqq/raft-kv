# raft-kv

从零实现的 **Raft 共识算法 + 基于 Raft 的容错 KV 存储**，参照 MIT 6.824 课程实验（Lab2A–2D / Lab3 / Lab4）与 Raft 论文 *Figure 2*。

- **Lab 2A–2D**：Raft 共识（选举 / 日志复制 / 持久化 / 快照）。
- **Lab 3**：基于 Raft 的线性一致 KV 服务。
- **Lab 4**：分片容错 KV（ShardMaster 配置服务 + ShardKV 分片存储，分片随配置在 replica group 间迁移）。

## 模块结构

```
raft-kv/
├── go.mod
├── .github/workflows/ci.yml   # GitHub Actions：vet + test + race
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
        └── shardkv_test.go # 分片 KV 测试
```

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

## 构建与测试

> 需要本地安装 Go 1.22+。本仓库未随附 Go 工具链。Windows 下 `GOCACHE`/`GOPATH` 必须用原生盘符绝对路径（如 `C:/Users/xxx/gocache`），否则报 "GOCACHE is not an absolute path"。

```bash
# 进入模块根目录（含 go.mod 的 raft-kv/）
go vet ./...
go test ./src/raft/... ./src/kvraft/... ./src/shardmaster/... ./src/shardkv/...
go test -race ./src/kvraft/...   # 竞态检测（可选，耗时更长）
```

### 测试覆盖（Labs 2–3 全绿，重复运行稳定）
- `raft`：`TestInitialElection` / `TestReElection`（2A 选举）、`TestBasicAgree` / `TestFailAgree` / `TestFailNoAgree` / `TestConcurrentStarts`（2B 复制）、`TestPersist1`（2C 持久化）、`TestSnapshot`（2D 快照）。
- `kvraft`：`TestKVBasic` / `TestKVConcurrency` / `TestKVFail`（故障转移）/ `TestKVPersist`（掉电重启重放）/ `TestKVSnapshot`（快照压缩：超过 `maxraftstate` 阈值主动快照、重启后从快照恢复）/ `TestKVSnapshotStress`（并发写入 + 周期性分区/重启抖动下快照路径不丢数据且 raft 状态有界）。
- `shardmaster`：Join / Leave / Move / Query 及其组合下的配置正确性与线性一致。
- `shardkv`：单组读写、多组迁移、并发迁移下的线性一致与无数据丢失。

### 工程健壮性要点
- **`restart()` 每次使用全新的 `applyCh`**：被 Kill 的旧 KV/Raft applier 仍阻塞在旧 channel 上（不关闭），若复用同一 channel，新 applier 会与之竞争 `ApplyMsg`，导致 `notify` 信号被已死 applier 吞掉、客户端永久挂起。
- **`labrpc` 关闭 server 时排空在途 RPC**：`Server.loop` 在 `done` 关闭后排空 `ch` 并关闭每条 pending 的 `m.done`；`Send` 通过 `select` 在 server 被关停时直接返回 `false`（按"不可达"重试），消除节点重启竞态导致的死锁。
- **`AppendEntries` 仅在日志真正变化时才持久化**：心跳（无新条目）不再重写整个状态，避免每 110ms 一次的全量 gob 序列化。

## 验证状态说明
- 固态的 Labs 2–3 与 `shardmaster` 已通过 `go vet` + `go test` 验证，并纳入 GitHub Actions CI（`vet` + `test` + `race`）。
- `src/shardkv/` 已实现，但若当前执行环境**缺少 Go 工具链**，则无法在此本地编译/运行其测试——此时仅做文档与工程化自检，**不会**提交未经 `go test` 验证的代码。请在装有 Go 1.22+ 的环境中跑 `go test ./src/shardkv/...` 完成最终验证，CI 亦会在推送时自动覆盖。

## 说明
- 这是面向学习的实验性实现，重点在正确性与可读性，非生产级部署。
- 持久化、RPC、网络均使用 MIT 6.824 提供的实验脚手架（`persister.go` / `labrpc.go`）。
