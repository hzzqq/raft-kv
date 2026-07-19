# raft-kv

从零实现的 **Raft 共识算法 + 基于 Raft 的容错 KV 存储**，参照 MIT 6.824 课程实验（Lab2A–2D / Lab3）与 Raft 论文 *Figure 2*。

## 模块结构

```
raft-kv/
├── go.mod
└── src/
    ├── raft/        # Raft 共识核心（选举 / 日志复制 / 持久化 / 快照）
    │   ├── raft.go       # 状态机、选举、日志复制、快照（2D）、后台 ticker/applier
    │   ├── persister.go  # 状态 / 快照的持久化接口
    │   ├── labrpc.go     # 实验用 RPC 框架（ClientEnd / Network）
    │   └── raft_test.go  # 共识层测试
    └── kvraft/      # 线性一致的 KV 服务（Get / Put / Append）
        ├── kvraft.go      # KVServer + Clerk，clientId+seq 幂等去重
        └── kvraft_test.go # KV 层测试
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

### kvraft（KV 层）
- 复用 `raft` 包的 `Network` / `ClientEnd` / `Raft` / `Make` / `ApplyMsg`。
- `Op` 携带 `ClientId` + `Seq`，`applier` 据此做**幂等去重**；重复命令直接复用上次结果。
- `Clerk` 轮询各节点，遇到 `WrongLeader` 自动切换 `leaderHint` 重试。
- `waitApplied` 用带缓冲的 `notify` channel + 1s 超时，防止 leader 切换导致结果错位。
- `applier` 对 leader 的 no-op（nil 命令）直接跳过，不更新状态机。

## 构建与测试

> 需要本地安装 Go 1.22+。本仓库未随附 Go 工具链。Windows 下 `GOCACHE`/`GOPATH` 必须用原生盘符绝对路径（如 `C:/Users/xxx/gocache`），否则报 "GOCACHE is not an absolute path"。

```bash
# 进入模块根目录（含 go.mod 的 raft-kv/）
go vet ./...
go test ./src/raft/... ./src/kvraft/...
```

### 测试覆盖（当前全绿，重复运行稳定）

- `raft`：`TestInitialElection` / `TestReElection`（2A 选举）、`TestBasicAgree` / `TestFailAgree` / `TestFailNoAgree` / `TestConcurrentStarts`（2B 复制）、`TestPersist1`（2C 持久化）、`TestSnapshot`（2D 快照）。
- `kvraft`：`TestKVBasic` / `TestKVConcurrency` / `TestKVFail`（故障转移）/ `TestKVPersist`（掉电重启重放）/ `TestKVSnapshot`（快照压缩：超过 `maxraftstate` 阈值主动快照、重启后从快照恢复）/ `TestKVSnapshotStress`（并发写入 + 周期性分区/重启抖动下快照路径不丢数据且 raft 状态有界）。

### 工程健壮性要点

- **`restart()` 每次使用全新的 `applyCh`**：被 Kill 的旧 KV/Raft applier 仍阻塞在旧 channel 上（不关闭），若复用同一 channel，新 applier 会与之竞争 `ApplyMsg`，导致 `notify` 信号被已死 applier 吞掉、客户端永久挂起。
- **`labrpc` 关闭 server 时排空在途 RPC**：`Server.loop` 在 `done` 关闭后排空 `ch` 并关闭每条 pending 的 `m.done`；`Send` 通过 `select` 在 server 被关停时直接返回 `false`（按"不可达"重试），消除节点重启竞态导致的死锁。
- **`AppendEntries` 仅在日志真正变化时才持久化**：心跳（无新条目）不再重写整个状态，避免每 110ms 一次的全量 gob 序列化。

## 说明

- 这是面向学习的实验性实现，重点在正确性与可读性，非生产级部署。
- 持久化、RPC、网络均使用 MIT 6.824 提供的实验脚手架（`persister.go` / `labrpc.go`）。
