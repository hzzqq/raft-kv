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
- **持久化（2C）**：`persist()` 保存 `currentTerm` / `votedFor` / `log` / 快照边界；`readPersist()` 容忍损坏数据。
- **快照（2D）**：`Snapshot` / `CondInstallSnapshot` / `InstallSnapshot`，落后节点通过快照追赶。
- **并发**：所有状态访问走 `rf.mu`；复制由心跳计时器（~110ms）触发，避免持锁发 RPC 造成死锁。

### kvraft（KV 层）
- 复用 `raft` 包的 `Network` / `ClientEnd` / `Raft` / `Make` / `ApplyMsg`。
- `Op` 携带 `ClientId` + `Seq`，`applier` 据此做**幂等去重**；重复命令直接复用上次结果。
- `Clerk` 轮询各节点，遇到 `WrongLeader` 自动切换 `leaderHint` 重试。
- `waitApplied` 用带缓冲的 `notify` channel + 1s 超时，防止 leader 切换导致结果错位。

## 构建与测试

> 需要本地安装 Go 1.22+。本仓库未随附 Go 工具链。

```bash
# 进入模块根目录（含 go.mod 的 raft-kv/）
go vet ./...
go test ./src/raft/... ./src/kvraft/...
```

## 说明

- 这是面向学习的实验性实现，重点在正确性与可读性，非生产级部署。
- 持久化、RPC、网络均使用 MIT 6.824 提供的实验脚手架（`persister.go` / `labrpc.go`）。
