# Lab 4 设计笔记：分片容错 KV（ShardKV）

本文记录 `src/shardmaster/` 与 `src/shardkv/` 的核心设计，重点是**分片随配置在 replica group 之间迁移时如何保证不丢数据、不重复执行、线性一致**。配套实现说明见仓库根 `README.md`。

## 1. 三层结构

```
客户端 Clerk ──► ShardMaster（配置服务，Raft 复制）
                    │  Query 最新配置
                    ▼
            ShardKV replica groups（每组一个 Raft 副本集）
                    │  分片按配置归属，跨组迁移
                    ▼
               本地 KV 状态机（按分片隔离）
```

- **ShardMaster**：把「10 个分片 → gid」的映射作为一份份 `Config` 用 Raft 复制，客户端用 `Query` 读取。
- **ShardKV**：每个 replica group 只服务当前配置里归属自己的分片；配置变更触发分片迁入/迁出。

## 2. ShardMaster 配置服务

| 接口 | 作用 |
|------|------|
| `Join(gids→servers)` | 新增 replica group，触发再平衡 |
| `Leave(gids)` | 移除 replica group，其分片被重新分配 |
| `Move(shard, gid)` | 手工把某分片指定到某 group（用于测试/调优） |
| `Query(num)` | 读取第 `num` 份配置（`-1` 表示最新） |

- **再平衡 `rebalance`**：把所有 `gid` 排序后轮转分配 10 个分片。确定性 → 同一输入永远得到同一份配置，便于测试复现。
- **幂等**：`Op` 带 `CkId`+`Seq`，applier 仅当 `Seq > lastSeq[CkId]` 才应用，天然去重客户端重试。
- **丢失唤醒竞态**：调用方在 `rf.Start` **之前**分配并注册 `NotifyId`，applier 关闭对应 channel 唤醒等待者。若反过来（先 Start 再注册），leader 可能在注册前就应用该条日志、关闭一个尚无监听者的 channel，导致等待者永久挂起。
- **接线要点**：`ShardMaster.RaftRPC` 把 `RequestVote`/`AppendEntries`/`InstallSnapshot` 转交底层 `rf`。测试框架用同一 server id 同时承载两类 RPC，缺了转发则集群选不出 leader。

## 3. ShardKV 数据面

### 3.1 请求门控
`Get`/`PutAppend` 先查 `key2shard(key)`，若 `config.Shards[shard] != gid` 或本组尚未持有该分片（`shards` 中无此分片），返回 `ErrWrongGroup`。`Clerk` 收到后重新 `Query` 配置、改投正确 group 重试。

### 3.2 配置串行推进
`pollConfig` 每隔 ~80ms 向 ShardMaster 拉最新配置，但**仅当**：

1. 本组**无未决迁移**（`pendingIn` 与 `pendingOut` 都为空）；
2. 最新配置号 = 当前 + 1。

二者缺一不可，确保迁移**一次只跨一步**，不会在「旧迁移还没完成」时就叠加下一次重分配，从而避免分片归属错乱与数据丢失。

### 3.3 分片迁移（双路 + GC-after-ack）

```
旧 owner (gid=A)                        新 owner (gid=B)
─────────────────                      ─────────────────
applyNewConfig: oldG==A,newG==B         applyNewConfig: oldG==A,newG==B
  → pendingOut[s]=true                    → 若已有 incoming 则装入；否则 pendingIn[s]=true + fetchShard 拉取
  → go sendShard(s, B)
        │                                        │
        │ SendShard(分片数据) ────────────────► │ 在本组 Raft 提交 InstallShard
        │                                        │   → 回 ack (OK)
        ▼                                        │
  收到 ack → 本组提交 GCShard(s)                  │
  → delete(shards, s)  ✔ 安全回收                 ▼
                                            shards[s] 生效
```

- **推送 `SendShard`**：旧 owner 主动把分片数据发给新 owner；新 owner 必须在**本地 Raft 提交** `InstallShard` 后才回 `OK`。
- **拉取 `GetShard`/`fetchShard`**：新 owner 也可主动从旧 owner 拉取（双路互为兜底）。`GetShard` 仅 leader 响应，避免从落后的 follower 拉到空/陈旧数据导致丢数据。
- **GC-after-ack**：旧 owner 收到 ack（意味着新组已提交该分片）后才在本组提交 `GCShard` 回收。保证「对方落盘之前本组不丢」。

### 3.4 边界情况

- **从未分配的分片（`oldG == 0`）**：初始配置所有分片归 gid 0（哨兵=未分配），没有旧 owner，直接初始化空 `ShardData`，不触发迁移。
- **旧 owner 已被 Leave 移除**：拉取旧 owner 名单必须取自 **`prevConfig`**（本版配置里该 gid 的 `Groups` 可能已空）。
- **配置反复横跳**：`sendShard` 每轮检查 `config.Shards[s] == newG`，若归属又变则退出，由新配置重新发起，避免向错误目标发送。
- **`InstallShard` 先于 `NewConfig` 到达**：数据先缓冲到 `incoming`，待 `applyNewConfig` 推进到拥有该分片时再装入——配置与数据解耦，顺序无关。

### 3.5 跨迁移客户端幂等
每个分片独立保存 `LastSeq`/`LastResult`（`ShardData`），命令按 `(ClientId, Seq)` 去重；迁移前后客户端重试不会重复执行 Put/Append。

### 3.6 快照
KV 状态（各分片 `ShardData` + 当前/上一版 `Config` + 迁移中的 `incoming`/`pendingIn`/`pendingOut`）经 gob 压缩进 Raft 快照；`installSnapshot` 在重启或落后追赶时一次性恢复，保证迁移中的在途状态也不丢。

## 4. 验证清单

> 本环境若缺少 Go 工具链，下列验证需在装有 Go 1.22+ 的机器或 CI（GitHub Actions，ubuntu + go）上完成。

- [ ] `go vet ./src/shardmaster/... ./src/shardkv/...`
- [ ] `go test ./src/shardmaster/...`（Join/Leave/Move/Query 组合下配置正确、线性一致）
- [ ] `go test ./src/shardkv/...`（单组读写、多组迁移、并发迁移下无数据丢失、线性一致）
- [ ] `go test -race ./src/shardkv/...`（竞态检测，耗时更长）

CI 工作流 `.github/workflows/ci.yml` 已覆盖 `vet` + `test` + `race`，推送后自动执行。
