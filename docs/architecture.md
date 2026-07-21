# 系统架构（raft-kv 全栈）

> 本文是 **raft-kv** 的代码地图：从底层的 Raft 共识核心，到上层的 HTTP 网关、命令行客户端与全栈演示。它回答"每个包负责什么、它们之间怎么连、一个请求从进来到落盘经过哪几层"。
>
> 与之互补的两份文档：
> - `docs/lab4-shardkv-design.md` —— ShardKV 数据面的**深层设计笔记**（迁移状态机、边界情况、并发坑、已知风险）。
> - `docs/usage.md` —— 各组件的**使用指南 / 命令表**。
>
> 本文不重复它们的细节，只给全局视角。

---

## 1. 一句话定位

`raft-kv` 是 MIT 6.824 Lab 4（ShardKV）的一个**完整、可运行、可观测**实现：在一组 Raft 复制组之上，构建一个**分片（sharded）、容错、线性一致**的键值存储，并额外提供 HTTP 网关、CLI 客户端、全栈演示与指标可观测性。

---

## 2. 分层架构

```
┌──────────────────────────────────────────────────────────────────────┐
│                          用户 / 测试 / 脚本                            │
│   (go test · kvcli · demo · curl · GitHub CI · 浏览器看 /metrics)     │
└───────────────┬──────────────────────────┬───────────────────────────┘
                │ HTTP                      │ in-process Clerk (labrpc)
                ▼                           ▼
        ┌───────────────┐          ┌──────────────────────┐
        │  gateway      │          │  cluster (harness)    │
        │  (HTTP REST)  │          │  可复用 in-proc 集群   │
        └───────┬───────┘          └───────────┬──────────┘
                │                              │
                │  ShardKV.Client / Clerk      │
                ▼                              ▼
        ┌───────────────────────────────────────────────┐
        │              ShardKV 数据面 (src/shardkv)        │
        │   分片路由 · 迁移状态机 · ReadIndex 线性读 · 快照   │
        └───────────┬───────────────────────┬─────────────┘
                    │ 配置查询/变更            │ 复制 (AppendEntries)
                    ▼                         ▼
        ┌────────────────────┐     ┌─────────────────────────┐
        │  ShardMaster       │     │   Raft 共识核心          │
        │  (config service)  │     │   (src/raft, 每副本一份)  │
        └─────────┬──────────┘     └───────────┬─────────────┘
                  │ 自身也跑在 Raft 上            │ 持久化 + 快照
                  ▼                              ▼
            ┌──────────────────────────────────────────┐
            │   raft.Persister  (磁盘/内存持久化状态)      │
            └──────────────────────────────────────────┘

旁挂（additive，不改变上述主链路语义）：
        metrics  —— 零依赖并发安全计数器 + 直方图，挂在 kvraft / shardkv / raft 热路径
        kvcli    —— 纯 HTTP 客户端 + CLI（含 bench 压测）
        demo     —— 把"in-process Clerk"与"HTTP 网关"两条路径都演示一遍
```

### 层级职责一览

| 层 | 包 | 职责 | 关键类型 / 入口 |
|----|----|------|----------------|
| 共识核心 | `src/raft` | 领导者选举（Pre-Vote 预投票）、日志复制、持久化、快照、ReadIndex、LeadershipTransfer 领导权转移 | `raft.Make` · `ReadIndex` · `Start` · `RequestPreVote` · `TimeoutNow` |
| 单组 KV | `src/kvraft` | Lab 3 线性一致 KV（本仓库作为 Raft 落地的早期验证，ShardKV 复用了其模式） | `KVServer` · `Op` |
| 配置服务 | `src/shardmaster` | 维护 `Config` 序列（Join/Leave/Move/Query），自身跑在 Raft 上 | `ShardMaster` · `Config` |
| 数据面 | `src/shardkv` | 分片路由、迁移状态机、线性一致读、快照压缩 | `ShardKV` · `MakeShardKV` |
| 集群工具 | `src/cluster` | 可复用的 in-process labrpc 集群封装（测试 / 演示 / 网关都用它起集群） | `StartCluster` · `Clerk` |
| HTTP 网关 | `src/gateway` | 把集群暴露成 REST：`/kv/{key}`(GET/PUT/POST-append) · `/healthz`(存活) · `/readyz`(就绪) · `/metrics`(JSON/Prometheus 协商) · `/status`(集群健康) · `/debug/shards` · `/debug/migrate` · `/debug/configs` · `/debug/groups` · `/debug/accesslog` · `/debug/log`(分级结构化日志) · `/debug/config`(生效配置快照) | `Handler` · `main` |
| 客户端 | `src/kvcli` | HTTP 客户端 + 命令行（get/put/append/bench） | `Client` · `main` |
| 演示 | `src/demo` | 全栈冒烟：Clerk 路径 + HTTP 网关路径 | `main` |
| 可观测 | `src/metrics` | 零依赖 Counter + 有界直方图（p50/p95/p99） | `Registry` · `Snapshot` |
| 状态渲染 | `src/statusfmt` | 把网关 `/status` JSON 渲染为可读表格（CLI `start.sh status` 调用，无 jq/python 依赖） | `main` |

---

## 3. 目录 / 模块树

```
raft-kv/
├── src/
│   ├── raft/          # 共识核心（被 kvraft / shardmaster / shardkv 复用）
│   ├── kvraft/        # Lab 3 线性一致 KV（Raft 落地的早期练习）
│   ├── shardmaster/   # ShardMaster 配置服务（Join/Leave/Move/Query）
│   ├── shardkv/       # ShardKV 数据面（分片 + 迁移状态机 + 线性读）
│   ├── cluster/       # 可复用 in-process 集群 harness（测试/演示/网关共用）
│   ├── gateway/       # HTTP REST 网关（暴露集群为 REST API）
│   ├── kvcli/         # HTTP 客户端 + 命令行 + 压测
│   ├── demo/          # 全栈演示（Clerk 路径 + HTTP 网关路径）
│   └── metrics/       # 零依赖并发安全指标库
├── docs/
│   ├── architecture.md      # 本文：系统架构地图
│   ├── lab4-shardkv-design.md  # ShardKV 深层设计笔记
│   └── usage.md             # 各组件使用指南
├── ITERATION_RULES.md       # 自主迭代（self-driving）规则
├── run-tests.sh             # 托管 Go 工具链下跑测试
├── Makefile                 # build-binaries / demo / lint / cover
└── .github/workflows/ci.yml # CI（-race + 全栈冒烟）
```

---

## 4. 关键数据流

### 4.1 一条写请求的生命周期（leader 副本）

```
client (kvcli / Clerk)
   │  PUT /kv/foo = bar   (或 ck.Put("foo","bar"))
   ▼
gateway.Handler  ──► ShardKV.Client.PutAppend
   │
   ▼
ShardKV.PutAppend (持 kv.mu)
   ├─ 门控：config 是否生效？gid 是否本组？  ──否──► ErrWrongGroup / 重试
   ├─ 构造 Op{Type, Key, Value, ClientId, Seq}
   ├─ rf.Start(op)  ──► 提交到 Raft 日志
   │       │  （Raft 复制：follower AppendEntries → 多数派提交）
   │       ▼
   ├─ applier goroutine 从 applyCh 取出已提交 Entry
   │       ├─ 幂等去重（clientSeq 表）
   │       ├─ 写入本地状态机 shards[k][key]
   │       └─ 更新 appliedIndex / 指标
   └─ 返回结果（携带可能的 ErrWrongLeader 供客户端换主重试）
```

> **线性一致读（ReadIndex 优化）**：`Get` 走 fast-path——先确认本副本的 `appliedIndex >= commitIndex`（等价于"读到至少与领导者一样新的已提交状态"），命中则直接读本地状态机，省掉一次 Raft 日志追加；若中途失主或超时，回退为重新提议一次 no-op 以保证线性一致。详见 `lab4-shardkv-design.md §3` 与代码 `ShardKV.Get`。

### 4.2 配置变更与分片迁移

```
管理员 / 测试 / churn 脚本
   │  Join(gid, servers) / Leave(gid) / Move(shard, gid)
   ▼
ShardMaster (自身跑在 Raft 上)
   └─ 生成新的 Config{Num, Shards[10], Groups}
          │  Query(Num) 被 ShardKV 轮询
          ▼
ShardKV.pollConfig（串行推进，不抢跑）
   ├─ 发现 config.Num 前进：
   │     ├─ 本组失去的分片 → 置 pendingOut，向新 owner 发 SendShard
   │     ├─ 本组获得的分片 → 置 pendingIn + incoming，启动 fetchShard 拉取
   │     └─ bump fetchEpoch[shard]（让旧 fetcher 自退，见 cycle 33）
   ├─ 收到 SendShard（带深拷贝的状态机切片）→ applyInstallShard → 清 pendingIn
   ├─ ack 给旧 owner → 旧 owner 收齐 ack → 清 pendingOut
   └─ 当 pendingIn/pendingOut 全空 → applyNewConfig → config 正式生效
```

> **已知风险（务必读 `lab4-shardkv-design.md §7`）**：
> - 3+ 组**完整 rebalance**（Join/Leave 混合 churn）可能让某个分片卡在 `pendingIn/pendingOut` 无法清除 → 后续读挂起。Move-based churn 安全。
> - `TestSKVReMigration`（单分片 A→B→A 漂移）**偶发 flaky**（约 40% 概率 `pendingIn=[8]` 冻结），根因同上，只是触发面更广。
> 二者的服务端根因尚未根治，属于最高优先级待办。

### 4.3 崩溃恢复

```
副本进程/goroutine 被杀（Kill）
   │
   ▼  用「同一 raft.Persister」重建：
newRf  = raft.Make(peers, me, SAME_PERSISTER, applyCh)
newKV  = MakeShardKV(gid, sm, make_end, newRf, applyCh, maxraftstate)
net.AddServer(id, handler)   # 重新注册到 labrpc 网络，旧 handler 被替换
   │
   ▼
Raft 从 Persister 恢复：currentTerm / votedFor / log[] / snapshot
   → 重新选主 → 状态机从快照 + 剩余日志回放
   → ShardKV 从持久化的 shards/config/迁移状态恢复
```

`TestSKVPersistRestart` 验证了"整组副本崩溃后用同一 Persister 重启，数据仍可读、可继续写"。

---

## 5. 网络与传输

- **测试 / 演示 / 网关（本仓库内）**：统一走 `labrpc.Network` —— 一个 in-process 的、可注入延迟/断网/分区的 RPC 网络。这让"断网=崩溃""分区=网络隔离"在单进程内可复现（如 `TestGatewayFailFast` 用 `net.Enable(id,false)` 模拟副本失联）。
- **生产形态**：把 `labrpc` 换成真实 gRPC/HTTP 传输即可，数据面 `ShardKV` 的 RPC 方法名（`Get` / `PutAppend` / `SendShard` / `GetShard`）与接口签名保持不变，无需改核心逻辑。`demo` 与 `gateway` 的 in-file 注释均标注了这一点。

---

## 6. 可观测性（metrics）

`src/metrics` 是零依赖、并发安全的：
- `Counter`：单调计数（ops_total / ops_errors / entries_applied / snapshots_installed …）
- `Histogram`：有界环形缓冲，提供 `p50/p95/p99`（`op_latency_ms`）
- `Registry.Snapshot()` / `DumpJSON()` / `StartPeriodicReporter()`

挂载点：
- `gateway` 的 `GET /metrics` 把 `shardkv.Metrics.Snapshot()` 序列化成 JSON（计数器 + 分位延迟）。
- `demo` 每 400ms 往 stderr 流一份指标快照，演示时可见吞吐与延迟。
- `kvcli bench` 报告 ops/sec + p50/p95/p99。

---

## 7. 质量保障分层

| 层 | 手段 | 覆盖 |
|----|------|------|
| 单元 | `go test ./...`（10 个包） | raft / kvraft / shardmaster / shardkv / cluster / gateway / kvcli / demo / metrics / statusfmt |
| 并发 | 内置 map 检测器 + `-count=1` 多轮；CI 在 GitHub（有 gcc）跑 `-race` | kvraft / shardmaster / shardkv(Basic+Move+Concurrent+ReadIndex) |
| 迁移 | 专用矩阵：`ReMigration` · `ConfigProgress`(2组) · `LinearizableAppend` · `SnapshotChurn` · `PersistRestart` | ShardKV 迁移状态机 |
| 全栈 | `make build-binaries` + demo 冒烟；CI `fullstack-smoke` job | cluster → gateway → kvcli |

> 本地环境无 `gcc`，故 `go test -race` 仅在 CI 运行；本地以"并发 map 检测器 + 反复 `-count=1` 跑 churn 用例"作为并发回归替代。

---

## 8. 设计取舍速记

- **配置串行推进**（`pollConfig` 不抢跑）：避免"跳过某版 config 导致分片归属错乱"。代价是配置变更必须等迁移完成，间接放大了 rebalance 卡死时的读挂起（见 §4.2 风险）。
- **迁移双路 + ack-GC**：新 owner 拉取（fetchShard）+ 旧 owner 推送（SendShard）并存；旧 owner 收齐 ack 才清 pendingOut，保证不丢状态。
- **InstallShard 深拷贝**：快照整体覆盖与运行态分片互相独立，杜绝并发 map 竞态（早期 `TestSKVConcurrent` 的崩溃根因）。
- **客户端 1s RPC 超时 + 5s 有界重试**（`GetE/PutE/AppendE`）：集群冻结时网关 fail-fast（504）而非挂死，REST 语义正确。
- **ReadIndex 快读**：leader 命中直接读本地状态机，省一次日志追加，吞吐提升显著。

---

## 9. 待办（自主迭代 backlog）

1. **【最高优先级】根治 pendingIn/pendingOut 冻结**：3 组 rebalance 与 `TestSKVReMigration` flaky 的服务端根因（旧 owner GC 分片导致 fetchShard 死循环 / orphan incoming 不转发）。需要一次迁移状态机的专项整治，而非客户端打补丁。
2. 提升吞吐量：当前基准约 16.6 ops/sec，瓶颈在 Raft 串行化 + 客户端重试退避；ReadIndex 已缓解读路径，写路径仍有空间。
3. 真实传输层（gRPC/HTTP）替换 labrpc，使组件可独立部署。
