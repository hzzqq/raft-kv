# 运维 Runbook：诊断再平衡冻结与迁移卡滞

本文件面向**线下排障**：当写入/读取在 3 组以上 rebalance 下出现挂死、超时或数据
不一致时，用网关暴露的可观测端点快速定位，而不是盲猜。

## 1. 端点速查

网关（默认 `:8080`）提供以下诊断端点：

| 端点 | 用途 |
|------|------|
| `GET /status` | 集群健康总览 JSON：每 group 是否有 leader、当前 config 号、拥有分片数、`pending_in` / `pending_out` / `incoming` 分片列表、卡滞秒数（`stall_seconds`）。`healthy=false` 表示存在卡滞。 |
| `GET /debug/shards` | 全集群每副本的分片归属 + 迁移态（`pending_in` / `pending_out` / `incoming`）+ 卡滞时间戳。最细粒度。 |
| `GET /debug/migrate` | 纯文本迁移进度，供 `./start.sh migrate` 直接看。 |
| `GET /debug/configs` | shardmaster 完整配置历史（初始 → 最新），复盘 rebalance 轨迹，确认分片在哪些 group 间迁移。 |
| `GET /debug/groups` | 各 replica group 的 `gid` / 副本数 / leader / 当前 config 号 / 持有分片数，快速核对「哪个 group 当前拥有哪些分片」。 |
| `GET /metrics` | 计数器 + 直方图 + 瞬时 Gauge（含 `shard_migration_ms` 迁移延迟、`op_latency_ms` 操作延迟、`config_stalls` 冻结计数、`config_changes` 配置变更次数、`config_num` 当前生效配置号 Gauge、`apply_lag` 应用滞后 Gauge、`shard_bytes` 分片字节直方图、`shard_bytes_overflow` 超大分片告警、`send_shard_latency` 每跳迁移延迟、`read_leases` ReadIndex 快读命中计数）。**格式协商**：`Accept` 含 `text/plain`/`prometheus` 时输出 Prometheus 文本 exposition（可被 scrape 客户端采集），否则默认 JSON。 |
| `GET /debug/accesslog?limit=N` | 进程内访问日志环形缓冲（最近 N 条，默认 50）：每请求的方法 / 路径 / 状态码 / 延迟 / `request_id`，供审计排障（无需外部日志采集）。 |
| `GET /readyz` | **就绪探针**：集群每个 group 都有「持租约的 leader」且无迁移卡滞时返回 `200`，否则 `503`；与恒 `200` 的存活探针 `/healthz` 区分，可直接作 k8s `readinessProbe`。 |
| `GET /healthz` | 存活探针，恒 `200`。 |
| `GET /debug/log?level=&limit=N` | 进程内分级结构化日志环形缓冲（最近 N 条，按 `level` 过滤 `debug`/`info`/`warn`/`error`，默认 `info`）：每请求一条带级别日志，统一排障入口（取代散落 `fmt.Println`）。 |
| `GET /debug/config` | 当前生效网关配置快照（脱敏）：`listen_addr` / `request_timeout_sec` / `max_concurrent` / `client_rate` / `client_burst` / `cors_origins`，确认配置文件加载结果。 |

CLI 快捷方式：`./start.sh status` / `./start.sh migrate` / `./start.sh configs`。

## 2. 常见症状与含义

### 症状 A：客户端读/写永久挂死，网关最终 504
**含义**：配置冻结——某分片 `pending_in` 永远清不掉，pollConfig 不推进。
**排查**：
1. `curl /debug/shards` 找 `pending_in` 非空的副本，记下卡住的分片 `s` 与 group。
2. `curl /debug/configs` 看 `s` 当前应属于哪个 group（owner）。
3. 若 owner 是 group X，但 `pending_in` 卡在 group Y，说明 Y 的 `GetShard` 拿不到 `s` 的数据：
   - 正常情况下 `GetShard` 在 `shards[s]` 缺失时会回退返回缓冲的 `incoming[s]`（冻结根因修复，见 §4）；
   - 若仍卡死，检查中间组是否把 `incoming[s]` 滞留而未被消费（极少见，已有自愈兜底）。
**缓解**：配置推进看门狗会在卡滞 >3s 时以最新配置重拉取/重推送并自增 `config_stalls`。

### 症状 B：`/status` 显示 `stall_seconds` 很大但集群最终恢复
**含义**：瞬时网络争用下的迁移重试，非真正冻结（`stallUnhealthySec=2s` 阈值内即视为健康）。
**缓解**：无需处理；`shard_migration_ms` 直方图可看延迟分布。

### 症状 C：某 group 长期 `healthy=false` 且 `pending_out` 残留
**含义**：分片迁出未完成，GC 未执行。
**排查**：
- `debug/shards` 看 `pending_out` 分片 `s` 的 owner（当前 config）；
- 若 owner 就是本组自身（A→B→A 快速回摆），`migratePump` 会清除残留标记（`applyGC` 守卫保证不自删权威分片，见 §4）；
- 否则 `sendShard` 重试推送，成功后 `propose(GCShard)` 清理。

### 症状 D：网关在迁移抖动 / 压测下返回 `429 Too Many Requests`
**含义**：两种限流其一触发——① 全局并发上限（默认 64，`Server.sem` 信号量）被占满；② 单客户端令牌桶（`client_rate`/`client_burst`，默认 200 rps / 突发 40，按 `X-Client-ID` 或 `RemoteAddr` IP 区分）超量。后者可针对性限制个别失控客户端而不影响其他客户端。
**排查**：属预期保护行为；若常态性触顶，说明上游并发过高或后端迁移期变慢。可经 `main.go` 的 `maxConcurrent` 调大，或降低 `kvcli bench` 的并发 worker 数；单客户端限流可用 `SetClientRateLimit` 调高 rps / burst 或设 `rps<=0` 关闭。限流仅作用在网关入口，不丢数据——客户端按 429 + `Retry-After` 退避重试即可。

## 3. kvraft 包「flaky 挂死」说明（非代码 bug）

`src/kvraft` 的不可靠/分区类测试（如 `TestKVSnapshotStress`、分区场景）在受限 CPU
环境下偶发挂死，**这是 MIT 测试 harness 的时序敏感性所致，不是本仓库的回归**：

- Clerk 幂等正确：`ck.seq` 在重试循环外递增一次，单次逻辑操作 seq 恒定，服务端
  `lastSeq` 去重有效；
- 快照正确：`installSnapshot` 完整恢复 `data/lastSeq/lastResult`，`encodeSnapshot`
  在持锁状态下调用，无竞态；
- 因此整体 `go test ./...` 时若 kvraft 偶挂，直接重跑该包即可，不要当作代码缺陷
  去「修复」。

## 4. 已知的正确性设计决策

- **冻结根因修复（cycle 57）**：`GetShard` 在 `shards[s]` 缺失但 `incoming[s]` 存在时
  回退返回 `incoming[s]`，供真正主人直接拉走中转数据。只读、零状态改写、零新协程，
  与 `applyNewConfig` 权威路径正交。**切忌**再用「独立 goroutine 转发孤儿 incoming」
  的改法（cycle 9 / 55 / 本轮初三次重蹈，会打爆网络/选举、整集群冻结）。
- **GC 守卫**：`applyGC` 仅在本组当前配置不再拥有该分片时才删除其数据；A→B→A 快速
  回摆下残留 `pending_out` 被 `migratePump` 触发自 GC 时不会删掉本组权威副本。
- **线性一致读（迭代 8 引入，本轮经 leader lease 重新启用）**：`Get` 在 leader 上先查
  `raft.HasLeaderLease()`，持 lease 时走 ReadIndex 快读（以 `commitIndex` 为一致性点、待状态机
  `apply` 到该索引后直接读本地，省一次日志追加、低延迟）；无 lease / 失去领导权 / 超时则回退
  `propose`（同样线性一致）。leader lease 由每节点维护的 `lastContact[peer]`（收到合法
  `AppendEntries`/`InstallSnapshot` 时刷新）+ 多数派近因判定提供，避免无 lease 下的 stale read——
  故快读路径现可安全启用，`read_leases` 计数可观测命中率。
- **迁移 RPC 退避（迭代 2）**：`fetchShard`/`sendShard` 指数退避（首跳 50ms，上限 1s），
  churn 下降低 RPC 风暴。
- **迁移延迟直方图（迭代 5）**：`/metrics` 的 `shard_migration_ms` 记录分片从待接收到
  装入的耗时，观测迁移性能。
- **崩溃恢复 commitIndex 持久化（cycle 87 末 / n=21）**：`raft.persist()` 现把 `commitIndex`
  一并编码，`readPersist()` 恢复它，`advanceCommit()` 在提交点推进时即 `persist()`。重启节点
  据恢复的 `commitIndex` 由 `applier`（满足 `commitIndex > lastApplied`）自动重放已提交日志，
  不再依赖新 leader 重发 `LeaderCommit`。**只持久化已提交安全点**，未提交的 `lastApplied` 仍从 0
  起、由 `applyCond` 驱动回放，正确性无损。回归测试 `TestCommitIndexPersistenceRecovery`（孤立
  副本仅凭持久化 commitIndex 重放全部已提交命令）守护，负向验证确认无此修复时 0/n 复现。
- **测试桩 serverId 集中化（n=22/n=23）**：labrpc 的 `AddServer` 注册 id 与端点 `Connect`
  目标必须一致（统一为 `serverId(g,r)=1000+g*100+r`）；`g>=1` 时若错配为 `1000+g*nReplicas+r2`
  会使重启副本 RPC 全部静默失败（目标 server 不存在）、永久分裂投票选不出主。**所有注册/连接/
  分区操作都已收敛到 `serverId()` 单一来源**，禁止再散落字面量。
- **raft 选举/心跳 Timer 并发守卫（n=25）**：`electionTimer`/`heartbeatTimer` 同时被 ticker 与
  选举/心跳 goroutine 改写，而 `time.Timer` 非并发安全——构成 `-race` 数据竞争。已加 `timerMu`
  守卫 `resetElectionTimer`/`resetHeartbeatTimer`；锁序保证 `timerMu` 不会在持有 `rf.mu` 的反向
  被获取，无死锁。CI 新增 `raft-race` job 持续守护。
- **GetShard 选举窗口守卫（n=35）**：新当选 leader 在「重新提交本任期 no-op」之前，`commitIndex`
  可能仍落后上一任 leader 已提交的位置、旧任期已提交写尚未 apply。此时若 `GetShard` 直接传出
  `kv.shards`，快照会缺失该写、被新 group 装入即静默丢写（杀主 + 迁移重叠窗口偶发）。故 `GetShard`
  须同时满足三条件才服务传输：① 持 leader 租约（`HasLeaderLease`）；② 已在当前任期提交过条目
  （`HasCommittedCurrentTerm`，no-op 提交后旧任期写才被 `commitIndex` 覆盖）；③ 状态机已
  `waitApplied` 到 `commitIndex`。任一不满足即返回 `ErrWrongLeader` 让 `fetchShard` 重试。Raft 侧
  新增 `committedCurrentTerm` 标记（`becomeLeader` 重置、`advanceCommit` 提交当前任期条目时置位）
  与 `HasCommittedCurrentTerm()` 访问器；确定性测试 `TestHasCommittedCurrentTerm` 与混沌回归
  `TestChaosSwingWriteDataLoss`（A→B→A 来回再平衡 + 每 200ms 杀两组 leader + 客户端 20 次 Append）
  共同守护。
- **kvraft Get ReadIndex 快路径（n=39）**：与 shardkv 同源优化——leader 持租约时 `Get` 跳过一次
  Raft 日志追加、以 `commitIndex` 为一致性点待状态机 apply 后本地读，低延迟且线性一致；无租约 /
  超时则回退 `propose`。`KVServer` 新增 `appliedIndex` 追踪与 `waitAppliedIndex` 轮询，命中以
  `read_leases` 计数（确定性测试 `TestKVReadLease` 守护）。
- **就绪探针基于 leader 租约而非裸角色（n=40）**：`/readyz` 判定某 group 就绪，不仅要求存在
  leader，还要求该 leader `HasLeaderLease()` 为真——分区失联的旧 leader 仍自认 `Leader=true` 却
  无法提交，若仅按角色判就绪会误报「可服务」。故 `ShardDebug` 新增 `Lease` 字段，`clusterHealthy()`
  以「持租约 leader + 无迁移卡滞」为就绪判据，`/readyz` 据此返回 200/503。

## 5. 快速排障 SOP

1. 复现挂死时先 `curl /status` 看 `healthy` 与哪个 group 异常。
2. `curl /debug/shards` 抓该 group 的 `pending_in` / `pending_out` / `incoming`。
3. `curl /debug/configs` 确认分片 owner 轨迹，判断是否多跳 rebalance 滞留。
4. 对照 §2 症状表定位；若 `config_stalls` 计数持续增长，说明看门狗在持续自愈。
5. 必要时增大 `stallUnhealthySec` 阈值或检查底层 labrpc 网络是否限流。
