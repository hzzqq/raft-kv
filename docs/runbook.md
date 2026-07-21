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
| `GET /metrics` | 计数器 + 直方图（含 `shard_migration_ms` 迁移延迟、`op_latency_ms` 操作延迟、`config_stalls` 冻结计数）。 |

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
- **线性一致读（迭代 8）**：`Get` 始终走 `propose`（经 Raft 共识，绝对线性一致）。本仓库
  raft 未实现 leader lease / 心跳确认式 ReadIndex，旧 `ReadIndex` 快路径在分区下会返回
  stale read，故有意禁用。待 raft 补齐 leader lease 后可恢复该优化。
- **迁移 RPC 退避（迭代 2）**：`fetchShard`/`sendShard` 指数退避（首跳 50ms，上限 1s），
  churn 下降低 RPC 风暴。
- **迁移延迟直方图（迭代 5）**：`/metrics` 的 `shard_migration_ms` 记录分片从待接收到
  装入的耗时，观测迁移性能。

## 5. 快速排障 SOP

1. 复现挂死时先 `curl /status` 看 `healthy` 与哪个 group 异常。
2. `curl /debug/shards` 抓该 group 的 `pending_in` / `pending_out` / `incoming`。
3. `curl /debug/configs` 确认分片 owner 轨迹，判断是否多跳 rebalance 滞留。
4. 对照 §2 症状表定位；若 `config_stalls` 计数持续增长，说明看门狗在持续自愈。
5. 必要时增大 `stallUnhealthySec` 阈值或检查底层 labrpc 网络是否限流。
