# 可观测性参考（统一总览）

> 本文是 **raft-kv** 可观测能力的**单一权威入口**：把散落在 `README.md` / `docs/usage.md` /
> `docs/architecture.md` / `docs/runbook.md` 的端点、指标、响应头、健康评分汇总到一处，
> 并给出 Prometheus 采集与告警配方。细节仍以各文档为准，本文做「查表 + 配方」。
>
> 所有指标经网关 `GET /metrics` 一次性暴露（聚合 `shardkv.Metrics` + `shardmaster.Metrics`
> + 网关自身 `Metrics` 三套注册表，见 §1）。

---

## 1. 指标出口与格式

- **端点**：`GET /metrics`（默认 `:8080`）。
- **格式协商**（按 `Accept` 头）：
  - 含 `text/plain` / `prometheus` → **Prometheus 文本 exposition**（`# HELP`/`# TYPE` + 指标行），可被 scrape 客户端采集；
  - 其它 → **JSON 快照**（`shardkv` 顶层计数/直方图 + `{"shardmaster":…,"gateway":…}` 子键）。
- **直方图分位**：`op_latency_ms` / `http_request_latency_ms` / `shard_migration_ms` / `shard_bytes` / `send_shard_latency` / `transport_rpc_latency_ms` 等均输出 `min`/`max`/`p50`/`p95`/`p99`；空直方图分位置零（非 ±Inf），避免 Prometheus 拒绝。
- **命名空间**：按组件加前缀——`raft_*` / `kv_*` / `shardkv_*` / `sm_*` / `gw_*` / `gateway_*` / `http_*` / `transport_*`，单一注册表便于一次性 scrape 且避免同名冲突。

---

## 2. 指标目录（按子系统）

### 2.1 `raft`（共识核心，每副本一份）

| 指标 | 类型 | 含义 / 告警提示 |
|------|------|------|
| `raft_log_appends_total` | Counter | leader 累计追加日志条目数（写入吞吐） |
| `raft_term_changes_total` | Counter | 累计任期变更次数（选举发起与退位）；**频繁翻滚 = 脑裂 / 网络抖动信号** |
| `leadership_transfers` | Counter | 领导权转移（LeadershipTransfer）次数 |
| `leader_changes` | Counter | 领导变更次数 |
| `snapshots_installed` | Counter | 安装快照次数 |
| `log_applied` | Counter | 已应用日志条目数 |
| `transport_rpc_errors` | Counter | 框架层 handler 返回错误的 RPC 次数 |
| `transport_rpc_latency_ms` | Histogram | RPC 框架层延迟（按方法拆分） |

### 2.2 `kvraft`（Lab3 KV 状态机，被复用验证）

| 指标 | 类型 | 含义 |
|------|------|------|
| `ops_total` / `ops_errors` | Counter | 操作总数 / 错误数 |
| `op_latency_ms` | Histogram | 操作延迟 |
| `entries_applied` / `snapshots_installed` | Counter | 已应用条目 / 安装快照 |
| `read_leases` | Counter | ReadIndex 快读命中（线性一致低延迟读占比） |
| `kv_data_keys` / `kv_sessions` | Gauge | 状态机数据键数 / 去重会话表大小（近似，多实例共享注册表时取最后写入者） |
| `gc_sweeps_total` / `gc_sessions_evicted_total` | Counter | 会话 GC 扫描次数 / 累计回收会话数（排查「会话表只增不减」类泄漏，见 runbook §6.4） |

### 2.3 `shardkv`（数据面，迁移与分片）

| 指标 | 类型 | 含义 / 告警提示 |
|------|------|------|
| `ops_total` / `ops_errors` | Counter | 操作总数 / 错误数 |
| `op_latency_ms` | Histogram | 操作延迟 |
| `read_leases` | Counter | ReadIndex 快读命中 |
| `entries_applied` / `snapshots_installed` / `snapshots_taken` | Counter | 已应用 / 安装快照 / 主动快照 |
| `apply_lag` | Gauge | `commitIndex - lastApplied`（状态机 apply 滞后；持续 >0 提示慢 apply） |
| `config_changes` / `config_num` | Counter / Gauge | 配置变更次数 / 当前生效配置号 |
| `config_stalls` | Counter | 迁移卡滞看门狗触发次数（自愈兜底信号） |
| `shardkv_pending_in` | Gauge | 本组待接收分片数（已配置但数据未到位），**长期 >0 提示迁移卡死** |
| `shardkv_pending_out` | Gauge | 本组待迁出分片数（已不再拥有但数据未推走），同上 |
| `shardkv_shards_owned` | Gauge | 本组当前实际持有分片数 |
| `shardkv_pending_total` | Gauge | 本组迁移积压总量（`pending_in`+`pending_out`），**阈值告警主指标（#229）** |
| `shard_migration_ms` | Histogram | 分片入站迁移端到端耗时（从 `pendingInSince` 起算） |
| `shard_bytes` / `shard_bytes_overflow` | Histogram / Counter | 分片字节分布 / 超大分片告警 |
| `send_shard_latency` | Histogram | 每跳迁移（`SendShard`）延迟 |

### 2.4 `shardmaster`（控制面，自身跑在 Raft 上）

| 指标 | 类型 | 含义 |
|------|------|------|
| `sm_config_applied_total` | Counter | 累计应用的配置变更次数 |
| `sm_<kind>_total` | Counter | 按 kind 细分：`sm_join_total` / `sm_leave_total` / `sm_move_total` / `sm_query_total` |
| `sm_rebalance_moves_total` | Counter | 因配置变更而重新分配的分片数（rebalance 幅度） |
| `sm_config_num` | Gauge | 当前生效的配置版本号（Num） |
| `sm_propose_errors_total` | Counter | propose 失败次数（非 OK 返回） |
| `sm_invalid_args_total` | Counter | 参数校验失败被拒的配置变更次数 |
| `sm_queries_total` | Counter | Query 读取配置次数 |

### 2.5 `gateway`（HTTP 面，聚合上述三套 + 自身）

| 指标 | 类型 | 含义 |
|------|------|------|
| `gw_uptime_seconds` / `gw_goroutines` | FuncGauge | 进程运行时长 / 当前 goroutine 数（goroutine 暴涨 = 泄漏前兆） |
| `http_requests_total{method}` / `http_request_latency_ms` | CounterVec / Histogram | HTTP 请求数（按 `method` 切片，可算分方法 QPS）/ 处理延迟 |
| `http_responses_total{code,method}` | CounterVec | HTTP 响应数，按 `code`+`method` 切片（取代旧 `http_responses_<code>` 式独立指标名）。**错误率** = `sum(http_responses_total{code=~"5.."}) / sum(http_requests_total)`；304 等亦纳入此指标（`code="304"`，不再单独计数） |
| `gateway_concurrent_in_use` | Gauge | 当前在途请求数（对照 `max_concurrent=64`；持续打满 = 后端变慢或上游并发过高） |
| `gateway_ratelimit_concurrent_total` | Counter | 全局并发限流触发次数（429） |
| `gateway_ratelimit_client_total` | Counter | 单客户端令牌桶限流触发（429） |
| `gateway_ratelimit_route_total` | Counter | 按路由限流触发（429） |
| `gateway_breaker_trips_total` / `gateway_breaker_open` / `gateway_breaker_rejects_total` | Counter / Gauge / Counter | 熔断跳闸次数 / 当前是否打开 / 熔断拒绝请求数 |
| `gateway_response_bytes` | Histogram | 响应体传输字节分布 |
| `raft_min_health_score` | Gauge | 共识健康 **min** 评分（#223）：任一副本 `RaftCheck` 不变量被破坏即拉低，供阈值告警 |

> **指标库原语**：除普通 `Counter`/`Gauge`/`Histogram` 外，指标库还提供带标签的
> `CounterVec` / `GaugeVec`（见 `src/metrics`），适合「同一指标按 `code`/`method` 等维度切片」
> 的场景（如 `http_responses_total{code,method}`）。`WithLabelValues` 读路径走 `atomic.Value`
> 只读快照，**免锁、免分配**（#119 优化），高并发每请求取标签序列不再成为瓶颈。

---

## 3. 诊断端点（HTTP）

权威端点表见 `docs/architecture.md` §2；运维排障 SOP 见 `docs/runbook.md`。与可观测性最相关者：

| 端点 | 用途 |
|------|------|
| `GET /metrics` | 指标聚合出口（见 §1） |
| `GET /status` | 集群健康总览 JSON：`healthy` 标志 + 每 group leader/config/持有/待收/待迁/孤儿中转计数 + 卡滞秒数；`statusfmt` 渲染为表格并给 `health_score`/`balance_score`（退出码 2 = STALLED，可接 CI/巡检） |
| `GET /debug/shards` | 每副本分片归属 + 迁移态（`pending_in`/`pending_out`/`incoming`）+ 卡滞时间戳；**含 `#230` `diagnosis` 数据面自检段**（`pendingIn`∩`pendingOut` 自相矛盾 / `Owned` 重复 / `StallSeconds>60s` 等不变量） |
| `GET /debug/raft` | 共识健康汇聚：各副本 `RaftStatus` + `diagnostics.RaftCheck` 自检（角色/任期/认知 leader/租约/不变量），一眼看脑裂/任期翻滚/apply 落后 |
| `GET /debug/migrate` | 纯文本迁移进度（`pendingIn`/`pendingOut`/`incoming` 分布 + 最新 config 号），供 `./start.sh migrate` |
| `POST /debug/migrate-plan` | 配置变更 **dry-run**：提交 `current` + `PlanOp`，返回目标配置 / 结构错误 / 演进错误 / 迁移步骤（`shardmaster.Plan` 内存模拟，不触碰 Raft） |
| `GET /debug/configs` / `/debug/groups` / `/debug/config` | 配置历史 / group 成员与分片归属 / 当前生效配置快照（脱敏） |
| `GET /debug/version` | 构建信息（`version.LoadFromBuildInfo()` 自动补全 commit/build_time） |
| `GET /debug/routes` | 已注册路由清单 |
| `GET /debug/accesslog` / `/debug/log` | 进程内访问日志环形缓冲 / 分级结构化日志（`?level=&limit=`） |
| `GET /healthz` | 存活探针（恒 200） |
| `GET /readyz` | 就绪探针：每 group 有「持租约 leader」且无迁移卡滞才 200，否则 503（k8s `readinessProbe`） |

---

## 4. 可观测响应头（经 `wrap` 统一注入，所有路由受益）

| 头 | 含义 |
|----|------|
| `X-Request-ID` | 请求链路 ID（入站缺则 `crypto/rand` 生成 16 位 hex；访问日志/结构化日志均记录，便于链路追踪） |
| `X-Process-Time` | 服务端处理耗时（TTFB 口径，毫秒，三位小数） |
| `X-Response-Size` | 响应体**传输（wire）**字节数（gzip 开启时为压缩后字节）；直方图 `gateway_response_bytes` |
| `X-Request-Size` | 入站请求体声明大小（`Content-Length`；分块上传 `-1` 跳过，避免无意义负值） |
| `X-RateLimit-Limit` / `X-RateLimit-Remaining` / `X-RateLimit-Reset` | 每客户端/路由限流额度 / 剩余 / 重置时刻（限流开启时下发，客户端据此退避） |
| `X-Content-Type-Options: nosniff` / `X-Frame-Options: DENY` | 安全头（防 MIME 嗅探 / 点击劫持） |

---

## 5. 健康评分汇总

| 评分 | 来源 | 用途 |
|------|------|------|
| `health_score` / `balance_score` | `statusfmt.clusterHealthScore` / `shardBalance`（纯函数，0–100） | `./start.sh status` 表格摘要；CI/巡检判活（退出码 2 = STALLED） |
| `raft_min_health_score` | 网关 `GET /metrics` Gauge（#223，min 语义） | Prometheus 阈值告警 |
| `RaftCheck.Score` | `diagnostics.RaftCheck()`，每副本 0–100（不变量被破坏扣分） | 单节点运行时健康；`Score<100` 即排查 |
| `ShardCheck`（`diagnosis`） | `/debug/shards` 内字段（#230） | 数据面迁移不变量自检（人类可读版，与 §6 数值告警互补） |

---

## 6. Prometheus 采集与告警配方

### 6.1 scrape

```yaml
scrape_configs:
  - job_name: raft-kv
    metrics_path: /metrics
    # 网关默认 :8080；Accept 默认即 Prometheus 文本（或显式带 Accept: text/plain）
    static_configs: [{ targets: ["gateway:8080"] }]
```

> 若需 JSON 快照（如进 ELK），用不带 `prometheus` 的 `Accept` 拉取 `/metrics` 即可。

### 6.2 告警规则（示例）

```yaml
groups:
  - name: raft-kv
    rules:
      # 迁移积压未清零 = 再平衡冻结/卡滞（#229，最灵敏信号，早于 config_stalls）
      - alert: ShardKV_MigrationStalled
        expr: max by (instance) (shardkv_pending_total) > 0
        for: 5m
        annotations: { summary: "ShardKV 迁移积压未清零（疑似再平衡冻结）" }

      # 共识健康评分被拉低 = 某副本不变量破坏（#223）
      - alert: RaftHealthDegraded
        expr: raft_min_health_score < 100
        for: 2m
        annotations: { summary: "Raft 共识健康评分 < 100（疑似脑裂/任期翻滚/apply 落后）" }

      # 客户端熔断打开 = 下游持续失败
      - alert: GatewayBreakerOpen
        expr: gateway_breaker_open == 1
        for: 1m
        annotations: { summary: "网关客户端熔断已打开" }

      # apply 滞后 = 状态机落后提交点
      - alert: ShardKV_ApplyLag
        expr: shardkv_apply_lag > 1000
        for: 3m
        annotations: { summary: "ShardKV apply 滞后过大" }
```

> 阈值取经验值，请按实际集群规模与 churn 频率校准。迁移冻结根因已于 cycle 48 + `GetShard` 安全回退**根治**（见 `docs/lab4-shardkv-design.md` §7），故 `shardkv_pending_*` 持续 >0 现在主要作为**回归/异常**信号，而非常态。

---

## 7. 关联文档

- [`docs/architecture.md`](architecture.md) —— 系统架构与权威端点表（§2）
- [`docs/usage.md`](usage.md) —— 各组件使用指南 / 命令表
- [`docs/runbook.md`](runbook.md) —— 运维排障 SOP（含 §7 迁移积压告警细则）
- [`docs/lab4-shardkv-design.md`](lab4-shardkv-design.md) —— ShardKV 深层设计笔记（含 §7 冻结根治）
- [`docs/coverage.md`](coverage.md) —— 测试覆盖与近期迭代清单
