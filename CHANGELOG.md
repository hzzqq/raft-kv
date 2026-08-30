# CHANGELOG（自驱开发迭代交付记录）

> 由 `scripts/gen_changelog.py` 从 `.workbuddy/self-driving/state.json` 自动生成。
> 覆盖 cycle 39–150，共 108 轮交付；时间跨度 2026-07-22 ~ 2026-08-30。

按模块聚合；每条含 `task_id`、新增需求（`new_requirement`）、隐性问题（`implicit`）、自评分（`score`）。隐性问题为本轮主动挖掘的非显性缺陷/技术债。

## gateway

- **[47] `gw_ratelimit_headers`** — X-RateLimit-*头（隐性：客户端无感限额；score=15）
- **[48] `gw_graceful_grpc`** — Shutdown关gRPC监听（隐性：gRPC端口泄漏；score=12）
- **[52] `gw_trace_kvcli_close`** — X-Request-Id透传 / Client.Close()（隐性：请求无关联ID / 连接不回收；score=13）
- **[60] `gw_process_time`** — X-Process-Time 处理耗时响应头(TTFB,毫秒三位小数) + 通配兜底路由套 wrap（隐性：wrap 无处理耗时暴露;404 路径绕过 timingWriter 与 X-Request-ID/安全头(Go1.22 无 NotFoundHandler)；score=15）
- **[65] `gw_resp_size`** — X-Response-Size 响应体大小头 + gateway_response_bytes 直方图指标（隐性：网关可观测缺响应体大小维度,带宽/大响应不可监控(R7 均衡);与 X-Process-Time 共用外层 writer；score=13）
- **[66] `gw_migrate_plan`** — POST /debug/migrate-plan 配置变更 dry-run 端点(调用 shardmaster.Plan)（隐性：Plan(#202)纯函数无 HTTP 暴露,运维只能盲提交配置变更(R2 隐性可用性缺口)；score=14）
- **[68] `gw_request_size`** — X-Request-Size 请求体大小头(来自 r.ContentLength,分块-1跳过)（隐性：网关可观测缺请求体大小维度,超大请求/带宽不可监控(与 X-Response-Size 配对)；score=12）
- **[71] `gateway_sem_util`** — gateway 并发上限收敛到 util.Semaphore（隐性：散落裸 make(chan struct{}) 信号量无 ctx 取消/无观测,与 kvcli #207 不一致;util.Semaphore 缺非阻塞 TryAcquire；score=20）
- **[75] `gw_proc_gauges`** — 网关进程级 FuncGauge(uptime_seconds / goroutines) 暴露到 /metrics（隐性：运维无法直接观测进程运行时长与 goroutine 数(泄漏前兆)；score=14）
- **[82] `gw_debug_raft`** — GET /debug/raft 汇聚端点(各副本 RaftStatus + RaftCheck 自检)（隐性：运维需登录各节点才能看共识健康,脑裂/任期翻滚/apply落后无统一视图；score=16）
- **[83] `gw_raft_health_metric`** — /metrics 暴露 raft_min_health_score gauge（隐性：raft 健康此前不可被 Prometheus scrape/告警(仅 JSON 端点)；score=15）
- **[118] `gw_labeled_req_metrics`** — 网关请求指标升级为带标签 CounterVec：http_requests_total{method} + http_responses_total{code,method}（隐性：旧实现用『http_responses_<code>』式独立指标名，缺 method 维度且无法在单指标内切片算错误率/分方法 QPS(R2 隐性可观测缺口)；score=18）
- **[144] `gateway_connect_mode`** — gateway -connect 纯客户端接入模式 + clusterHealthy 远程降级（直连 ShardMaster 取最新 ConfigNum）（隐性：纯客户端模式无本地副本，原 /readyz 遍历本地 KV 句柄会导致空指针/卡死（R2 远程接入可用性缺口）；score=15）

## kvcli

- **[42] `kvcli_gzip`** — gzip透明解压（隐性：不处理压缩响应；score=14）
- **[43] `kvcli_health`** — Ping/Healthy/Ready探针（隐性：无法主动探活；score=14）
- **[44] `kvcli_retryafter`** — Retry-After兑现（隐性：固定退避不尊服务端；score=13）
- **[45] `kvcli_breaker`** — 客户端熔断（隐性：下游雪崩无保护；score=16）
- **[46] `kvcli_metrics`** — 客户端指标（隐性：行为不可观测；score=13）
- **[63] `kvcli_max_concurrent`** — SetMaxConcurrent(n) 限制 MGet/MSet 并发回源 goroutine 数（隐性：MGet/MSet 每 key 起 goroutine 且无上限,超大批量打爆客户端/后端(R2 隐性)；score=14）
- **[67] `kvcli_semaphore_reuse`** — 批量并发收敛复用 util.Semaphore + ctx 取消语义（隐性：#203 手搓 batchSem 裸 channel 重复造轮子,且满信号量+ctx取消会挂死；score=13）
- **[79] `kvcli_batch_workerpool`** — WorkerPool(SubmitCtx) 落地 kvcli MGet/MSet 批量扇出（隐性：批量此前手搓 goroutine+util.Semaphore 样板(Acquire/Release 易漏、goroutine 生命周期分散);maxConcurrent<=0 仍按 key 数开池保留历史无限制语义；score=16）

## util

- **[39] `util_timed_cache`** — TimedCache(TTL并发缓存)（隐性：短期凭据/会话需手动过期；score=18）
- **[40] `util_snow_id`** — SnowID(唯一ID)（隐性：缺趋势递增ID；score=16）
- **[41] `util_json_error`** — JSONError(结构化错误)（隐性：API错误体不统一；score=15）
- **[78] `util_workerpool_ctx`** — WorkerPool.TrySubmit(非阻塞) + SubmitCtx(ctx感知提交)（隐性：原 Submit 持锁阻塞发送有死锁隐患 + tasks 通道被关存在 panic 风险；无 ctx/非阻塞入口致调用方退化为无界 goroutine；score=17）
- **[97] `util_semaphore_racefix`** — 并发无超发/无数据竞争压力测试(TryAcquireWeighted 边界覆盖)（隐性：TryAcquireWeighted 与 InUse 读取 channel len/cap 与并发 send/recv 构成 DATA RACE;InUse 观测读亦非同步；score=17）
- **[99] `util_cb_halfopen_flood`** — 半开探针限额(顺序/单探针/并发三例断言)（隐性：CBHalfOpen 下 Allow() 对全部并发返回 true,Open→HalfOpen 瞬间向恢复中下游倾泻探测流量(惊群)；score=18）
- **[101] `util_errgroup_panic_recover`** — errgroup panic 恢复回归测试（隐性：ErrGroup.Go 单个任务 panic 击穿 goroutine 拖垮进程,批量场景无防护(与 transport.safeCall 哲学不一)；score=16）

## raft

- **[76] `raft_metrics`** — Raft 共识层可观测性补齐(raft_log_appends_total / raft_term_changes_total)（隐性：控制面任期翻转与写入吞吐不可见,排查脑裂/频繁选举无量化依据；score=14）
- **[80] `raft_status_selfcheck`** — Raft.Status() 只读快照 + diagnostics.RaftCheck 不变量自检（隐性：共识层对运维完全不透明(脑裂/任期翻滚/apply落后无信号)；RaftCheck 此前无单测；score=18）
- **[150] `exp_partition_split_brain`** — 真网络分裂（脑裂）故障注入原语 + 场景 B：2+3 分区下不双写、愈合后收敛（隐性：原有分区注入只有 Enable(false)（整节点掉线），造不出「少数派节点仍存活且内部互通、旧 leader 仍自认 leader」的真脑裂——而这才是双写风险最大、最需要被证明的场景；score=18）

## shardkv

- **[49] `shardkv_data_ops`** — Snapshot/Restore/Merge/Subtract（隐性：分片迁移缺标准工具；score=17）
- **[81] `shardkv_raft_status`** — ShardKV.RaftStatus() 透出底层 raft 只读健康快照（隐性：共识层健康封闭在 raft 包内，数据面/运维只能从分片态间接推测(脑裂/任期翻滚/apply落后无一手信号)；score=16）
- **[90] `shardkv_migration_gauges`** — shardkv_pending_in/out/owned/total gauge(迁移积压可告警)（隐性：迁移卡死(pendingIn/pendingOut 长期非零)此前只能看 /debug/shards JSON,无时序指标,Prometheus 无法告警；score=17）

## shardmaster

- **[50] `sm_transition`** — Valid/IsValidTransition（隐性：配置演进无校验；score=15）
- **[62] `sm_plan_preview`** — Plan(current,PlanOp) 配置变更预览器(dry-run)（隐性：此前无 dry-run 校验,运维直提交非法 Join/Leave/Move 易致 rebalance 卡死(R2 隐性)；score=14）
- **[73] `sm_metrics`** — shardmaster 控制面可观测性(Metrics 注册表 + /metrics 暴露)（隐性：控制面 Join/Leave/Move/rebalance/配置版本对运维完全不可见；score=16）

## kvraft

- **[58] `kvraft_snapshot_fix`** — installSnapshot 持锁 + 拒绝陈旧快照 + nil map 归一（隐性：applier 对 appliedIndex 无锁写(竞态) + 陈旧快照会回滚状态机破坏线性一致；score=18）
- **[89] `kvraft_status_finalize`** — KVStatus GCTTL/GCInterval 派生 + 注册表登记断言单测(防 Help 静默 no-op)（隐性：KVStatus.Role 类型错配(raft.Role 赋 string)致全量构建失败,该部分长期以'未验证'滞留工作树；score=19）

## transport

- **[53] `transport_idle_timeout`** — Server.SetIdleTimeout 读空闲超时(半开连接回收)（隐性：serveConn 无读超时,半开/慢速连接永久占用 goroutine(泄漏)；score=17）
- **[54] `transport_warmup_dialto`** — ClientConn.Warmup 连接预热 / SetDialTimeout 建链超时可配（隐性：首RPC建链延迟尖刺 / 5s硬编码不可调(可控性缺口)；score=14）
- **[55] `transport_codec`** — GobCodec 二进制编解码 / 泛型 TypedHandler / ServiceBuilder 链式注册（隐性：服务端字节编解码样板冗长 + codec 无锁读写竞态(InvokeMsg 在锁外读 codec)；score=17）
- **[61] `transport_register_func`** — 包级泛型函数 RegisterFunc 便捷注册(业务函数直接挂方法名,内部 TypedHandler 默认JSON)（隐性：serveConn 调 handler 无 panic 恢复,单个 handler 崩溃会拖垮服务端 goroutine/进程(R2 隐性健壮性)；score=16）
- **[64] `transport_graceful_stop`** — GracefulStop 优雅关闭(等待在途 RPC 清空) + inFlight 计数 + Metrics.InFlight（隐性：Stop 立即关监听不等待在途,滚动发布会截断在途请求(R2 隐性)；score=14）
- **[74] `transport_metrics`** — transport 服务端 RPC 框架层可观测性(统一 metrics 注册表 + 按方法计数/错误/延迟直方图)（隐性：框架仅内置原子计数(bytesSent/rpcs/errs),无按方法拆分与延迟分布,且未纳入统一 metrics 注册表(Prometheus/JSON 不可见)；score=16）
- **[98] `transport_invoke_pool_poison`** — 取消风暴回归测试 + 错误帧连接复用断言（隐性：Invoke 取消协程关闭连接与成功路径 putConn 复用竞态,无 deadline 纯取消 ctx 下把已关闭连接放回池中毒化连接池；score=19）

## diagnostics

- **[51] `diag_selfcheck`** — SelfCheck配置链自检（隐性：整链损坏难定位；score=14）
- **[91] `diagnostics_shardcheck`** — ShardCheck 数据面不变量自检 + 接入 /debug/shards（隐性：诊断包此前只覆盖共识层(RaftCheck)与配置链(SelfCheck),数据面迁移健康(pendingIn/pendingOut 自相矛盾/卡滞)无任何量化信号;且 updateRaftHealthGauge 在 s.c==nil 时 panic(/metrics 早于集群就绪即崩)；score=18）

## metrics

- **[116] `bench_hotpaths`** — 热点路径基准测试(metrics/util/transport 共 9 个 Benchmark)（隐性：经 115 轮后性能维度从未被量化，后续任何优化都无基线可对比(且 GaugeVec.WithLabelValues 91ns/1alloc 的锁+Join 热路径此前不可见)；score=14）
- **[117] `metrics_countervec`** — metrics.CounterVec 带标签计数器原语 + Registry.CounterVec 访问器(WritePrometheus/Snapshot 导出)（隐性：此前按状态/方法维度计数只能手搓『http_responses_<code>』式独立指标名(见 gateway)，缺 method 维度且无法在单指标内切片算错误率(R2)；score=18）
- **[119] `metrics_labelvec_lockfree`** — 标签向量读路径免锁化（共享 labelVec 存储：读快照 atomic.Value，写才加锁重建）（隐性：GaugeVec/CounterVec.WithLabelValues 每次取独占锁+分配拼接 key，网关每请求两次调用成多核热点（锁竞争+分配）（R2 隐性性能悬崖）；score=17）

## version

- **[57] `version_activate`** — version 包激活 + /debug/version 回退构建期注入 + IsDev()/Short()（隐性：version 包全库零引用(死代码);未 SetVersion 时网关版本接口无值可回退；score=13）
- **[70] `version_autofill`** — version 包从 runtime/debug.ReadBuildInfo() 自动补全（隐性：裸 go build/go run 二进制 /version 恒报 unknown commit,排障无法定位构建来源；score=16）

## statusfmt

- **[56] `statusfmt_score`** — run(in,out,jsonMode) 可测核心 / -json 机器可读报告 / STALLED 退出码2供探活（隐性：clusterHealthScore/shardBalance 评分函数此前从未接入输出(准死代码)；score=15）

## docs

- **[59] `docs_sync`** — README 追加 R6/R7 交付段 / usage.md 补 statusfmt 独立用法（隐性：近22轮(#177-#198)新能力零文档记录,文档漂移；score=12）
- **[69] `docs_sync_209`** — 文档同步 R6/R7 段扩至 #177-#208（隐性：近 22 轮新能力零文档记录(漂移)；score=10）
- **[72] `docs_sync_212`** — 文档同步 #210/#211（隐性：新增构建自报/并发 gauge 未记录(漂移)；score=10）
- **[77] `obs_doc_sync`** — 可观测性收口文档(#213-#216)+ 修复 TestHasCommittedCurrentTerm 机器负载敏感 flaky（隐性：近 4 轮新增控制面/框架层指标零文档记录;选举守卫测试在高负载下误报失败；score=15）
- **[84] `docs_sync_223`** — README 补齐 #217-#223 交付段(共识/数据面健康快照+客户端/工具韧性收口)（隐性：近 7 轮(#217-#223)新能力零文档记录(文档漂移)；score=12）
- **[85] `docs_arch_endpoints`** — architecture 网关端点表与 /metrics 描述同步近期能力（隐性：端点表漏列 /debug/raft|/version|/routes|/migrate-plan;/metrics 描述仍停留在单表 JSON；score=12）
- **[86] `docs_usage_endpoints`** — usage 端点表补 /debug/raft + /metrics 描述同步（隐性：/metrics 描述停留在单表 JSON，缺 /debug/raft 端点与 Prometheus 协商说明；score=11）
- **[87] `docs_runbook_observ`** — runbook 补 kvraft 状态机可观测(Status/GC)排障段（隐性：§6.3 未提网关汇聚端点与 raft_min_health_score；kvraft 状态机可观测零文档；score=12）
- **[88] `docs_coverage_sync`** — coverage 补时效性说明 + 近期 cluster-free 单测清单（隐性：覆盖率数值快照早于 #212-#226 推送，未反映新增大量单测，易误导；score=11）
- **[92] `docs_gw_freeze_drift`** — 消除 README/设计文档§7(已根治) 与 architecture.md(未根治) 关于 3-group 迁移冻结的文档矛盾（隐性：architecture.md §4.2/§9 仍称『根因尚未根治/最高优先级待办』，与同仓 README 及 lab4-shardkv-design.md §7(标题即『冻结已根治』) 直接矛盾，误导运维/审计；目录树漏列 runbook.md/coverage.md；score=15）
- **[93] `docs_kvcli_api`** — 补全 kvcli 客户端完整 API 文档(usage.md §4 仅记载 get/put/append/bench/MGet/MSet/SetMaxConcurrent)（隐性：大量库方法(Del/MDel/Exists/Incr/SetNX/Cas/AppendGet/Pipeline/Ping/Healthy/Ready/EnableCache/EnableBreaker/EnableGzip/EnableSingleFlight/WarmUp/Metrics/Close 等) 在文档中完全缺失,用户/开发者无从知晓这些能力存在；score=16）
- **[94] `docs_migrate_gauges`** — 收口迁移可观测文档(#229 gauge / #230 diagnosis 此前零文档记载)（隐性：shardkv_pending_in/out/owned/total 四个 Prometheus 告警主指标(#229) 与 /debug/shards 的 diagnosis 自检字段(#230) 在 runbook/usage/architecture/coverage 全未记载,运维无法基于其做阈值告警；score=16）
- **[96] `docs_crosslink_integrity`** — 文档时效收口——coverage.md 仍称 kvraft_status_test.go『尚未提交』(实际 #228 已提交) 且快照框定停于 #212-#226（隐性：coverage.md 与迭代实际进度脱节(迭代已推进至 #234):既误称某单测文件未提交,又未反映 #227-#234 新增 cluster-free 单测与 docs/observability.md;全仓内部 markdown 链接此前从未系统性校验；score=15）
- **[120] `docs_ci_sync_120`** — 文档/CI 收口：可观测性文档同步新增标签指标 + CounterVec 原语 + 指标校验器识别 CounterVec/GaugeVec（隐性：新增 http_responses_total{code,method}/CounterVec 原语零文档记载；check_metrics_docs 不识别 CounterVec/GaugeVec，未来标签指标漂移无门禁拦截（R2 隐性）；score=14）
- **[146] `docs_cross_machine`** — README 跨机部署补充「真·跨机（每节点独立进程）」小节（kvnode 多进程 + gateway -connect 示例 + 指向 cross_machine_test.py）（隐性：原 README 跨机章节只有单进程 --tcp-config 形态，缺生产多机拓扑文档（R2 文档盲区）；score=13）
- **[147] `docs_kvnode_followup`** — 跨机部署收尾：DEPLOYMENT §2.6 补「真·每节点独立进程」多机启动细节（kvnode 多进程 + gateway -connect 纯客户端 + 指向 cross_machine_test.py）；kvnode 抽 startNode 并加 main_test.go 消覆盖硬缺口（kvnode 测试 0→1，覆盖表 ❌→✅）（隐性：I21-I25 交付后 DEPLOYMENT 仍只写单进程 --tcp-config 形态、kvnode 入口零测试（red），文档/测试盲区；score=12）

## scripts

- **[102] `docs_link_checker`** — Markdown 内部链接自动校验器 + CI 门禁(docs-links job,纯 Python 不依赖 Go)（隐性：文档漂移此前只能靠人工通读发现,无自动化门禁,no-go 环境下更易悄悄劣化；score=14）
- **[103] `docs_endpoint_checker`** — 网关端点/CLI 与文档一致性自动校验器 + CI 门禁（隐性：端点新增/改名后文档是否同步只能靠人工通读,无自动化断言(与 #241 同属 no-go 文档工程化缺口)；score=13）
- **[104] `docs_metrics_checker`** — 指标注册名与文档一致性自动校验器+CI门禁（隐性：指标新增/改名后文档是否同步只能靠人工,无自动化断言(no-go 下更易劣化)；score=15）
- **[105] `doc_api_gate`** — kvcli.Client/util 公共 API 与文档一致性自动校验器+CI门禁（隐性：5 个 util 公共类型(BufferPool/Closer/CbState/MultiLimiter/TokenBucket)零文档,外部复用者无从知晓其存在;且 Client 方法改名/删除后文档漂移无断言；score=17）
- **[106] `gen_changelog`** — 迭代交付记录自动生成 CHANGELOG.md + --verify 同步门禁（隐性：state.json 迭代日志存于隐藏工作目录,对用户/审计不可见;且交付历史易与实际脱节；score=16）
- **[107] `go_patterns_scan`** — Go 反模式免编译静态扫描器+CI门禁（隐性：ioutil/非测试 time.After 等已根治坏味道无自动守卫,后续改动易悄然回归;纯文本扫描可免 Go 工具链；score=15）
- **[108] `check_all_orchestrator`** — 统一自检编排器 check_all.py + README 脚本索引（隐性：6 个免 Go 校验器零散调用易漏跑,scripts 模块零文档;CHANGELOG 与 state 漂移无单入口发现；score=15）
- **[110] `state_integrity_gate`** — 自驱开发日志完整性校验器 + gen_changelog 去重韧性（隐性：state.json log 已被注入 cycle65/66 重复条目,静默击穿 gen_changelog --verify,CI docs-links 实际已失败却无人察觉(审计链污染)；score=17）
- **[112] `checker_inventory_meta`** — 校验器套件接线一致性 meta 门禁（隐性：新增 8 个免 Go 校验器后,任一漏接 check_all/ci.yml/README 即产生门禁盲区且无人察觉(harness 自身漂移风险)；score=15）
- **[113] `coverage_doc_meta`** — coverage.md 与校验器清单一致性 meta 门禁 + 刷新过时覆盖率文档（隐性：coverage.md 仍停在 #235 时代未列 8 校验器,误导审计以为工程化收口维度缺失(harness 已覆盖却文档不体现)；score=15）
- **[115] `godoc_gate`** — godoc 导出标识符文档覆盖率门禁（隐性：导出 API 无 doc 注释则 go doc 空白,外部用户无法发现能力(此前无门禁)；score=17）
- **[121] `check_test_coverage_121`** — 免 Go 测试纪律护栏: 包级测试缺口 + 导出符号未引用提示(check_test_coverage.py)（隐性：120+ 轮功能迭代缺测试纪律护栏,新源码/导出 API 若无测试则覆盖率静默下降无人察觉(R2);且 meta 校验器 GUARDED 硬编码漏管新校验器(harness 自身漂移)；score=16）
- **[122] `gen_test_coverage_122`** — coverage.md 自动生成「模块↔测试」映射表 + --verify 漂移门禁(gen_test_coverage.py)（隐性：coverage.md 的测试覆盖章节为人工快照,与真实代码脱节(此前已多次 drift);无自动护栏,新源码/测试增减后表格静默失准(R2 隐性)；score=15）
- **[123] `checker_selftest_123`** — 校验器自身回归测试(make selftest / CI): fixture 驱动 check_test_coverage 纯函数（隐性：免 Go 自检门禁自身零测试,任一改动令其在干净仓库误报/崩溃时 CI 静默失败(cycle110 同类问题在门禁自身的变种, R2 隐性)；score=15）
- **[124] `hook_install_guard_124`** — 提交门禁钩子安装状态护栏 + 真正安装 hook(此前本环境静默失效)（隐性：.git/hooks/pre-commit 从未安装,文档承诺的 make hooks 门禁在此工作副本根本不触发,漂移缺陷可静默落库;且 .gitignore 漏 __pycache__ 致 Python 缓存进未跟踪；score=17）
- **[125] `secret_scan_guard_125`** — 密钥/凭证泄露静态扫描门禁(此前零安全维度)（隐性：仓库无任何密钥泄露防护:误提交私钥/AWS AK-SK/token 会直接落库并被推送远端,造成不可逆凭证暴露(不报错但危害极大,R2 隐性安全缺口)；score=18）
- **[128] `secret_scanner_selftest_128`** — 密钥扫描门禁的回归自测(7 例 fixture: PEM/AKIA/AWS-SK/WARN明文/Slack/GitHub/干净代码)（隐性：check_secrets.py 作为安全门禁此前自身零测试,任一改动令其漏报凭证时 CI 静默通过(cycle110 同类问题在「安全门禁自身」的变种,R2 隐性);且其扫描 scripts/ 会命中自带 fixture 文件导致误报 HARD(需自排除)；score=16）
- **[131] `checker_selftest_godoc_md`** — godoc/md_links 门禁的回归自测（隐性：13 个免 Go 门禁仅 2 个有自测,godoc 与链接门禁自身退化无人察觉(cycle110 同类在门禁自身的变种);check_godoc 经 fixture 审计未见真实 bug；score=16）
- **[132] `checker_selftest_api_metrics_endpoints`** — api/metrics/endpoints 三文档一致性门禁的回归自测（隐性：文档一致性门禁此前无自测,任一改动令其在干净仓库误报/崩潰时 CI 静默失败(cycle110 同类在门禁自身的变种);审计发现 check_api_docs 的 CLIENT_RE 硬编码接收者名 c|s|kc|cl,改为 [a-z]+ 更鲁棒且仍排除 GRPCClient；score=17）
- **[133] `checker_selftest_sweep_final`** — 剩余 5 个门禁(go_patterns/state_integrity/doc_inventory/coverage_doc/hooks_installed)回归自测,完成全 12 门禁自测覆盖（隐性：自驱迭代沉淀 13 个免 Go 校验门禁,此前仅 2 个有自测,门禁自身退化(cycle110 同类)长期无人察觉;本弧(131-133)补齐全部自测；score=18）
- **[134] `coverage_gate`** — Go 测试覆盖率门槛门禁(check_go_coverage.py + min_total 70% 入 scripts/coverage.config.json)（隐性：CI coverage job 只打印 summary 从不强制下限,覆盖率静默回退无人察觉(R2 隐性可观测缺口:有数据无护栏)；score=17）
- **[135] `selftest_runner`** — 校验器自测自动发现运行器(run_selftests.py)（隐性：自测清单硬编码在 Makefile/ci-local/CI 三处,新增校验器自测可静默跳过(cycle110 同类门禁自身漂移,R2 隐性)；score=16）
- **[136] `leaked_artifacts`** — 构建/覆盖率临时产物泄漏护栏(check_leaked_artifacts)（隐性：go test -coverprofile 写出的 covtest_* 临时目录未被 .gitignore 覆盖(根目录 cfg.json 可误提交),工作树污染无人察觉(R2)；score=17）
- **[137] `check_all_json`** — check_all.py --json 机器可读门禁报告（隐性：统一门禁结果仅文本输出,CI/看板无法程序化消费聚合结果(R2 可观测缺口);且 --json 初版未捕获子进程 stdout 致 JSON 被污染；score=15）
- **[139] `go_patterns_fatal`** — check_go_patterns 新增 log.Fatal/os.Exit 库代码 WARN 模式（隐性：库代码(main/测试除外)中 log.Fatal/os.Exit 会直接中止进程,此前未被任何门禁覆盖(R2 隐性错误面)；score=14）
- **[140] `godoc_pkg_doc`** — godoc 包级文档注释(// Package X)覆盖的软提示（隐性：14 个导出包全部缺 // Package 包级文档,go doc 包概览为空却无人察觉;设为硬失败会打挂 CI 故仅软提示(R2 可观测盲区)；score=14）
- **[145] `cross_machine_e2e_test`** — OS 进程级跨机实测（10 进程：9 节点 + 1 网关，taskkill 模拟宕机验证少数派容错）；TestNodePerProcessCluster 集成回归（隐性：此前跨机仅单进程 loopback 验证，缺真·多进程边界与宕机容错实测（R2 部署化真验缺口）；score=19）

## other

- **[114] `contributing_doc`** — CONTRIBUTING.md 开发者本地验证全流程 + 自驱迭代约定收口（隐性：9 校验器 + pre-commit 钩子 + state.json/CHANGELOG 约定仅散落 Makefile/CI/README,无单一 onboarding 入口,新贡献者无法安全续跑迭代；score=14）
- **[126] `quickstart_doc_126`** — QUICKSTART.md 零基础上手指南(构建/全栈运行/客户端/免Go自检/测试/下一步)（隐性：新贡献者无任何单一入口从零把系统跑起来:运行方式散落 README 快速启动/demo.md/CONTRIBUTING,缺一份串联 prereq->build->run->client->dev-loop 的 5 分钟上手(R2 隐性 onboarding 缺口)；score=15）
- **[127] `ci_gate_target_127`** — make ci 本地等效 CI 门禁 + CI gate job 跑统一编排器check_all.py(防御纵深)（隐性：CI 仅逐条跑各脚本,统一编排器check_all.py本身的聚合/退出码/warn_only逻辑从未在CI被验证;且本地无单一入口秒级复跑整条门禁链(只有make docs跑check_all,但缺校验器自测)；score=14）
- **[129] `license_mit`** — MIT 开源许可证(治理合规)（隐性：公开发布仓库无 OSI 许可证,默认保留所有权利,阻塞复用/贡献；score=14）
- **[130] `security_policy`** — SECURITY.md 漏洞披露政策(安全治理)（隐性：已落地密钥扫描门禁(check_secrets)但外部研究者无漏洞上报渠道,安全门禁缺人工闭环；score=15）
- **[138] `bench_guard_wired`** — 接通休眠的性能回归护栏(check_bench_regression 此前从未被调用)（隐性：check_bench_regression.py 自 cycle116 存在却未被 make/CI 调用,且 bench-baseline.json 为空 {},护栏实际失效(R2 harness 漂移,cycle110/133 同类)；score=16）
- **[149] `exp_leader_fault`** — 可展示容错实验框架（独立 Go 模块，隔离父项目门禁）+ 场景 A leader 故障切换（隐性：项目价值长期停在「健康分 100」与工具链自检，缺「系统确实容错」的可演示证据（实训/面试最值钱部分）；score=16）

---

本文件为生成产物，重新运行 `python3 scripts/gen_changelog.py` 即可刷新。如需手工追加说明，请在生成后单独维护，或扩展生成脚本。
