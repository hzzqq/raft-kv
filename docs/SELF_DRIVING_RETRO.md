# 自驱迭代复盘（cycle 1–140，2026-07-19 → 2026-07-26）

> 本文是 `raft-kv` 项目 **140 轮 AI 自主迭代**的方法论归档，不是项目使用文档。
> 它回答三个问题：我们交付了什么、哪些纪律有效、以及**为什么在第 140 轮主动收尾**。
> 目标读者：未来要跑 AI 自驱循环的你自己。

---

## 1. 一句话结论

我们用「AI 自主循环（self-driving-dev + drive-all）」在 7 天内对 MIT 6.824 Lab4 的 ShardKV 实现完成了 **140 轮迭代**、**311 个 commit**，交付了一个**核心扎实、传输层自研、测试硬核**的分布式 KV 骨架；但**后期迭代陷入了"给自驱循环自己造工具"的价值塌陷**，故在 cycle 140 主动收官，把方法论沉淀于此，后续改动转人工主导。

## 2. 成果盘点（真家伙 vs 元工具）

### 真家伙（产品代码，可复用）
- **共识核心**：`src/raft`（raft.go 1102 行）+ `src/shardkv`（shardkv.go 1810 行）+ `src/shardmaster` + `src/kvraft`，全功能实现，非玩具。
- **自研传输层**：`src/transport`（2074 行）——零依赖 gRPC 风格 TCP 帧，含 TLS、连接池、优雅关闭、invoke-cancel、空闲回收、指标。这是真功夫，不是包装。
- **真网关**：`gateway.ServeGRPCWith(lis net.Listener, …)` 起真实 TCP 监听，`Persister` 持久化抽象齐全，`demo` 可端到端跑 cluster→HTTP→client 全栈。
- **测试硬核**：**480 个测试函数 / 152 个测试文件**，含 `chaos_test.go`、`churn` 真集群集成测试（`cluster.StartCluster(2,3,3,0)` 启 2 group × 3 副本 × 3 shardmaster 的完整内存集群）。

### 元工具（服务自驱循环本身，消费者还是自驱循环）
- `scripts/` 下 **19 个 Python 门禁/护栏/自测工具**：`check_godoc` / `check_go_patterns` / `check_go_coverage` / `check_bench_regression` / `check_leaked_artifacts` / `run_selftests.py` 等。
- 近 40 条 commit 中 **33 条是元工具/文档，仅 7 条动产品代码**（cycle 134–140 全在做门禁/护栏/自测运行器）。

## 3. 工程纪律（哪些有效、哪些过度）

### 有效的纪律（建议保留）
| 纪律 | 效果 |
|---|---|
| `go build ./...` + `go vet` + 定向 `go test` 每轮必过才提交 | 140 轮零红条累积，没有"修一个坏一个" |
| 绝不 `--force` push、普通快进 | 历史始终可追、可回滚 |
| 核心改动靠**真集群集成测试**护体（非 mock 骗自己） | 正确性被真实多节点场景验证 |
| 局部中间件改动用 cluster-free（httptest + 精确 `-run`）快速验证 | 避开沙箱 raft 选举偶发挂死，反馈快 |
| 可观测性优先（metrics/header/gauge 随手加） | 排障有抓手 |

### 过度 / 踩坑（建议规避）
| 问题 | 反思 |
|---|---|
| **-race 本机从不跑**（Windows 无 gcc，靠 CI） | 对并发算法项目，本机"绿"≠真绿；核心竞态只能靠 CI 兜底 |
| **提交信息风格漂移**：`self-driving dev [R7 #210]` / `cycle140: …` / `chore: auto-sync …` 三风格混用，且 **author 全空** | 一个内部细节讲究到 HTTP header 的项目，却放任最基础的 commit hygiene——自相矛盾 |
| **文档体量失控**：README 355 行 + usage 260 行 + 6 顶层 md + docs/ 8 个 | 大量是"自驱轮次成就清单"，读起来像进度汇报而非使用指南；文档为 AI 循环服务，不为人服务 |
| **测试数 ≠ 核心保障**：大量 middleware/可观测头单测撑起 480，但 raft 选举/快照边界分支覆盖率仍低（coverage.md 自承） | 数量在验证外壳，核心正确性靠那批真集成测试 |

## 4. 门禁/元工具链清单（scripts/）

| 工具 | 作用 |
|---|---|
| `check_all.py` | 门禁总入口，`--json` 机器可读报告 |
| `run_selftests.py` | 自测自动发现运行器（消除硬编码清单漂移） |
| `check_godoc.py` | 导出包级文档（`// Package X`）覆盖软提示 |
| `check_go_patterns.py` | 库代码 `log.Fatal`/`os.Exit` 的 WARN 模式 |
| `check_go_coverage.py` | Go 测试覆盖率门槛门禁 |
| `check_bench_regression.py` | 性能回归护栏（曾长期未被调用，cycle138 才接通） |
| `check_leaked_artifacts.py` | 构建/覆盖率临时产物泄漏护栏 |
| `check_secrets.py` / `check_state_integrity.py` | 密钥/状态完整性扫描 |
| `check_md_links.py` / `check_doc_inventory.py` / `check_metrics_docs.py` / `check_api_docs.py` / `check_docs_endpoints.py` / `check_coverage_doc.py` | 文档与各代码面一致性 |

> 这些工具**唯一消费者是自驱循环本身**。它们能跑、有价值，但维护成本随循环增长——这正是价值塌陷的一部分。

## 5. 价值塌陷的教训（本文最重要的部分）

自驱循环的目标被设为「迭代 200 轮」。当 cycle 越过 ~100 后，项目进入**自指循环**：

1. 核心算法（raft/shardkv）出于"谨慎"几乎不再动；
2. 周边基础设施（gateway/metrics/observability）已被打磨到边际收益极低；
3. 循环为了"继续有东西做"，开始**给自驱过程本身造工具**（门禁、护栏、自测运行器、覆盖率门槛）；
4. 这些工具的产出又只被自驱循环消费 → 闭环，无外部价值增量。

**信号**：当你发现自己在写 `check_checker.py` 而不是改产品代码时，就该收尾了。
**量化信号**：近 40 commit 中元工具占比 >80% 即触发收尾决策。

## 6. 给未来 AI 自驱项目的 5 条

1. **设「价值闸门」而非「轮数目标」**：用"近 N 轮产品代码占比 < X%"作为停止条件，而不是"跑满 200 轮"。
2. **核心与外壳分开验证**：核心改动必须真集群集成测试；中间件才用 cluster-free 快速回路。
3. **本地也要能跑 `-race`**：选有 cgo 的工具链，否则本机绿是假绿。
4. **commit hygiene 从第一天统一**：单一 message 模板 + 署名，别等 140 轮再后悔。
5. **文档写给"人"**：架构/使用/设计三件套够用，别把迭代日志当文档。

## 7. 收官状态与后续路线

- **已收尾**：`drive-all` / `unstick` 自动化已删除；`state.json` 标记 `paused=true, finalized=true`（cycle 140）。
- **后续路线（人工主导，不走自驱）**：
  - **核心深啃**：raft 选举/快照边界确定性单测补全；shardkv 配置变更幂等 / 迁移重试健壮性（基于 `installedCfgNum`/`proposedConfigNum` 基建）。
  - **真部署化**：把 `Persister` 从内存落盘（真实文件 WAL + fsync），让节点重启可恢复；补多机运维 / 客户端 SDK。
  - **定位收敛**：明确"学习标本 / 展示品 / 真中间件"三选一，避免每个方向都做一点但不彻底。

---

*本文件由 AI 在 cycle 140 收官时生成，作为自驱方法论资产沉淀。它不随自驱循环更新。*
