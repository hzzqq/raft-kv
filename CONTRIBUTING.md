# 贡献与本地验证指南（CONTRIBUTING）

本仓库在自驱迭代中沉淀了一套「免 Go 工具链」的静态自检门禁、提交前钩子与
CI 流水线。本文把**开发者本地验证全流程**与**自驱迭代约定**收敛到一处，
避免知识散落在 `Makefile` / `README.md` / `.github/workflows/ci.yml` 各处。

## 1. 本机环境的两个前提

- **`go` 默认不在交互 shell 的 PATH**：仓库随附托管 Go 工具链
  （`C:/Users/Administrator/.workbuddy/binaries/go/go/bin/go.exe`，Windows）。
  需要时直接调用该绝对路径，或在 `Makefile` 顶部把 `GO` 指向它。
- **该托管 Go 环境无 `gcc`**，因此本机**无法编译/运行 `go test -race`**。
  竞态类回归（混沌、迁移状态机）依赖 CI 侧 `ubuntu + gcc` 的 `*-race` job；
  本机以「高频 churn + 多轮循环」测试（如 `TestSKVReMigration` / `TestChaosLongRun`）
  替代 race detector 暴露 liveness / 数据竞态。

## 2. 本地验证入口（按开销从小到大）

| 场景 | 命令 | 说明 |
|------|------|------|
| 文档 / 日志门禁（秒级，免 Go） | `make docs` | 等价于 CI 的 `docs-links` job，串跑全部 9 个免 Go 校验器 |
| 复现 CI 关键 job | `./scripts/ci-local.sh docs` | 与上面同源；`ci-local.sh` 还支持 `test` / `raft` / `chaos` / `build` |
| 提交前门禁 | `make hooks` 安装后，`git commit` 自动跑 | 见 §3 |
| 单测（需 Go） | `make test` / `make test-all` | shardkv 重点包 / 全量 |
| 覆盖率快照 | `make cover` | 生成 `cover.out`，数值见 `docs/coverage.md` |
| 全栈冒烟 | `make demo` | 拉起 cluster → HTTP 网关 → 客户端 |

> 改完文档 / 脚本 / `state.json` 后，**先 `make docs`** 再提交，可秒级发现漂移。

## 3. 提交前钩子（强烈建议安装）

```bash
make hooks      # 把 scripts/pre-commit.sh 装入 .git/hooks/pre-commit
```

安装后每次 `git commit` 会先跑 `scripts/check_all.py`：任一免 Go 校验器失败即
**阻断提交**。这能从根上阻止「文档漂移 / `CHANGELOG.md` 失配 / 迭代日志污染」
被静默落库（此类问题曾因 CI 门禁未本地化而长期潜伏）。

临时跳过（不推荐）：`git commit --no-verify`。

## 4. 免 Go 自检校验器清单（9 个 + 3 个 meta 守护）

统一由 [`scripts/check_all.py`](scripts/check_all.py) 编排，CI [`docs-links`](.github/workflows/ci.yml)
job 与 `make docs` 复跑。每个校验器的目的与门禁强度见
[`README.md` 脚本索引](README.md) 与 [`docs/coverage.md`](docs/coverage.md)。
其中 3 个 *meta* 校验器守护「校验器套件自身一致性」，防止新增校验器漏接门禁。

## 5. 自驱迭代约定（state.json / CHANGELOG）

本仓库由自主开发循环（self-driving dev）持续迭代，逐轮交付记录在
`.workbuddy/self-driving/state.json` 的 `log` 字段中（审计链）。

- **`CHANGELOG.md`** 由 [`scripts/gen_changelog.py`](scripts/gen_changelog.py) 从
  `state.json` **自动生成**：`make docs` 中的 `gen_changelog.py --verify` 会校验
  二者同步，失配即阻断。
- 迭代日志「重复注入 / 字段越界」由 [`scripts/check_state_integrity.py`](scripts/check_state_integrity.py)
  兜底；`docs/coverage.md` 与校验器清单的一致性由
  [`scripts/check_coverage_doc.py`](scripts/check_coverage_doc.py) 兜底。
- 如需手工追加说明，请在生成产物之外单独维护，或扩展生成脚本。

## 6. 提交流程纪律

1. 本地 `make docs` + （有 Go 时）`make test` 全过。
2. `make hooks` 已装则提交自动过门禁；未装也请手动跑 `make docs`。
3. 普通 `git commit`，禁止 `git push --force`、禁止 `rm -rf` 等不可逆命令。
4. 推送经用户授权的范围进行（详见 `README.md` 验证状态说明）。

## 7. 运维排障

线上指标语义、告警阈值与排障路径见 [`docs/runbook.md`](docs/runbook.md)；
架构与端点总览见 [`docs/architecture.md`](docs/architecture.md) 与
[`docs/usage.md`](docs/usage.md)；全部指标目录见 [`docs/observability.md`](docs/observability.md)。
