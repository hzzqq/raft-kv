# 快速上手（Quickstart）

5 分钟从零把 `raft-kv`（Lab4 ShardKV）跑起来：启动一个多副本 Raft 集群 + HTTP 网关，
并用客户端读写分片 KV。

## 0.  prerequisites

- **Go 1.22+**：`go version` 确认。无 Go 时仍可跑 `scripts/` 下的免 Go 工程化自检
  （见 §4），但无法编译/测试本项目。
- **Git Bash / Bash**（Windows 推荐 Git Bash，`make` 与 `start.sh` 均依赖它）。
- 本机若用托管 Go 且无 gcc，`go test -race` 会失败——竞态检测交给 CI（见 `.github/workflows/ci.yml`）。

## 1.  构建

```bash
make build-binaries      # 产出 bin/gateway、bin/kvcli、bin/demo
```

或直接用 `go build ./...` 做编译校验。

## 2.  一条命令跑起全栈（推荐）

```bash
make demo                # 进程内拉起 2 组副本集群 + 常驻 HTTP 网关(:8080)
```

`demo` 会在进程内启动多个 Raft 组（分片 KV 的核心），并在 `:8080` 起一个 HTTP 网关，
网关背后是真实的多组共识集群，便于直接验证分片迁移、线性一致读等行为。

## 3.  客户端读写

网关起好后（另一终端）：

```bash
# 通过 Make 封装
make cli args="put hello world"
make cli args="get hello"
make cli args="append hello !"

# 或直接调 kvcli
./bin/kvcli put hello world
./bin/kvcli get hello

# 或直接用 curl（HTTP 网关端点见 docs/usage.md 与 docs/observability.md）
curl -XPOST 'http://localhost:8080/kv/put?key=hello' -d 'world'
curl 'http://localhost:8080/kv/get?key=hello'
```

更多 `kvcli` 子命令与库 API（批量 `MGet/MSet`、原子 `Cas/Incr/SetNX`、韧性
`EnableBreaker/EnableCache/EnableGzip` 等）见 [`docs/usage.md`](docs/usage.md)。

### 仅起网关（不跑 demo）

```bash
make serve          # 前台常驻 :8080
make serve-bg       # 后台常驻（写 raft-kv-gateway.pid / .log）
make stop           # 停止后台网关
```

## 4.  开发自检（免 Go）

本仓沉淀了一套**不依赖 Go 工具链**的纯 Python 静态校验器，统一由 `scripts/check_all.py`
编排，并由提交前钩子（`make hooks` 安装）在每次 `git commit` 时阻断文档漂移 /
CHANGELOG 失配 / 密钥泄露：

```bash
make docs                       # 等价 CI docs-links job，跑全部免 Go 自检
python3 scripts/check_all.py   # 直接跑，给出 PASS/FAIL 汇总
make hooks                     # 安装提交前门禁钩子（推荐）
make selftest                  # 校验器自身的回归测试
```

密钥泄露、端点/指标/API 与文档一致性等门禁清单见 [`README.md` 工程化自检脚本](README.md)
与 [`docs/coverage.md`](docs/coverage.md)。

## 5.  跑测试

```bash
make test            # 仅 shardkv 重点包（轻量）
make test-all        # 全量（含 gateway/metrics/kvcli，较重）
make smoke           # 秒级冒烟：编译 + vet + 各包 cluster-free 单测
./scripts/ci-local.sh docs   # 本机复现 CI 文档门禁
```

> 本地托管 Go 无 gcc，`make test-race` 不可用；`-race` 竞态用例在 CI 的 `*-race` job 跑。

## 6.  下一步

- 架构与分片迁移设计：[`docs/architecture.md`](docs/architecture.md)、[`docs/lab4-shardkv-design.md`](docs/lab4-shardkv-design.md)
- 运维/告警/卡滞排查：[`docs/runbook.md`](docs/runbook.md)
- 可观测性（指标 / `/debug/*` 端点）：[`docs/observability.md`](docs/observability.md)
- 贡献约定与本地验证全流程：[`CONTRIBUTING.md`](CONTRIBUTING.md)
