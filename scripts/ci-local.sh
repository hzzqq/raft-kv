#!/usr/bin/env bash
# ci-local.sh —— 在本机复现 .github/workflows/ci.yml 的关键 job（除 -race：本机
# managed Go 无 gcc，竞态检测依赖 CI 侧 chaos-race job）。按 job 分段，失败即退出。
#
# 用法：
#   ./scripts/ci-local.sh            # 跑全部（vet + test + raft + migration-stress + chaos + build + demo）
#   ./scripts/ci-local.sh test       # 仅 vet + 全量测试
#   ./scripts/ci-local.sh raft       # 仅 raft 用例（含 commitIndex 持久化回归测试，对应 CI raft-race job 的 -race 本地等价）
#   ./scripts/ci-local.sh chaos      # 仅混沌用例（I16/I18）
#   ./scripts/ci-local.sh build      # 仅构建 + demo 全栈冒烟
#   ./scripts/ci-local.sh docs       # 仅校验仓库内 Markdown 文档内部链接一致性
set -euo pipefail

cd "$(dirname "$0")/.."

RUN="${1:-all}"

run_docs() {
  echo "==> [docs] 校验 Markdown 内部链接一致性（不依赖 Go 工具链）"
  python3 scripts/check_md_links.py .
  echo "==> [docs] 校验网关端点/CLI 与文档一致性"
  python3 scripts/check_docs_endpoints.py
  echo "==> [docs] 校验指标注册名与文档一致性"
  python3 scripts/check_metrics_docs.py
  echo "==> [docs] 校验 kvcli.Client / util 公共 API 与文档一致性"
  python3 scripts/check_api_docs.py
  echo "==> [docs] 校验 CHANGELOG.md 与 state.json 迭代日志同步"
  python3 scripts/gen_changelog.py --verify
  echo "==> [docs] 校验自驱开发日志完整性(免编译): 重复/越界/缺失字段"
  python3 scripts/check_state_integrity.py
  echo "==> [static] 校验 Go 反模式(免编译): ioutil/非测试 time.After 硬阻断, fmt.Print/panic/TODO 提示"
  python3 scripts/check_go_patterns.py
  echo "==> [coverage] 测试覆盖率缺口探测(免编译, 软提示): 包无测试/导出符号未引用"
  python3 scripts/check_test_coverage.py
  echo "==> [hooks] 提交门禁钩子安装状态(免编译, 软提示): 缺失不阻断, 提示 make hooks"
  python3 scripts/check_hooks_installed.py || true
  echo "==> [secrets] 密钥/凭证泄露扫描(免编译, 硬阻断): PEM私钥/AWS AKIA/SK"
  python3 scripts/check_secrets.py
  echo "==> [coverage] 校验 docs/coverage.md 模块↔测试表与实际一致(免编译)"
  python3 scripts/gen_test_coverage.py --verify
}

run_test() {
  echo "==> [vet]"
  go vet ./...
  echo "==> [test] go test ./... -count=1 -timeout 600s"
  go test ./... -count=1 -timeout 600s
}

run_raft() {
  echo "==> [raft] raft 用例（含 commitIndex 持久化回归，对应 CI raft-race job 的非 -race 等价）"
  go test ./src/raft/ -count=1 -timeout 300s -v
}

run_chaos() {
  echo "==> [chaos] shardkv 混沌用例（I16/I18）"
  go test ./src/shardkv/ \
    -run 'TestChaosLeaderKillDuringMigration|TestChaosLongRun|TestSKVReMigration|TestSKVThreeGroupChurn|TestSKVConfigProgress|TestSKVReadIndex|TestSKVLinearizableAppend' \
    -count=3 -timeout 1200s -v
}

run_build() {
  echo "==> [build] gateway / kvcli / demo"
  mkdir -p bin
  go build -o bin/gateway ./src/gateway
  go build -o bin/kvcli   ./src/kvcli
  go build -o bin/demo    ./src/demo
  echo "==> [demo] 全栈冒烟"
  go run ./src/demo
}

case "$RUN" in
  test)  run_test ;;
  raft)  run_raft ;;
  chaos) run_chaos ;;
  build) run_build ;;
  docs)  run_docs ;;
  all)
    run_test
    run_raft
    run_chaos
    run_build
    run_docs
    ;;
  *) echo "unknown target: $RUN (want test|raft|chaos|build|docs|all)"; exit 2 ;;
esac
echo "OK"
