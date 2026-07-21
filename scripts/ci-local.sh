#!/usr/bin/env bash
# ci-local.sh —— 在本机复现 .github/workflows/ci.yml 的关键 job（除 -race：本机
# managed Go 无 gcc，竞态检测依赖 CI 侧 chaos-race job）。按 job 分段，失败即退出。
#
# 用法：
#   ./scripts/ci-local.sh            # 跑全部（vet + test + migration-stress + chaos + build + demo）
#   ./scripts/ci-local.sh test       # 仅 vet + 全量测试
#   ./scripts/ci-local.sh chaos      # 仅混沌用例（I16/I18）
#   ./scripts/ci-local.sh build      # 仅构建 + demo 全栈冒烟
set -euo pipefail

cd "$(dirname "$0")/.."

RUN="${1:-all}"

run_test() {
  echo "==> [vet]"
  go vet ./...
  echo "==> [test] go test ./... -count=1 -timeout 600s"
  go test ./... -count=1 -timeout 600s
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
  chaos) run_chaos ;;
  build) run_build ;;
  all)
    run_test
    run_chaos
    run_build
    ;;
  *) echo "unknown target: $RUN (want test|chaos|build|all)"; exit 2 ;;
esac
echo "OK"
