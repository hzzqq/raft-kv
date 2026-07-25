#!/usr/bin/env bash
# smoke.sh —— 快速、沙箱安全的冒烟门禁（与 run-tests.sh 互补）。
#
# 定位：run-tests.sh 跑的是「全量 / 带 churn 重用例」的完整套件（动辄数分钟），
# 适合 CI 或本地深度验证；本脚本聚焦「秒级反馈」——只跑编译 + 静态检查 + 各包中
# cluster-free（不拉起真实 Raft 集群）的单元/白盒用例，用于开发过程与自动化循环里
# 的频繁自检，避免每次改动都被迫跑完整个重型套件。
#
# 用法：
#   ./scripts/smoke.sh            # 跑全部冒烟项
#   ./scripts/smoke.sh -q        # 静默模式，仅输出 FAIL/汇总
#
# 环境：复用 run-tests.sh 的托管 Go 工具链（绝对 Windows 路径，Git Bash 可识别）。
set -euo pipefail
cd "$(dirname "$0")/.."

export GO="C:/Users/Administrator/.workbuddy/binaries/go/go/bin/go.exe"
export GOCACHE="C:/Users/Administrator/.cache/go-raftkv"
export GOPATH="C:/Users/Administrator/.cache/gopath-raftkv"
export GO111MODULE=on

QUIET=0
[[ "${1:-}" == "-q" ]] && QUIET=1
run() { # run <desc> <cmd...>
  local desc="$1"; shift
  if [[ $QUIET -eq 1 ]]; then
    if "$@" >/dev/null 2>&1; then echo "PASS  $desc"; else echo "FAIL  $desc"; return 1; fi
  else
    echo "== $desc =="; "$@"; fi
}

fail=0
run "go build ./..." "$GO" build ./... || fail=1
run "go vet ./..." "$GO" vet ./... || fail=1

# 纯工具 / 可观测包：全量快测（均 cluster-free，<5s）。
for p in ./src/metrics ./src/util ./src/version ./src/diagnostics ./src/transport; do
  run "go test $p" "$GO" test "$p" -count=1 || fail=1
done

# kvraft：仅 cluster-free 的状态机/GC/会话用例。
run "go test kvraft(cluster-free)" "$GO" test ./src/kvraft/ -count=1 \
  -run 'Status|KVStatus|GC|Config|ClientSession' || fail=1

# shardkv：仅 core_iter 白盒迁移状态机用例（不触发完整集群 churn）。
run "go test shardkv(core_iter)" "$GO" test ./src/shardkv/ -count=1 \
  -run 'TestInstallShardConfigNumIdempotent|TestDropStaleIncoming|TestNoPendingLeakAfterConfigAdvance|TestConfigChangeSnapshot|TestMetricsGauges|TestLargeShardMetric|TestMigrationBacklogGauges' || fail=1

# gateway：仅 cluster-free 的 debug/metrics/routes/version 端点用例（不含重型 Raft 集群用例）。
run "go test gateway(cluster-free)" "$GO" test ./src/gateway/ -count=1 \
  -run 'Debug|Metrics|Routes|Version|Migrate' || fail=1

if [[ $fail -eq 0 ]]; then
  echo "SMOKE OK"
else
  echo "SMOKE FAILED"
  exit 1
fi
