#!/usr/bin/env bash
# smoke_runtime.sh —— raft-kv 真部署路径「运行时」冒烟验证（网络无关，用 localhost）。
#
# 为什么需要它：I4 写了 docker-compose / Prometheus / Grafana，但此前只在 deploycheck
# 里做静态不变量校验（指标名存在、拓扑一致），从没在真实多进程形态下跑过。本脚本用
# 本地 go 编译 kvnode/gateway/kvcli，以 localhost 不同端口 + 内存模式起
# 3 ShardMaster + 3 ShardKV + gateway -connect，验证：
#   1) 6 节点全部 /healthz 就绪、gateway /readyz 就绪
#   2) gateway 经 -connect 真能 Put/Get（部署路径运行期可用）
#   3) 每个 kv 节点 /metrics 真暴露 raftkv_raft_is_leader / term / has_leader_lease /
#      leader_elections_total（证明 I152 共识面指标在真部署下可被 scrape）
#   4) kill 当前 leader 后，新 leader 选出、某副本 leader_elections_total 增加
#      （即 Grafana「leader 切换次数」曲线真的会动），且 gateway Put/Get 切换后仍通
#
# 注意：构建产物落在仓库内 .smoke/（已被 .gitignore 忽略）。go.exe 是 Windows 二进制，
# 必须用仓库相对/绝对路径（git-bash 与 go.exe 视图一致），不能用 /tmp（会被解析成 C:\tmp）。
#
# 用法：bash scripts/smoke_runtime.sh
set -u
GO=/e/go-sdk/go/bin/go.exe
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
SMOKE=.smoke
BIN="$SMOKE/bin"
mkdir -p "$BIN"
LOCALCFG="$SMOKE/cluster.local.json"
PIDS=()

fail() { echo "FAIL: $1"; cleanup; exit 1; }
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; wait 2>/dev/null; }

# ---- 本地拓扑：localhost 不同端口 + 内存模式（data_dir 空）----
cat > "$LOCALCFG" <<'EOF'
{
  "n_groups": 1,
  "n_replicas": 3,
  "n_sm": 3,
  "max_raft_state": 1000,
  "data_dir": "",
  "nodes": [
    {"name":"m0","addr":"127.0.0.1:17000"},
    {"name":"m1","addr":"127.0.0.1:17001"},
    {"name":"m2","addr":"127.0.0.1:17002"},
    {"name":"g0-0","addr":"127.0.0.1:17100"},
    {"name":"g0-1","addr":"127.0.0.1:17101"},
    {"name":"g0-2","addr":"127.0.0.1:17102"}
  ]
}
EOF

echo "==> 编译 kvnode/gateway/kvcli (CGO_ENABLED=0)"
CGO_ENABLED=0 "$GO" build -o "$BIN/kvnode.exe" ./src/kvnode || fail "build kvnode"
CGO_ENABLED=0 "$GO" build -o "$BIN/gateway.exe" ./src/gateway || fail "build gateway"
CGO_ENABLED=0 "$GO" build -o "$BIN/kvcli.exe" ./src/kvcli || fail "build kvcli"

# diag 端口：m0..2=19100..19102, g0-0..2=19103..19105
start_node() {
  local name=$1 diag=$2
  "$BIN/kvnode.exe" -config "$LOCALCFG" -name "$name" -http ":$diag" >"$SMOKE/$name.log" 2>&1 &
  PIDS+=($!)
}
start_node m0 19100
start_node m1 19101
start_node m2 19102
start_node g0-0 19103
start_node g0-1 19104
start_node g0-2 19105

wait_health() {
  local port=$1
  for _ in $(seq 1 60); do
    curl -s -o /dev/null "http://127.0.0.1:$port/healthz" && return 0
    sleep 0.2
  done
  return 1
}
for p in 19100 19101 19102 19103 19104 19105; do
  wait_health "$p" || { echo "---- $p 日志 ----"; tail -15 "$SMOKE/node_$p.log" 2>/dev/null; tail -15 "$SMOKE"/*.log 2>/dev/null | head -30; fail "节点 diag :$p 未就绪"; }
done
echo "    [ok] 6 节点 /healthz 就绪"

# gateway -connect
"$BIN/gateway.exe" -connect "$LOCALCFG" -addr ":18080" >"$SMOKE/gateway.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 80); do
  curl -s -o /dev/null "http://127.0.0.1:18080/readyz" && break
  sleep 0.2
done
curl -s -o /dev/null "http://127.0.0.1:18080/readyz" || fail "gateway /readyz 未就绪"
echo "    [ok] gateway -connect /readyz 就绪"

# Put/Get 经 gateway
"$BIN/kvcli.exe" -addr http://localhost:18080 put smoke hello >/dev/null 2>&1 || fail "kvcli put 失败"
got=$("$BIN/kvcli.exe" -addr http://localhost:18080 get smoke 2>/dev/null)
[ "$got" = "hello" ] || fail "kvcli get 返回 '$got'，期望 hello"
echo "    [ok] gateway Put/Get 通 (smoke=hello)"

# scrape 每个 kv 节点 /metrics，确认共识面指标真存在
check_metric() {
  local port=$1 want=$2
  curl -s "http://127.0.0.1:$port/metrics" | grep -q "$want" || fail "kv :$port /metrics 缺 $want"
}
for p in 19103 19104 19105; do
  check_metric "$p" "raftkv_raft_is_leader"
  check_metric "$p" "raftkv_raft_term"
  check_metric "$p" "raftkv_raft_has_leader_lease"
  check_metric "$p" "raftkv_raft_leader_elections_total"
done
echo "    [ok] 3 个 kv 节点 /metrics 暴露 is_leader/term/has_leader_lease/leader_elections_total"

# ---- R8: kill 当前 leader，验证可观测 + 服务连续 ----
leader_port=""
for p in 19103 19104 19105; do
  if curl -s "http://127.0.0.1:$p/metrics" | grep -Eq 'raftkv_raft_is_leader\{[^}]*\} 1'; then
    leader_port=$p; break
  fi
done
[ -n "$leader_port" ] || fail "未找到当前 leader（无 is_leader=1）"
echo "    当前 leader 在 diag :$leader_port"

# 记录各 kv 节点切换前的 leader_elections_total（leader 计数只数本进程当选次数）。
declare -A before_el
for p in 19103 19104 19105; do
  before_el[$p]=$(curl -s "http://127.0.0.1:$p/metrics" | grep '^raftkv_raft_leader_elections_total' | awk '{print $2+0}')
done

# PIDS 顺序：m0 m1 m2 g0-0 g0-1 g0-2 gateway → leader diag 1910x 对应 g0-(x-19103)
idx=$(( 3 + (leader_port - 19103) ))
kill "${PIDS[$idx]}" 2>/dev/null
echo "    已 kill leader 进程 (pid ${PIDS[$idx]})"

for _ in $(seq 1 80); do
  curl -s -o /dev/null "http://127.0.0.1:18080/readyz" && break
  sleep 0.2
done
"$BIN/kvcli.exe" -addr http://localhost:18080 put smoke world >/dev/null 2>&1 || fail "切换后 kvcli put 失败"
got2=$("$BIN/kvcli.exe" -addr http://localhost:18080 get smoke 2>/dev/null)
[ "$got2" = "world" ] || fail "切换后 kvcli get 返回 '$got2'，期望 world"
echo "    [ok] leader 切换后 gateway 仍通 (smoke=world)"

# 找到新 leader，确认它自身的 leader_elections_total 比切换前增加（证明有真实选举发生）。
new_leader=""
for _ in $(seq 1 40); do
  for p in 19103 19104 19105; do
    if curl -s "http://127.0.0.1:$p/metrics" | grep -Eq 'raftkv_raft_is_leader\{[^}]*\} 1'; then
      new_leader=$p; break 2
    fi
  done
  sleep 0.2
done
[ -n "$new_leader" ] || fail "re-election 后未选出新 leader"
after_val=$(curl -s "http://127.0.0.1:$new_leader/metrics" | grep '^raftkv_raft_leader_elections_total' | awk '{print $2+0}')
before_val=${before_el[$new_leader]:-0}
[ "$after_val" -gt "$before_val" ] 2>/dev/null \
  || fail "新 leader $new_leader 的 leader_elections_total 未增加 ($before_val -> $after_val)，切换不可观测"
echo "    [ok] 切换可观测：新 leader :$new_leader leader_elections_total $before_val -> $after_val"

cleanup
echo "PASS: 真部署路径运行时验证全部通过（R1 + R8）"
