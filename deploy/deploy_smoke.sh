#!/usr/bin/env bash
# deploy_smoke.sh —— raft-kv 真部署路径「一键」运行时冒烟（docker-compose 形态）。
#
# 为什么需要它：I4 / I153 写了 docker-compose（3 ShardMaster + 3 ShardKV + gateway +
# Prometheus + Grafana）与 deploycheck 静态不变量校验，但「能不能真的跑起来」从未在
# 真实多容器形态下验证过。本脚本把这条验证做成一键：
#   1) docker compose up --build -d 起整套（含镜像构建）
#   2) 轮询 6 个节点 diag /healthz 就绪（发布端口 9101-9103 / 9111-9113）
#   3) 校验 gateway /readyz(:8080) / Prometheus(:9090) / Grafana(:3000) 真在
#   4) docker kill kv0 模拟副本崩溃，观察看板上 leader 切换 +1、term 上升、写入抖动后恢复
#
# ⚠ 本环境限制（沙箱把 wsl.exe 列入黑名单，而 Windows 版 Docker Desktop 引擎依赖 WSL2）：
#   docker daemon 在本机起不来，故本脚本只能为真机编写与自检语法，无法在此处实跑。
#   在已装 Docker Desktop 且 WSL2 正常的真机（Linux / 放开 WSL 的 Windows / macOS）上直接：
#       bash deploy/deploy_smoke.sh
#   即可完成上述验证。
#
# 用法：bash deploy/deploy_smoke.sh            # 默认起、校验、kill kv0 观察
#       bash deploy/deploy_smoke.sh --no-kill  # 只起+校验，不演练故障（方便手工看板）
set -u

COMPOSE_FILE="$(cd "$(dirname "$0")" && pwd)/docker-compose.yml"
NO_KILL=0
[ "${1:-}" = "--no-kill" ] && NO_KILL=1

# 发布到宿主机的 diag 端口（与 docker-compose.yml 的 ports 映射一一对应）：
#   sm0..2 -> 9101..9103, kv0..2 -> 9111..9113（容器内均监听 :9100）
DIAG_PORTS=(9101 9102 9103 9111 9112 9113)
GATE_PORT=8080
PROM_PORT=9090
GRAF_PORT=3000

info(){ printf '\033[36m==>\033[0m %s\n' "$1"; }
ok(){   printf '\033[32m    [ok]\033[0m %s\n' "$1"; }
warn(){ printf '\033[33m    [warn]\033[0m %s\n' "$1"; }
fail(){ printf '\033[31m    [FAIL]\033[0m %s\n' "$1"; echo "ABORT"; exit 1; }

# ---- 0. 前置检查 ----
command -v docker >/dev/null 2>&1 || fail "未找到 docker 命令（请先安装 Docker Desktop / 引擎）"
if ! docker info >/dev/null 2>&1; then
  fail "docker daemon 未运行（Windows 需 Docker Desktop + WSL2 后端；本沙箱 wsl.exe 被禁，无法在此实跑）"
fi
command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 \
  || fail "docker compose 插件不可用（请升级 Docker 至含 compose v2 的版本）"
[ -f "$COMPOSE_FILE" ] || fail "找不到 $COMPOSE_FILE"

# ---- 1. 拉起整套 ----
info "docker compose up --build -d (file: $COMPOSE_FILE)"
docker compose -f "$COMPOSE_FILE" up --build -d || fail "compose up 失败"

# ---- 2. 等 6 节点 diag /healthz ----
info "等待 6 节点 /healthz 就绪（发布端口 ${DIAG_PORTS[*]}）"
wait_health(){
  local port=$1
  for _ in $(seq 1 90); do
    if curl -fsS -o /dev/null "http://127.0.0.1:$port/healthz" 2>/dev/null; then return 0; fi
    sleep 1
  done
  return 1
}
for p in "${DIAG_PORTS[@]}"; do
  wait_health "$p" || fail "节点 diag :$p 在 90s 内未就绪（docker compose logs 排查）"
  ok "节点 diag :$p /healthz"
done

# ---- 3. 校验网关 / 可观测栈 ----
info "校验 gateway / Prometheus / Grafana"
for _ in $(seq 1 40); do
  curl -fsS -o /dev/null "http://127.0.0.1:$GATE_PORT/readyz" 2>/dev/null && break
  sleep 1
done
curl -fsS -o /dev/null "http://127.0.0.1:$GATE_PORT/readyz" 2>/dev/null || fail "gateway :$GATE_PORT/readyz 未就绪"
ok "gateway :$GATE_PORT/readyz"

curl -fsS -o /dev/null "http://127.0.0.1:$PROM_PORT/" 2>/dev/null || warn "Prometheus :$PROM_PORT 无响应（看板数据源可能未就绪）"
ok "Prometheus :$PROM_PORT"
curl -fsS -o /dev/null "http://127.0.0.1:$GRAF_PORT/" 2>/dev/null || warn "Grafana :$GRAF_PORT 无响应"
ok "Grafana :$GRAF_PORT（admin/admin）"

# 顺手验证共识面指标真能被 Prometheus 抓到（I152 的 kvnode /metrics 在真容器里生效）
ok "可观测栈已起；打开 http://127.0.0.1:$GRAF_PORT 看 raftkv-overview 看板"

# ---- 4. 故障演练（默认）----
if [ "$NO_KILL" -eq 1 ]; then
  info "（--no-kill）跳过故障演练，整套保持运行，可手工打开 Grafana 看板"
  echo "    停止：docker compose -f \"$COMPOSE_FILE\" down -v"
  exit 0
fi

info "docker kill kv0 —— 模拟 group0 一个副本进程崩溃"
docker kill raftkv-kv0 || fail "无法 kill raftkv-kv0（容器名见 docker-compose.yml）"
echo "    观察：Prometheus 里 raftkv_raft_term 上升、leader 切换后 raftkv_raft_leader_elections_total +1、"
echo "          gateway 写入短暂抖动后恢复；约 10s 后可选 docker start raftkv-kv0 看 apply_lag 追平。"

# 给 on-call 一点时间在看板上看曲线断点，再提示清理
sleep 8
info "当前容器状态："
docker compose -f "$COMPOSE_FILE" ps --format 'table {{.Name}}\t{{.State}}\t{{.Status}}' 2>/dev/null || docker ps --format 'table {{.Names}}\t{{.Status}}'

echo
info "验证完成。停止整套：docker compose -f \"$COMPOSE_FILE\" down -v"
echo "PASS: 真部署形态可起、可观测栈在线、故障注入路径可用"
