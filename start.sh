#!/usr/bin/env bash
# raft-kv 启动脚本（Git Bash / WSL / Linux）
#
# 把整套 ShardKV 系统「真正拉起来」：进程内启动 2 组副本集群，并起一个常驻
# HTTP 网关（默认 :8080），可被 kvcli / curl 持续访问。旧版只跑一次性 demo 后
# 退出，这个版本默认前台常驻，另提供后台启动 + 停止。
#
# 用法：
#   ./start.sh              # 默认 = serve：构建网关并前台常驻（Ctrl+C 停止）
#   ./start.sh serve        # 同上
#   ./start.sh bg           # 后台启动（写 raft-kv-gateway.pid + .log）
#   ./start.sh stop         # 停止后台网关
#   ./start.sh build        # 构建全部二进制到 bin/
#   ./start.sh demo         # 跑一次性端到端演示（跑完即退）
#   ./start.sh test         # 跑分片 KV 测试
#   ./start.sh cli <args>   # 运行 kvcli，例如 ./start.sh cli get hello
set -euo pipefail
cd "$(dirname "$0")"

# 托管 Go 工具链：Windows 下 go 不在 PATH；Linux/macOS 用系统 go。
if command -v go >/dev/null 2>&1; then
  GO=go
else
  GO="/c/Users/Administrator/.workbuddy/binaries/go/go/bin/go.exe"
fi
export PATH="$PATH:/c/Users/Administrator/.workbuddy/binaries/go/go/bin"
export GOCACHE="${GOCACHE:-C:/Users/Administrator/.cache/go-raftkv}"
export GOPATH="${GOPATH:-C:/Users/Administrator/.cache/gopath-raftkv}"
export GO111MODULE=on

# 是否 Windows（决定二进制后缀）
case "$(uname -s 2>/dev/null)" in
  *MINGW* | *CYGWIN* | *MSYS*) ISWIN=1 ;;
  *) ISWIN=0 ;;
esac
if [ "$ISWIN" = "1" ]; then GW="bin/gateway.exe"; else GW="bin/gateway"; fi

ADDR=":8080"
PIDFILE="raft-kv-gateway.pid"
LOGFILE="raft-kv-gateway.log"

cmd="${1:-serve}"
shift || true

case "$cmd" in
  serve)
    "$GO" build -o "$GW" ./src/gateway
    echo ">> raft-kv 网关启动于 $ADDR（Ctrl+C 停止；另开终端用 ./start.sh cli get hello 访问）"
    exec "$GW" "$ADDR"
    ;;
  bg)
    "$GO" build -o "$GW" ./src/gateway
    "$GW" "$ADDR" >"$LOGFILE" 2>&1 &
    echo $! >"$PIDFILE"
    echo ">> 后台启动：PID=$(cat "$PIDFILE")，日志=$LOGFILE"
    echo ">> 访问：curl http://localhost${ADDR}/healthz"
    echo ">> 停止：./start.sh stop"
    ;;
  stop)
    if [ -f "$PIDFILE" ]; then
      kill "$(cat "$PIDFILE")" 2>/dev/null && echo ">> 已停止（PID=$(cat "$PIDFILE")）" || echo ">> 进程不存在"
      rm -f "$PIDFILE"
    else
      echo ">> 未找到 $PIDFILE，尝试按窗口标题停止"
      taskkill //FI "WINDOWTITLE eq raft-kv-gateway" //F >/dev/null 2>&1 || true
    fi
    ;;
  build)
    "$GO" build ./...
    mkdir -p bin
    "$GO" build -o bin/gateway ./src/gateway
    "$GO" build -o bin/kvcli   ./src/kvcli
    "$GO" build -o bin/demo    ./src/demo
    echo ">> 构建完成 -> bin/"
    ;;
  demo)
    "$GO" run ./src/demo
    ;;
  test)
    "$GO" test ./src/shardkv/... -count=1 -timeout 300s "$@"
    ;;
  cli)
    "$GO" run ./src/kvcli "$@"
    ;;
  *)
    echo "用法: ./start.sh [serve|bg|stop|build|demo|test|cli]"
    exit 1
    ;;
esac
