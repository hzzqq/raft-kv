#!/usr/bin/env bash
# run-tests.sh —— 在「托管 Go 工具链」下跑全套 / 指定测试。
#
# 背景：本机 Go 不在默认 PATH 上，且 GOPATH/GOCACHE 必须是 Windows 绝对路径
# （相对路径会触发 "GOPATH entry is relative" 报错）。本脚本把这些写死，保证在
# Git Bash / WSL / Linux 下零配置可跑。
#
# 用法：
#   ./run-tests.sh                 # 跑全部包
#   ./run-tests.sh shardkv        # 只跑 src/shardkv
#   ./run-tests.sh shardkv -run TestSKVConcurrent   # 带额外参数
#
# 注意：本地环境没有 gcc，无法跑 `go test -race`；需要 -race 的用例交给 GitHub
# Actions（ci.yml 里已配置，runner 自带 gcc）。本脚本默认不带 -race。
set -euo pipefail
cd "$(dirname "$0")"

# 托管 Go 工具链（绝对 Windows 路径，Git Bash 可识别）
export GO="C:/Users/Administrator/.workbuddy/binaries/go/go/bin/go.exe"
export GOCACHE="C:/Users/Administrator/.cache/go-raftkv"
export GOPATH="C:/Users/Administrator/.cache/gopath-raftkv"
export GO111MODULE=on

# 第一个参数若为包名（不含 -），约定为要测的包；否则默认全部。
PKG="./..."
if [[ $# -gt 0 && "$1" != -* ]]; then
  PKG="./src/$1/..."
  shift
fi

echo "== go build =="
"$GO" build ./...

echo "== go vet =="
"$GO" vet ./src/shardkv/ ./src/shardmaster/ ./src/kvraft/ ./src/raft/

echo "== go test: $PKG =="
"$GO" test "$PKG" -count=1 -timeout 400s "$@"

echo "== DONE =="
