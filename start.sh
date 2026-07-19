#!/usr/bin/env bash
# Lab4 ShardKV 开发启动脚本：构建 + 静态检查 + 运行 Lab4 分片 KV 测试
# 用法：  ./start.sh          （在 Git Bash / WSL / Linux 下）
set -euo pipefail
cd "$(dirname "$0")"
export PATH="$PATH:/c/Users/Administrator/.workbuddy/binaries/go/go/bin"

echo "== go build =="
go build ./...

echo "== go vet =="
go vet ./...

echo "== go test: Lab4 ShardKV =="
go test ./src/shardkv/... -count=1 -timeout 300s
