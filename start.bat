@echo off
REM Lab4 ShardKV 开发启动脚本（Windows）：构建 + 静态检查 + 运行 Lab4 分片 KV 测试
REM 用法：双击或在 cmd / PowerShell 中运行 start.bat
cd /d %~dp0
set PATH=%PATH%;C:\Users\Administrator\.workbuddy\binaries\go\go\bin

echo == go build ==
go build ./...

echo == go vet ==
go vet ./...

echo == go test: Lab4 ShardKV ==
go test ./src/shardkv/... -count=1 -timeout 300s

echo == build binaries (gateway / kvcli / demo) ==
if not exist bin mkdir bin
go build -o bin/gateway.exe ./src/gateway
go build -o bin/kvcli.exe   ./src/kvcli
go build -o bin/demo.exe    ./src/demo

echo == full-stack smoke: run demo (cluster -^> HTTP gateway -^> client) ==
go run ./src/demo
