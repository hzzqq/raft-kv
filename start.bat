@echo off
REM raft-kv 启动脚本（Windows cmd / PowerShell）
REM
REM 把整套 ShardKV 系统「真正拉起来」：进程内启动 2 组副本集群，并起一个常驻
REM HTTP 网关（默认 :8080），可被 kvcli / curl 持续访问。旧版只跑一次性 demo 后
REM 退出，这个版本默认前台常驻，另提供后台启动 + 停止。
REM
REM 用法：
REM   start.bat              （默认 = serve：构建网关并前台常驻，Ctrl+C 停止）
REM   start.bat serve        同上
REM   start.bat bg           后台启动（日志 raft-kv-gateway.log）
REM   start.bat stop         停止后台网关
REM   start.bat build        构建全部二进制到 bin/
REM   start.bat demo         跑一次性端到端演示（跑完即退）
REM   start.bat test         跑分片 KV 测试
REM   start.bat cli <参数>   运行 kvcli，例如 start.bat cli get hello
cd /d %~dp0
set PATH=%PATH%;C:\Users\Administrator\.workbuddy\binaries\go\go\bin
set GOCACHE=C:\Users\Administrator\.cache\go-raftkv
set GOPATH=C:\Users\Administrator\.cache\gopath-raftkv
set GO111MODULE=on

set ADDR=:8080
set PIDFILE=raft-kv-gateway.pid
set LOGFILE=raft-kv-gateway.log

if "%1"=="" goto serve
if /I "%1"=="serve" goto serve
if /I "%1"=="bg" goto bg
if /I "%1"=="stop" goto stop
if /I "%1"=="build" goto build
if /I "%1"=="demo" goto demo
if /I "%1"=="test" goto test
if /I "%1"=="cli" goto cli
goto usage

:serve
go build -o bin\gateway.exe ./src/gateway
echo >> raft-kv 网关启动于 %ADDR%（Ctrl+C 停止；另开终端用 start.bat cli get hello 访问）
bin\gateway.exe %ADDR%
goto end

:bg
go build -o bin\gateway.exe ./src/gateway
start "raft-kv-gateway" /B bin\gateway.exe %ADDR% > %LOGFILE% 2>&1
echo >> 后台启动：日志=%LOGFILE%
echo >> 访问：curl http://localhost%ADDR%/healthz
echo >> 停止：start.bat stop
goto end

:stop
if exist %PIDFILE% (
  for /f "usebackq tokens=*" %%i in (%PIDFILE%) do taskkill /PID %%i /F >nul 2>&1
  del /Q %PIDFILE% >nul 2>&1
  echo >> 已停止
) else (
  taskkill /FI "WINDOWTITLE eq raft-kv-gateway" /F >nul 2>&1
  echo >> 未找到 %PIDFILE%，已尝试按窗口标题停止
)
goto end

:build
go build ./...
if not exist bin mkdir bin
go build -o bin\gateway.exe ./src/gateway
go build -o bin\kvcli.exe   ./src/kvcli
go build -o bin\demo.exe    ./src/demo
echo >> 构建完成 -^> bin/
goto end

:demo
go run ./src/demo
goto end

:test
go test ./src/shardkv/... -count=1 -timeout 300s %2 %3 %4 %5
goto end

:cli
go run ./src/kvcli %2 %3 %4 %5 %6 %7
goto end

:usage
echo 用法: start.bat [serve^|bg^|stop^|build^|demo^|test^|cli]
goto end

:end
