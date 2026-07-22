@echo off
chcp 65001 >nul 2>nul
REM =====================================================================
REM  raft-kv launcher (Windows .bat)
REM  Boots the whole ShardKV system: 2 in-process replica clusters + a
REM  long-running HTTP gateway.
REM
REM  Usage (pass arg directly):
REM    start.bat                -> interactive menu
REM    start.bat serve          -> foreground gateway (Ctrl+C to stop)
REM    start.bat bg             -> background gateway (log: raft-kv-gateway.log)
REM    start.bat stop           -> stop background gateway
REM    start.bat status         -> cluster health overview (gateway must run)
REM    start.bat migrate        -> live migration progress (gateway must run)
REM    start.bat configs        -> config history (gateway must run)
REM    start.bat build          -> build all binaries into bin/
REM    start.bat demo           -> one-shot end-to-end demo (exits when done)
REM    start.bat test           -> run shard KV tests
REM    start.bat cli <args>     -> run kvcli, e.g. start.bat cli get hello
REM =====================================================================

cd /d %~dp0

REM ---- Go toolchain probe (prepend managed go, prefer "go" in PATH, else full path) ----
set MGO=C:\Users\Administrator\.workbuddy\binaries\go\go\bin
set PATH=%PATH%;%MGO%
set GO=go
where go >nul 2>nul
if errorlevel 1 (
  if exist "%MGO%\go.exe" ( set GO=%MGO%\go.exe ) else ( set GO= )
)
set GOCACHE=C:\Users\Administrator\.cache\go-raftkv
set GOPATH=C:\Users\Administrator\.cache\gopath-raftkv
set GO111MODULE=on

set ADDR=:8080
set PIDFILE=raft-kv-gateway.pid
set LOGFILE=raft-kv-gateway.log

REM ---- dispatch ----
if "%1"=="" goto menu
if /I "%1"=="serve"   goto serve
if /I "%1"=="bg"      goto bg
if /I "%1"=="stop"    goto stop
if /I "%1"=="status"  goto status
if /I "%1"=="migrate" goto migrate
if /I "%1"=="configs" goto configs
if /I "%1"=="build"   goto build
if /I "%1"=="demo"    goto demo
if /I "%1"=="test"    goto test
if /I "%1"=="cli"     goto cli
goto usage

REM =====================================================================
:menu
cls
echo ============================================================
echo            raft-kv launcher
echo ============================================================
echo  [1] serve   - foreground gateway (Ctrl+C to stop)
echo  [2] bg      - background gateway
echo  [3] stop    - stop background gateway
echo  [4] status  - cluster health overview (needs gateway)
echo  [5] build   - build all binaries into bin/
echo  [6] demo    - one-shot end-to-end demo
echo  [7] test    - run shard KV tests
echo  [8] cli     - run kvcli
echo  [0] exit
echo ============================================================
set /p CHOICE=Select [1-8,0]:
if "%CHOICE%"=="1" goto serve
if "%CHOICE%"=="2" goto bg
if "%CHOICE%"=="3" goto stop
if "%CHOICE%"=="4" goto status
if "%CHOICE%"=="5" goto build
if "%CHOICE%"=="6" goto demo
if "%CHOICE%"=="7" goto test
if "%CHOICE%"=="8" goto cli
if "%CHOICE%"=="0" goto end
echo Invalid choice, try again.
goto menu

REM =====================================================================
:serve
call :ensure_go || goto end
echo [build] building gateway...
%GO% build -o bin\gateway.exe ./src/gateway
if errorlevel 1 ( echo build failed & goto end )
echo raft-kv gateway listening on %ADDR% (Ctrl+C to stop; in another terminal: start.bat cli get hello)
bin\gateway.exe %ADDR%
goto end

REM =====================================================================
:bg
call :ensure_go || goto end
echo [build] building gateway...
%GO% build -o bin\gateway.exe ./src/gateway
if errorlevel 1 ( echo build failed & goto end )
echo starting gateway in background, log=%LOGFILE%
start "raft-kv-gateway" /B bin\gateway.exe %ADDR% > %LOGFILE% 2>&1
for /f "tokens=2 delims==;" %%p in ('wmic process where "name='gateway.exe'" get processid /value 2^>nul ^| findstr /I "gateway.exe"') do (
  echo %%p> %PIDFILE% 2>nul
)
echo access: curl http://localhost%ADDR%/healthz
echo stop:  start.bat stop
goto end

REM =====================================================================
:stop
if exist %PIDFILE% (
  for /f "usebackq tokens=*" %%i in (%PIDFILE%) do taskkill /PID %%i /F >nul 2>&1
  del /Q %PIDFILE% >nul 2>&1
  echo stopped (PID from %PIDFILE%)
) else (
  taskkill /FI "WINDOWTITLE eq raft-kv-gateway" /F >nul 2>&1
  echo %PIDFILE% not found, tried stopping by window title
)
goto end

REM =====================================================================
:status
curl -s http://localhost%ADDR%/status 2>nul | %GO% run ./src/statusfmt
if errorlevel 1 echo gateway not running? run: start.bat bg or start.bat serve
goto end

:configs
curl -s http://localhost%ADDR%/debug/configs 2>nul
if errorlevel 1 echo gateway not running? run: start.bat bg or start.bat serve
goto end

:migrate
curl -s http://localhost%ADDR%/debug/migrate 2>nul
if errorlevel 1 echo gateway not running? run: start.bat bg or start.bat serve
goto end

REM =====================================================================
:build
call :ensure_go || goto end
%GO% build ./...
if not exist bin mkdir bin
%GO% build -o bin\gateway.exe ./src/gateway
%GO% build -o bin\kvcli.exe   ./src/kvcli
%GO% build -o bin\demo.exe    ./src/demo
echo build done -> bin/
goto end

:demo
call :ensure_go || goto end
%GO% run ./src/demo
goto end

:test
call :ensure_go || goto end
%GO% test ./src/shardkv/... -count=1 -timeout 300s %2 %3 %4 %5
goto end

:cli
call :ensure_go || goto end
%GO% run ./src/kvcli %2 %3 %4 %5 %6 %7
goto end

REM =====================================================================
:usage
echo Usage: start.bat [serve^|bg^|stop^|status^|migrate^|configs^|build^|demo^|test^|cli]
goto end

REM ---- subroutine: verify Go is available (returns on success, exit /b 1 on fail) ----
:ensure_go
if exist "%GO%" goto :eof
where %GO% >nul 2>nul
if errorlevel 1 (
  echo [error] Go toolchain not found: %GO%
  echo Install Go or confirm managed path C:\Users\Administrator\.workbuddy\binaries\go\go\bin exists.
  exit /b 1
)
goto :eof

:end
