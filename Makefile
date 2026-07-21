# Lab4 ShardKV 开发便捷入口
GO ?= go
export PATH := $(PATH):/c/Users/Administrator/.workbuddy/binaries/go/go/bin

.PHONY: build vet test test-race clean lint cover test-cover build-binaries demo serve serve-bg stop cli

build:
	$(GO) build ./...

vet:
	$(GO) vet ./...

test:
	$(GO) test ./src/shardkv/... -count=1 -timeout 300s

# 注意：本机 Windows 环境无 gcc，无法编译 race 检测器；此目标在支持 -race 的
# 环境（Linux / macOS / 装了 gcc 的 Windows）下才有意义。
test-race:
	$(GO) test ./src/shardkv/... -race -count=1 -timeout 300s

# 构建三个可执行：gateway / kvcli / demo（输出到 bin/）。
build-binaries:
	mkdir -p bin
	$(GO) build -o bin/gateway ./src/gateway
	$(GO) build -o bin/kvcli   ./src/kvcli
	$(GO) build -o bin/demo    ./src/demo

# 全栈冒烟：直接跑 demo（cluster -> HTTP 网关 -> HTTP 客户端）。
demo: build-binaries
	$(GO) run ./src/demo

# 前台常驻：构建网关并拉起（默认 :8080），Ctrl+C 停止。
# 这是「把系统真正跑起来」的入口，替代旧版跑完 demo 就退的行为。
serve: build-binaries
	./bin/gateway :8080

# 后台常驻：写 PID + 日志，便于远程 / 自动化场景。
serve-bg: build-binaries
	./bin/gateway :8080 > raft-kv-gateway.log 2>&1 &
	echo $$! > raft-kv-gateway.pid
	@echo "gateway 后台已启动，PID=$$(cat raft-kv-gateway.pid)，日志 raft-kv-gateway.log；停止：make stop"

# 停止后台网关。
stop:
	@if [ -f raft-kv-gateway.pid ]; then kill $$(cat raft-kv-gateway.pid) 2>/dev/null && echo "已停止" || echo "进程不存在"; rm -f raft-kv-gateway.pid; fi

# 运行命令行客户端（例：make cli args="get hello"）。
cli:
	$(GO) run ./src/kvcli $(args)

clean:
	$(GO) clean ./...

# 静态检查（需先安装 golangci-lint：https://golangci-lint.run/install/）。
# 配置见 .golangci.yml。本地无 gcc 不影响 lint（它是纯静态分析）。
lint:
	golangci-lint run ./...

# 覆盖率：跑全量测试并生成 cover.out，再打印「总覆盖率」一行概览。
# 注意：shardkv 的 churn 用例较重（单次 ~100s+），整体跑完需数分钟，已给足 timeout。
cover:
	$(GO) test ./... -count=1 -timeout 900s -coverprofile=cover.out -covermode=atomic
	$(GO) tool cover -func=cover.out | tail -1
	@echo "HTML 报告：go tool cover -html=cover.out"

# 与 cover 同义，方便记忆。
test-cover: cover
