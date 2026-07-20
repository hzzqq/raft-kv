# Lab4 ShardKV 开发便捷入口
GO ?= go
export PATH := $(PATH):/c/Users/Administrator/.workbuddy/binaries/go/go/bin

.PHONY: build vet test test-race clean

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

clean:
	$(GO) clean ./...
