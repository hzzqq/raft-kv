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

clean:
	$(GO) clean ./...
