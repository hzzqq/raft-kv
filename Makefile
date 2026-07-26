# Lab4 ShardKV 开发便捷入口
GO ?= go
export PATH := $(PATH):/c/Users/Administrator/.workbuddy/binaries/go/go/bin

.PHONY: build vet test test-shardkv test-all test-race fmt bench clean lint cover test-cover build-binaries demo serve serve-bg stop cli smoke hooks docs test-cov test-cov-verify selftest ci

build:
	$(GO) build ./...

vet:
	$(GO) vet ./...

# 仅跑 ShardKV 重点包（轻量，向后兼容旧习惯 / CI）。
test:
	$(GO) test ./src/shardkv/... -count=1 -timeout 300s

# 同 test，语义别名（显式表达"只 shardkv"）。
test-shardkv:
	$(GO) test ./src/shardkv/... -count=1 -timeout 300s

# 全量：覆盖所有包（含本轮新增的 gateway/metrics/kvcli/statusfmt/demo 的 cluster-free 测试）。
# shardkv 的 churn 用例较重，已给足 timeout；CI 环境另有 race job。
test-all:
	$(GO) test ./... -count=1 -timeout 600s

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
	python3 scripts/check_go_coverage.py --profile cover.out
	@echo "HTML 报告：go tool cover -html=cover.out"

# 与 cover 同义，方便记忆。
test-cover: cover

# 安装提交前门禁：把 scripts/pre-commit.sh 装入 .git/hooks/pre-commit，
# 使每次 git commit 自动跑全部免 Go 静态自检（CHANGELOG 同步 / 文档一致性 /
# 日志完整性），任一失败即阻断提交——避免漂移类缺陷被静默落库。
hooks:
	@mkdir -p .git/hooks
	cp scripts/pre-commit.sh .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit 2>/dev/null || true
	@echo "已安装 pre-commit 钩子（运行 make hooks 可续装 / 重装）。"

# 仅跑免 Go 文档/日志门禁（等价于 CI docs-links job 的本地入口，
# 不依赖 go/gcc，可在任意环境复跑）。
docs:
	$(PYTHON3) scripts/check_all.py

# 刷新 docs/coverage.md 的「模块↔测试」映射表（免 Go 扫描，自动生成）。
test-cov:
	python3 scripts/gen_test_coverage.py

# 仅校验映射表与实际代码一致（不写文件），可供 CI / pre-commit 使用。
test-cov-verify:
	python3 scripts/gen_test_coverage.py --verify

# 校验器自检：跑 scripts/tests 下的单元测试，守护「门禁自身」不退化（免 Go）。
selftest:
	python3 scripts/tests/test_check_test_coverage.py
	python3 scripts/tests/test_check_secrets.py
	python3 scripts/tests/test_check_godoc.py
	python3 scripts/tests/test_check_md_links.py
	python3 scripts/tests/test_check_api_docs.py
	python3 scripts/tests/test_check_metrics_docs.py
	python3 scripts/tests/test_check_docs_endpoints.py
	python3 scripts/tests/test_check_go_patterns.py
	python3 scripts/tests/test_check_state_integrity.py
	python3 scripts/tests/test_check_doc_inventory.py
	python3 scripts/tests/test_check_coverage_doc.py
	python3 scripts/tests/test_check_hooks_installed.py
	python3 scripts/tests/test_check_go_coverage.py

# 本地等效 CI 门禁（免 Go）：跑统一自检编排器 + 校验器自身回归，
# 等价于 CI `gate` job。不依赖 go/gcc，可在任意环境秒级复跑整条门禁链。
ci:
	python3 scripts/check_all.py
	python3 scripts/tests/test_check_test_coverage.py
	python3 scripts/tests/test_check_secrets.py
	python3 scripts/tests/test_check_godoc.py
	python3 scripts/tests/test_check_md_links.py
	python3 scripts/tests/test_check_api_docs.py
	python3 scripts/tests/test_check_metrics_docs.py
	python3 scripts/tests/test_check_docs_endpoints.py
	python3 scripts/tests/test_check_go_patterns.py
	python3 scripts/tests/test_check_state_integrity.py
	python3 scripts/tests/test_check_doc_inventory.py
	python3 scripts/tests/test_check_coverage_doc.py
	python3 scripts/tests/test_check_hooks_installed.py

# 格式检查：列出 ./src 下未通过 gofmt 的文件。默认不自动 -w，避免波及上游 6.824
# 起始代码；如需就地重写本轮回改动文件，可手动：gofmt -w ./src/<pkg>。
fmt:
	gofmt -l ./src

# 快速冒烟门禁（秒级反馈，沙箱安全）：编译 + vet + 各包 cluster-free 单测，
# 不拉起重型 Raft 集群 churn 用例。深测请用 test-all。脚本自管 Go 工具链路径，
# 在缺 gcc 的 Windows 环境下零配置可跑（与 run-tests.sh 同源）。
smoke:
	./scripts/smoke.sh

# 基准：跑 raft 提交路径基准（BenchmarkRaftAgree 等）各一次，量化提交吞吐。
# 需要连后端压测时也可用：make cli args="bench mixed 2000 8"（连已启动网关）。
bench:
	$(GO) test -run='^$$' -bench=. -benchtime=1x ./src/raft
