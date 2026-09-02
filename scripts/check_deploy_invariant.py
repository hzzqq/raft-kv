#!/usr/bin/env python3
"""部署不变量 + 实验回归门禁（需 Go 工具链）。

本脚本把此前「只在文档里声称、却从未被任何门禁运行」的两类最有价值测试真正接进门禁：

  - deploycheck 不变量测试（I4 真部署化的核心）：断言 compose/Prometheus/Grafana/
    告警与代码真实暴露的指标名 + label 维度一致，否则「看板幻觉 / 拓扑漂移」当场暴露。
  - 实验性能回归（场景 C）：分片扩展比必须 >= 阈值，否则心跳节流修复被 revert 后
    吞吐跌回 ~100 ops/sec、扩展比趋近 1.0x 也无人察觉。

运行策略（兼顾提交速度与 CI 严谨）：
  - 默认：跑 deploycheck 测试 + experiments.TestScalingRatio（快，约数秒），外加
    leader --assert / partition --assert / migration --assert——场景 A/B/D 最强正确性保证
    （无脑裂双写 / 零丢失写，I174 落盘的 client_view_*.json 的 ok 字段）每次提交必跑
    （leader/partition 于 I176、migration 于 I177 从 CI-only 提升为 always-on，
    正确性比性能更该 always-on；migration 自 I177 起已叠加「迁移期间网络分区」三重并发故障，
    是 ShardKV 最硬路径的客户端视角实证，代价是每次提交多 ~8~30s）。
  - 若环境变量 RAFTKV_PERF_GATE=1（CI 设置）：额外跑完整 perf --assert
    （1/2/3/5 组全窗口基准，约 15~25s，作为重量级性能回归护栏）。

用法：
    python3 scripts/check_deploy_invariant.py
任一子检查失败即非零退出，可直接作为 pre-commit / CI 门禁。
"""
import os
import shutil
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _find_go():
    # 与 deploy_smoke_local.py 一致：优先 WorkBuddy 托管的 Go（本机唯一能正常编译的
    # 工具链）。E:\go-sdk 与 E:\e\go-sdk 两份安装 src/vendor 损坏，编译标准库报
    # "package X is not in std"。nt 下必须用盘符绝对路径，否则 Python 把 "/e/..." 解析成
    # E:\e\go-sdk\... 那份坏的。
    if os.name == "nt":
        cands = (
            "C:/Users/Administrator/.workbuddy/binaries/go/go/bin/go.exe",
            "E:/go-sdk/go/bin/go.exe",
        )
    else:
        cands = (
            "/c/Users/Administrator/.workbuddy/binaries/go/go/bin/go",
            "/e/go-sdk/go/bin/go",
            "E:/go-sdk/go/bin/go",
        )
    for cand in cands:
        if os.path.exists(cand):
            return cand
    return shutil.which("go") or "go"


def _run(go, cwd, args, label):
    cmd = [go] + args
    # 不显式设 GOROOT：在 experiments 这种独立 go.mod 子模块里，显式覆盖 GOROOT 会让 go
    # 误报 "package X is not in std"。让 go 自行推断。但本机坏安装 E:\e\go-sdk 与本托管 go
    # 共用默认 GOCACHE，且 root 模块（deploycheck）与 experiments 子模块共用同一 GOCACHE
    # 时会互相污染 std 缓存触发偶发 not in std——故用独立干净 GOCACHE（.gocache_invariant），
    # 且不与 deploy_smoke_local.py 的缓存混用，避免 check_all 连跑时跨模块缓存污染。
    env = dict(os.environ)
    gc = os.path.join(ROOT, ".gocache_invariant")
    env["GOCACHE"] = gc
    print(f"==> [部署不变量] {label}: {' '.join(cmd)} (cwd={os.path.relpath(cwd, ROOT)})")
    for attempt in range(1, 4):
        proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, env=env)
        if proc.returncode == 0:
            break
        if attempt < 3:
            print(f"    [重试 {attempt}/3] {label} 失败（疑似 not in std flake），清空 GOCACHE 重来")
            shutil.rmtree(gc, ignore_errors=True)
            os.makedirs(gc, exist_ok=True)
    if proc.stdout:
        for line in proc.stdout.strip().splitlines()[-12:]:
            print("    " + line)
    if proc.stderr:
        for line in proc.stderr.strip().splitlines()[-12:]:
            print("    [stderr] " + line)
    if proc.returncode != 0:
        print(f"    ✗ {label} 失败 (exit={proc.returncode})")
        return False
    print(f"    ✓ {label} 通过")
    return True


def main() -> int:
    go = _find_go()
    if shutil.which(go) is None and not os.path.exists(go):
        print("✗ 找不到 go 工具链（PATH 与 /e/go-sdk/go/bin/go.exe 均不存在）。"
              "部署不变量测试无法运行 —— 请在 CI/提交环境安装 Go。", file=sys.stderr)
        return 1

    ok = True
    # 1) deploycheck：看板/告警 指标名 + label 维度 真实存在（R5）。
    ok &= _run(go, ROOT, ["test", "./src/deploycheck/", "-count=1"],
               "deploycheck 不变量测试")

    # 2) 场景 C 分片扩展比回归护栏（R2 的快路径：单组地板 + 双组>=1.3x）。
    exp_dir = os.path.join(ROOT, "experiments")
    ok &= _run(go, exp_dir, ["test", "-run", "TestScalingRatio", "-count=1"],
               "实验 分片扩展比回归 (TestScalingRatio)")

    # 3) 场景 A/B/D 客户端视角不变量回归（I175→I176→I177）：每次提交必跑，正确性比性能更该 always-on。
    #    leader/partition/migration 的最强正确性保证（无脑裂双写 / 零丢失写，I174 落盘的 client_view_*.json
    #    的 ok 字段）被门禁强制；migration 自 I177 起 always-on，且已叠加「迁移期间网络分区」三重并发故障
    #    （跨组 rebalance + 一组一副本崩溃 + 一组一副本网络分区），是 ShardKV 最硬路径的客户端视角实证。
    #    代价是每次提交多 ~8~30s，但这是系统最该守住的不变量。
    ok &= _run(go, exp_dir, ["run", ".", "-scenario", "leader", "-assert"],
               "实验 场景A 客户端视角不变量 (leader --assert)")
    ok &= _run(go, exp_dir, ["run", ".", "-scenario", "partition", "-assert"],
               "实验 场景B 客户端视角不变量 (partition --assert)")
    ok &= _run(go, exp_dir, ["run", ".", "-scenario", "migration", "-assert"],
               "实验 场景D 多组迁移故障不变量 (migration --assert)")
    ok &= _run(go, exp_dir, ["run", ".", "-scenario", "witness", "-assert"],
               "实验 场景E Witness 副本容错不变量 (witness --assert)")

    # 4) 重量级完整性能回归（I176）：仅 CI 开启，避免拖慢每次提交。
    #    perf 全窗口基准（~15~25s）是重路径，正确性已由 A/B/D 覆盖，性能回归放 CI 更经济。
    if os.environ.get("RAFTKV_PERF_GATE") == "1":
        ok &= _run(go, exp_dir, ["run", "." , "-scenario", "perf", "-assert"],
                   "实验 完整性能回归 (perf --assert)")
    else:
        print("==> [部署不变量] 跳过完整 perf --assert（默认不跑；CI 设 RAFTKV_PERF_GATE=1 启用）")

    if not ok:
        print("\n✗ 部署不变量/实验回归门禁未通过。", file=sys.stderr)
        return 1
    print("\n✓ 部署不变量 + 实验回归门禁通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
