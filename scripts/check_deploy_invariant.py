#!/usr/bin/env python3
"""部署不变量 + 实验回归门禁（需 Go 工具链）。

本脚本把此前「只在文档里声称、却从未被任何门禁运行」的两类最有价值测试真正接进门禁：

  - deploycheck 不变量测试（I4 真部署化的核心）：断言 compose/Prometheus/Grafana/
    告警与代码真实暴露的指标名 + label 维度一致，否则「看板幻觉 / 拓扑漂移」当场暴露。
  - 实验性能回归（场景 C）：分片扩展比必须 >= 阈值，否则心跳节流修复被 revert 后
    吞吐跌回 ~100 ops/sec、扩展比趋近 1.0x 也无人察觉。

运行策略（兼顾提交速度与 CI 严谨）：
  - 默认：跑 deploycheck 测试 + experiments.TestScalingRatio（快，约数秒），外加
    leader --assert / partition --assert——场景 A/B 最强正确性保证（无脑裂双写 / 零丢失写，
    I174 落盘的 client_view_*.json 的 ok 字段）每次提交必跑（I176 从 CI-only 提升为 always-on，
    正确性比性能更该 always-on，代价是每次提交多 ~8~20s）。
  - 若环境变量 RAFTKV_PERF_GATE=1（CI 设置）：额外跑完整 perf --assert
    （1/2/3/5 组全窗口基准，约 15~25s，作为重量级性能回归护栏）与 migration --assert
    （多组跨组 rebalance + 副本崩溃期间零丢失写，I176 新场景，~5~10s 重路径）。

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
    # 优先用 PATH 里的 go；否则退回到本机托管安装的固定路径（git 钩子子进程的
    # PATH 可能不含 go，导致门禁误判通过——必须显式定位）。
    for cand in ("go", "/e/go-sdk/go/bin/go.exe"):
        if shutil.which(cand):
            return cand
        if os.path.exists(cand):
            return cand
    return "go"


def _run(go, cwd, args, label):
    cmd = [go] + args
    print(f"==> [部署不变量] {label}: {' '.join(cmd)} (cwd={os.path.relpath(cwd, ROOT)})")
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
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

    # 3) 场景 A/B 客户端视角不变量回归（I175→I176）：每次提交必跑，正确性比性能更该 always-on。
    #    leader/partition 的最强正确性保证（无脑裂双写 / 零丢失写，I174 落盘的 client_view_*.json
    #    的 ok 字段）被门禁强制；代价是每次提交多 ~8~20s，但这是系统最该守住的不变量。
    ok &= _run(go, exp_dir, ["run", ".", "-scenario", "leader", "-assert"],
               "实验 场景A 客户端视角不变量 (leader --assert)")
    ok &= _run(go, exp_dir, ["run", ".", "-scenario", "partition", "-assert"],
               "实验 场景B 客户端视角不变量 (partition --assert)")

    # 4) 重量级完整性能回归 + 场景 D 多组迁移故障不变量（I176）：仅 CI 开启，避免拖慢每次提交。
    #    perf 全窗口基准（~15~25s）+ migration（多组 churn 期间零丢失写，~5~10s）都是重路径。
    if os.environ.get("RAFTKV_PERF_GATE") == "1":
        ok &= _run(go, exp_dir, ["run", "." , "-scenario", "perf", "-assert"],
                   "实验 完整性能回归 (perf --assert)")
        ok &= _run(go, exp_dir, ["run", ".", "-scenario", "migration", "-assert"],
                   "实验 场景D 多组迁移故障不变量 (migration --assert)")
    else:
        print("==> [部署不变量] 跳过完整 perf/migration --assert（默认不跑；CI 设 RAFTKV_PERF_GATE=1 启用）")

    if not ok:
        print("\n✗ 部署不变量/实验回归门禁未通过。", file=sys.stderr)
        return 1
    print("\n✓ 部署不变量 + 实验回归门禁通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
