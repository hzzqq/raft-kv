#!/usr/bin/env python3
"""自动发现并运行全部「校验器自身回归测试」（免 Go，守护门禁不退化）。

仓库沉淀了 13 个免 Go 校验器，每个都有 `scripts/tests/test_*.py` 形式的 fixture 驱动
回归测试。此前这些测试被**硬编码**在 3 处（Makefile 的 `selftest`/`ci` 目标、`scripts/
ci-local.sh docs`、`ci.yml` 的 docs-links job 末尾），任一清单漏列即导致新校验器的
自测被静默跳过——这正是 cycle110「门禁自身退化」同类隐患的变种（R2 隐性）。

本脚本一次性消除该重复：自动 glob `scripts/tests/test_*.py` 并逐个子进程运行，汇总
PASS/FAIL，任一失败即返回非 0。Makefile / ci-local.sh / CI 三处统一改为调用本脚本，
从此新增 `test_*.py` 即自动纳入，无清单漂移。

用法：
  python3 scripts/run_selftests.py          # 运行全部，打印每条结果
  python3 scripts/run_selftests.py --quiet  # 仅打印汇总

退出码：0=全部通过；1=存在失败。
"""
import argparse
import glob
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTS_DIR = os.path.join(ROOT, "scripts", "tests")


def discover() -> list:
    pattern = os.path.join(TESTS_DIR, "test_*.py")
    return sorted(glob.glob(pattern))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quiet", action="store_true", help="仅打印汇总")
    args = ap.parse_args()

    tests = discover()
    if not tests:
        print(f"[WARN] 未发现任何自测：{TESTS_DIR}", file=sys.stderr)
        return 0

    results = []
    for path in tests:
        rel = os.path.relpath(path, ROOT)
        if not args.quiet:
            print(f"==> {rel}")
        proc = subprocess.run([sys.executable, path], cwd=ROOT,
                              capture_output=not args.quiet, text=True)
        ok = proc.returncode == 0
        if not args.quiet and not ok and proc.stdout:
            for line in proc.stdout.strip().splitlines()[-6:]:
                print("    " + line)
        results.append((rel, ok, proc.returncode))

    print("\n" + "=" * 56)
    print("校验器自测汇总：")
    failed = 0
    for rel, ok, code in results:
        mark = "PASS" if ok else "FAIL"
        if not ok:
            failed += 1
        print(f"  [{mark}] {rel}  (exit={code})")
    print("=" * 56)
    if failed:
        print(f"自测失败：{failed}/{len(results)} 项。")
        return 1
    print(f"全部通过：{len(results)}/{len(results)} 项。OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
