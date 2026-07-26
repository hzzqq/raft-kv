#!/usr/bin/env python3
"""校验「免 Go 校验器套件」自身的接线一致性（meta 门禁）。

仓库沉淀了 8+ 个纯 Python 静态校验器（scripts/check_*.py + gen_changelog.py），
它们须同时出现在 4 个位置才能发挥门禁作用：
  1. scripts/check_all.py 的 CHECKS 列表（本地/统一入口）
  2. .github/workflows/ci.yml 的 docs-links job（CI 侧）
  3. README.md 的脚本索引表（人类可读、可发现）
  4. scripts/pre-commit.sh 或 Makefile 的 hooks 目标（提交前门禁）

任一新加的校验器若漏接其中一处，门禁即出现「盲区」，且无人察觉——
这正是本脚本要防的「harness 自身漂移」。属免 Go 静态检查。

用法：
  python3 scripts/check_doc_inventory.py
返回 0 = 全部校验器接线一致；非 0 = 发现遗漏。
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS = os.path.join(ROOT, "scripts")
STATE = os.path.join(ROOT, ".workbuddy", "self-driving", "state.json")
README = os.path.join(ROOT, "README.md")
CI = os.path.join(ROOT, ".github", "workflows", "ci.yml")
CHECK_ALL = os.path.join(ROOT, "scripts", "check_all.py")
PRECOMMIT = os.path.join(ROOT, "scripts", "pre-commit.sh")
MAKEFILE = os.path.join(ROOT, "Makefile")

# 受本脚本约束的校验器（basename）；gen_changelog 虽非 check_* 但同属免 Go 门禁
GUARDED = [
    "check_md_links.py",
    "check_docs_endpoints.py",
    "check_metrics_docs.py",
    "check_api_docs.py",
    "gen_changelog.py",
    "check_state_integrity.py",
    "check_go_patterns.py",
    "check_doc_inventory.py",
    "check_coverage_doc.py",
    "check_godoc.py",
    "check_test_coverage.py",
]


def _read(p):
    try:
        return open(p, encoding="utf-8").read()
    except OSError:
        return ""


def main() -> int:
    problems = 0
    ca = _read(CHECK_ALL)
    ci = _read(CI)
    readme = _read(README)
    pc = _read(PRECOMMIT)
    mk = _read(MAKEFILE)

    for name in GUARDED:
        miss = []
        # 1) check_all.py CHECKS
        if name not in ca:
            miss.append("check_all.py")
        # 2) ci.yml docs-links
        if name not in ci:
            miss.append("ci.yml")
        # 3) README（gen_changelog 以 --verify 形式出现在表内，仍按 basename 匹配）
        if name not in readme:
            miss.append("README.md")
        if miss:
            err = f"`{name}` 未接入：{', '.join(miss)}"
            print(f"  [缺陷] {err}", file=sys.stderr)
            problems += 1

    # 4) pre-commit / Makefile hooks 至少一处引用 check_all（即整套门禁被提交前启用）
    if "check_all.py" not in pc and "check_all.py" not in mk:
        print("  [缺陷] pre-commit.sh 与 Makefile 均未引用 check_all.py"
              "（提交前门禁未启用）。", file=sys.stderr)
        problems += 1

    # 反向：check_all.py 里列出的脚本文件是否真实存在（防死引用）
    for m in re.finditer(r'scripts/(check_\w+\.py|gen_changelog\.py)', ca):
        rel = os.path.join(SCRIPTS, m.group(1))
        if not os.path.isfile(rel):
            print(f"  [缺陷] check_all.py 引用了不存在的脚本：{m.group(1)}", file=sys.stderr)
            problems += 1

    if problems:
        print(f"校验器接线一致性校验失败：{problems} 处遗漏。", file=sys.stderr)
        return 1
    print(f"校验器接线一致性 OK（受管 {len(GUARDED)} 个，全部接入"
          f" check_all / ci.yml / README，且 pre-commit 已启用）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
