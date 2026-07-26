#!/usr/bin/env python3
"""构建/覆盖率临时产物泄漏护栏（免 Go 工具链）。

本仓库的覆盖率/测试工具链会在仓库根目录写出临时产物：
  - `go test -coverprofile` 产生的 `covtest_*` 临时目录（内含 cfg.json / cover.out）
  - 根级 `*.out`（覆盖率）、`*.test`（go test 二进制）、`tmp_*` 等

这些产物一旦进入工作树且未被 `.gitignore` 覆盖（如 `covtest_*/cfg.json`），就会被
`git add` 误提交，污染仓库。本脚本在提交前/CI 中扫描根目录，发现未忽略的泄漏即
硬失败；已忽略但仍残留的给出 WARN 清理建议。同时校验 `.gitignore` 是否含有
`covtest_*/` 规则——这是根因防护（漏规则则任何 covtest 产物都能被提交）。

属免 Go 静态检查，接入 check_all / ci.yml / README（与另三个 meta 校验器同列）。

用法：
    python3 scripts/check_leaked_artifacts.py          # 扫描并报告
    python3 scripts/check_leaked_artifacts.py --root X # 指定仓库根（测试用）

退出码：0=无未忽略泄漏；1=发现未忽略泄漏或 .gitignore 缺 covtest 规则。
"""
import argparse
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 根目录级泄漏文件名/目录名模式（仅扫根级，避免误伤 src/scripts 等源码树）。
LEAK_RE = re.compile(r'^(covtest_.*|tmp_.*)$|\.(out|test)$')
COVTEST_RULE = "covtest_*/"


def _gitignored(root: str, path: str) -> bool:
    """返回 path（相对 root）是否被 .gitignore 忽略。"""
    rel = os.path.relpath(path, root)
    try:
        proc = subprocess.run(
            ["git", "-C", root, "check-ignore", "-v", rel],
            capture_output=True, text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return proc.returncode == 0


def _gitignore_has_covtest_rule(root: str) -> bool:
    p = os.path.join(root, ".gitignore")
    try:
        text = open(p, encoding="utf-8").read()
    except OSError:
        return False
    return COVTEST_RULE.strip() in text.splitlines()


def analyze(root: str):
    """返回 (fails, warns)，各为 (relpath, reason) 列表。"""
    fails, warns = [], []
    if not os.path.isdir(root):
        return fails, warns

    for name in sorted(os.listdir(root)):
        full = os.path.join(root, name)
        rel = os.path.relpath(full, root)
        # 只关心根级、且匹配泄漏模式的条目
        if not LEAK_RE.search(name):
            continue
        # 跳过已被整体忽略的目录（如 bin/ 已被 .gitignore 覆盖，属正常产物）
        if _gitignored(root, full):
            warns.append((rel, "已被 .gitignore 忽略但仍残留，建议清理"))
            continue
        # 未忽略 = 会被 git add 提交，硬失败
        if os.path.isdir(full) and name.startswith("covtest_"):
            # covtest 目录即便根被忽略，其内部未忽略文件才是真泄漏；这里根未忽略即整体泄漏
            fails.append((rel, "未忽略的临时目录，会被提交（覆盖率工具链泄漏）"))
        elif name.endswith(".out") or name.endswith(".test") or name.startswith("tmp_"):
            fails.append((rel, "未忽略的临时产物，会被提交"))
        else:
            warns.append((rel, "残留的临时产物，建议清理"))

    if not _gitignore_has_covtest_rule(root):
        fails.append((
            ".gitignore",
            f"缺少 `{COVTEST_RULE.strip()}` 规则：covtest 临时目录可被误提交（根因）",
        ))
    return fails, warns


def run(root: str) -> int:
    root = os.path.abspath(root)
    fails, warns = analyze(root)
    for rel, reason in warns:
        print(f"  [WARN] {rel}: {reason}")
    for rel, reason in fails:
        print(f"  [FAIL] {rel}: {reason}", file=sys.stderr)

    if fails:
        print(f"泄漏护栏失败：{len(fails)} 处未忽略泄漏。请清理或补全 .gitignore。",
              file=sys.stderr)
        return 1
    if warns:
        print(f"泄漏护栏通过（含 {len(warns)} 项残留 WARN，建议清理）：无未忽略泄漏。")
        return 0
    print("泄漏护栏通过：根目录无构建/覆盖率临时产物泄漏。OK")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=ROOT, help="仓库根目录（默认自动推断）")
    args = ap.parse_args()
    return run(args.root)


if __name__ == "__main__":
    sys.exit(main())
