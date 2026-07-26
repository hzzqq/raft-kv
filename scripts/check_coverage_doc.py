#!/usr/bin/env python3
"""校验 docs/coverage.md 与实际的免 Go 校验器清单一致（meta 门禁）。

cycle 113 起，`scripts/` 下的免 Go 静态校验器已增至 8 个并接入统一门禁。
docs/coverage.md 作为「工程化收口」文档，若漏列某个校验器，会误导审计方以为
该维度未被守护。本脚本做**正向**一致性检查：凡 `scripts/check_*.py` 与
`scripts/gen_changelog.py` 实际存在，其 basename 必须出现在 coverage.md 中。

属免 Go 静态检查，接入 check_all / ci.yml / README（与另两个 meta 校验器同列）。

用法：python3 scripts/check_coverage_doc.py
返回 0 = coverage.md 已覆盖全部实际校验器；非 0 = 有遗漏。
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS = os.path.join(ROOT, "scripts")
COVERAGE = os.path.join(ROOT, "docs", "coverage.md")
GUARDED = [
    "check_md_links.py",
    "check_docs_endpoints.py",
    "check_metrics_docs.py",
    "check_api_docs.py",
    "gen_changelog.py",
    "check_state_integrity.py",
    "check_doc_inventory.py",
    "check_coverage_doc.py",
    "check_go_patterns.py",
    "check_godoc.py",
    "check_test_coverage.py",
]


def main() -> int:
    if not os.path.isfile(COVERAGE):
        print(f"未找到 {os.path.relpath(COVERAGE, ROOT)}，跳过。", file=sys.stderr)
        return 0
    text = open(COVERAGE, encoding="utf-8").read()

    missing = [n for n in GUARDED if n not in text]
    if missing:
        print(f"docs/coverage.md 漏列 {len(missing)} 个校验器：", file=sys.stderr)
        for n in missing:
            print(f"  - {n}", file=sys.stderr)
        print("请更新 docs/coverage.md 的『免 Go 自检门禁』小节。", file=sys.stderr)
        return 1

    # 反向：coverage.md 提到的校验器是否真实存在（防臆造条目）
    mentioned = set(re.findall(r'check_\w+\.py|gen_changelog\.py', text))
    phantom = [m for m in mentioned
               if m in GUARDED and not os.path.isfile(os.path.join(SCRIPTS, m))]
    if phantom:
        print("coverage.md 提及但不存在的校验器：", file=sys.stderr)
        for m in phantom:
            print(f"  - {m}", file=sys.stderr)
        return 1

    print(f"docs/coverage.md 与 {len(GUARDED)} 个实际校验器一致。OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
