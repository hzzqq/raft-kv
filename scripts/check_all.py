#!/usr/bin/env python3
"""统一编排仓库内全部「免 Go 工具链」静态自检脚本（CI `docs-links` job 的本地单入口）。

本仓库在自驱迭代中沉淀了一批纯 Python 的文档/代码一致性校验器（不依赖 Go 工具链，
可在无 go 环境复跑）。本脚本把它们串成一条流水线，给出统一通过与失败汇总，避免
开发者逐个手动调用、也避免各校验逻辑在多处重复。

用法：
    python3 scripts/check_all.py            # 依次运行全部校验，汇总结果
    python3 scripts/check_all.py --quiet    # 仅打印每条的最终 PASS/FAIL 与总结

任一校验硬失败即整体返回非 0，可直接作为 pre-commit / CI 门禁。
"""
import argparse
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# (脚本, 参数列表, 描述)
CHECKS = [
    ("scripts/check_md_links.py", ["."], "Markdown 内部链接一致性"),
    ("scripts/check_docs_endpoints.py", [], "网关端点/CLI 与文档一致性"),
    ("scripts/check_metrics_docs.py", [], "指标注册名与文档一致性"),
    ("scripts/check_api_docs.py", [], "kvcli.Client/util 公共 API 与文档一致性"),
    ("scripts/gen_changelog.py", ["--verify"], "CHANGELOG.md 与迭代日志同步"),
    ("scripts/check_state_integrity.py", [], "自驱开发日志完整性"),
    ("scripts/check_doc_inventory.py", [], "校验器套件接线一致性"),
    ("scripts/check_coverage_doc.py", [], "coverage.md 与校验器清单一致"),
    ("scripts/check_go_patterns.py", [], "Go 反模式静态扫描"),
    ("scripts/check_godoc.py", [], "godoc 导出标识符文档覆盖率"),
]


def main() -> int:
    quiet = _args().quiet
    results = []
    for rel, args, desc in CHECKS:
        path = os.path.join(ROOT, rel)
        cmd = [sys.executable, path] + args
        if not quiet:
            print(f"==> [{desc}] {rel} {' '.join(args)}")
        try:
            proc = subprocess.run(cmd, cwd=ROOT, capture_output=not quiet, text=True)
        except FileNotFoundError as e:
            print(f"    缺失: {e}", file=sys.stderr)
            results.append((rel, desc, False, "文件缺失"))
            continue
        ok = proc.returncode == 0
        if not quiet and not ok and proc.stdout:
            # 仅打印失败摘要的最后若干行，避免刷屏
            tail = proc.stdout.strip().splitlines()[-8:]
            print("    " + "\n    ".join(tail))
        results.append((rel, desc, ok, f"exit={proc.returncode}"))

    print("\n" + "=" * 56)
    print("自检汇总：")
    failed = 0
    for rel, desc, ok, info in results:
        mark = "PASS" if ok else "FAIL"
        if not ok:
            failed += 1
        print(f"  [{mark}] {desc}  ({rel}, {info})")
    print("=" * 56)
    if failed:
        print(f"整体失败：{failed}/{len(results)} 项未通过。")
        return 1
    print(f"全部通过：{len(results)}/{len(results)} 项。OK")
    return 0


def _args():
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--quiet", action="store_true")
    return ap.parse_args()


if __name__ == "__main__":
    sys.exit(main())
