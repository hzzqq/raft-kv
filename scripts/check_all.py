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
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# (脚本, 参数列表, 描述, warn_only)
# warn_only=True 表示失败仅记为 WARN（不计入整体 FAIL，不阻断 CI）。
CHECKS = [
    ("scripts/check_md_links.py", ["."], "Markdown 内部链接一致性", False),
    ("scripts/check_docs_endpoints.py", [], "网关端点/CLI 与文档一致性", False),
    ("scripts/check_metrics_docs.py", [], "指标注册名与文档一致性", False),
    ("scripts/check_api_docs.py", [], "kvcli.Client/util 公共 API 与文档一致性", False),
    ("scripts/gen_changelog.py", ["--verify"], "CHANGELOG.md 与迭代日志同步", False),
    ("scripts/check_state_integrity.py", [], "自驱开发日志完整性", False),
    ("scripts/check_doc_inventory.py", [], "校验器套件接线一致性", False),
    ("scripts/check_coverage_doc.py", [], "coverage.md 与校验器清单一致", False),
    ("scripts/check_go_patterns.py", [], "Go 反模式静态扫描", False),
    ("scripts/check_godoc.py", [], "godoc 导出标识符文档覆盖率", False),
    ("scripts/check_test_coverage.py", [], "测试覆盖率缺口探测(软提示)", True),
    ("scripts/check_hooks_installed.py", [], "提交门禁钩子安装状态(软提示)", True),
    ("scripts/check_secrets.py", [], "密钥/凭证泄露扫描(硬阻断)", False),
    ("scripts/gen_test_coverage.py", ["--verify"], "coverage.md 模块↔测试表与实际一致", False),
    ("scripts/check_leaked_artifacts.py", [], "构建/覆盖率临时产物泄漏护栏", False),
]


def _run_one(rel, args, desc, warn_only, quiet, capture):
    path = os.path.join(ROOT, rel)
    cmd = [sys.executable, path] + args
    if not quiet:
        print(f"==> [{desc}] {rel} {' '.join(args)}")
    try:
        proc = subprocess.run(cmd, cwd=ROOT, capture_output=capture, text=True)
    except FileNotFoundError as e:
        print(f"    缺失: {e}", file=sys.stderr)
        return {"name": rel, "desc": desc, "status": "fail",
                "rc": 1, "warn_only": warn_only}
    ok = proc.returncode == 0
    if not quiet and not ok:
        if proc.stdout:
            tail = proc.stdout.strip().splitlines()[-8:]
            print("    " + "\n    ".join(tail))
        if proc.stderr:
            tail = proc.stderr.strip().splitlines()[-8:]
            print("    [stderr] " + "\n    [stderr] ".join(tail))
    if ok:
        status = "pass"
    elif warn_only:
        status = "warn"
    else:
        status = "fail"
    return {"name": rel, "desc": desc, "status": status,
            "rc": proc.returncode, "warn_only": warn_only}


def _collect(quiet, capture):
    return [_run_one(rel, args, desc, warn_only, quiet, capture)
            for (rel, args, desc, warn_only) in CHECKS]


def _summarize(results):
    passed = sum(1 for r in results if r["status"] == "pass")
    warned = sum(1 for r in results if r["status"] == "warn")
    failed = sum(1 for r in results if r["status"] == "fail")
    return {"total": len(results), "passed": passed,
            "warned": warned, "failed": failed, "ok": failed == 0}


def _render_text(results, quiet):
    print("\n" + "=" * 56)
    print("自检汇总：")
    for r in results:
        mark = r["status"].upper()
        print(f"  [{mark}] {r['desc']}  ({r['name']}, exit={r['rc']})")
    print("=" * 56)
    s = _summarize(results)
    if s["failed"]:
        print(f"整体失败：{s['failed']}/{s['total']} 项未通过。")
        return 1
    if s["warned"]:
        print(f"通过（含 {s['warned']} 项 WARN 软提示）："
              f"{s['total'] - s['warned']}/{s['total']} 硬门禁通过。")
        return 0
    print(f"全部通过：{s['total']}/{s['total']} 项。OK")
    return 0


def _render_json(results):
    out = {"summary": _summarize(results), "checks": results}
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if out["summary"]["ok"] else 1


def main() -> int:
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--json", action="store_true",
                    help="输出机器可读 JSON 报告（供 CI / 看板消费）")
    args = ap.parse_args()
    # JSON 模式下不打印逐条进度，且强制捕获子进程 stdout，保持 stdout 为纯 JSON
    quiet = args.quiet or args.json
    capture = (not args.quiet) or args.json
    results = _collect(quiet, capture)
    if args.json:
        return _render_json(results)
    return _render_text(results, args.quiet)


if __name__ == "__main__":
    sys.exit(main())
