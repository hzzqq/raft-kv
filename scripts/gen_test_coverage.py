#!/usr/bin/env python3
"""自动生成/校验 docs/coverage.md 的「模块↔测试」映射表（免 Go 工具链）。

check_test_coverage.py 已能在免编译前提下扫描各 Go 包的测试覆盖情况（包是否有
_test.go、导出符号是否在包测试中被引用）。本脚本消费其 --json 产物，自动渲染一张
「模块 ↔ 测试文件」映射表写回 docs/coverage.md，并用成对标记

    <!-- test-coverage-table:start -->
    ...
    <!-- test-coverage-table:end -->

圈定，使该表**随代码演进自动保持最新**，避免人工维护 drift。

`--verify` 模式仅做一致性比对（只读），若实际覆盖与文档表中嵌入的快照不一致即退出
非 0，可直接作为 CI 门禁（防止 coverage.md 的测试映射章节与真实代码脱节）。

用法：
    python3 scripts/gen_test_coverage.py            # 重新生成 coverage.md 中的映射表
    python3 scripts/gen_test_coverage.py --verify   # 校验文档表与实际一致(不写文件)
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COVERAGE = os.path.join(ROOT, "docs", "coverage.md")
START = "<!-- test-coverage-table:start -->"
END = "<!-- test-coverage-table:end -->"


def collect_json():
    # 临时文件必须落在系统 temp 目录：WorkBuddy 沙箱的 safe-delete 劫持会把
    # os.unlink 改走回收站，且回收站不可用时 fail-closed 抛 OSError；而 shim 对
    # 「系统 temp 下的文件」走原生删除，可彻底规避该偶发门禁失败（cycle 收官复盘）。
    tmp = tempfile.NamedTemporaryFile(
        "w", suffix=".json", dir=tempfile.gettempdir(), delete=False, encoding="utf-8"
    )
    tmp.close()
    try:
        subprocess.run(
            [sys.executable, os.path.join(ROOT, "scripts", "check_test_coverage.py"),
             "--json", tmp.name],
            cwd=ROOT, check=True,
        )
        with open(tmp.name, encoding="utf-8") as f:
            return json.load(f)
    finally:
        # 容错清理：即便仍被劫持或文件已不存在，清理失败也绝不能阻断主流程。
        try:
            os.unlink(tmp.name)
        except OSError:
            pass


def render(report: dict) -> str:
    lines = [
        START,
        "",
        "_本表由 `scripts/check_test_coverage.py` 自动生成（免 Go 扫描），"
        "成对标记之间的内容请勿手工编辑，改代码后运行 `make test-cov` 刷新。_",
        "",
        "| 模块 (包) | 源码文件 | 测试文件 | 有测试 | 高信号未覆盖导出符号 |",
        "|-----------|---------:|--------:|:------:|---------------------:|",
    ]
    for p in report["packages"]:
        name = p["pkg"].replace("src/", "")
        untested = p["unref_funcs"] + p["unref_types"] + p["unref_vars"] + p["unref_consts"]
        if untested:
            shown = ", ".join(untested[:8])
            if len(untested) > 8:
                shown += f" …(+{len(untested) - 8})"
        else:
            shown = "—"
        has = "✅" if p["has_test"] else "❌"
        lines.append(
            f"| `{name}` | {p['src_files']} | {p['test_files']} | {has} | {shown} |"
        )
    s = report["summary"]
    lines.append("")
    lines.append(
        f"> 汇总：{s['packages']} 个包，{s['packages_without_test']} 个无测试；"
        f"未引用导出符号 func={s['unref_funcs_total']} / type={s['unref_types_total']} / "
        f"var={s['unref_vars_total']} / const={s['unref_consts_total']}。"
        f"「未覆盖符号」为软提示，可能含被间接覆盖的结果/视图类型。"
    )
    lines.append("")
    lines.append(END)
    return "\n".join(lines)


def extract_current(text: str):
    s = text.find(START)
    e = text.find(END)
    if s == -1 or e == -1:
        return None
    return text[s:e + len(END)]


def main() -> int:
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--verify", action="store_true", help="仅校验文档表与实际一致，不写文件")
    args = ap.parse_args()

    report = collect_json()
    new_block = render(report)

    if not os.path.isfile(COVERAGE):
        print(f"未找到 {os.path.relpath(COVERAGE, ROOT)}。", file=sys.stderr)
        return 1

    text = open(COVERAGE, encoding="utf-8").read()
    current = extract_current(text)

    if args.verify:
        if current is None:
            print("coverage.md 缺失测试映射表标记，请先运行 gen_test_coverage.py 生成。",
                  file=sys.stderr)
            return 1
        if current.strip() == new_block.strip():
            print("docs/coverage.md 测试映射表与实际一致。OK")
            return 0
        print("docs/coverage.md 测试映射表与实际不一致（drift）：", file=sys.stderr)
        try:
            with open(os.path.join(ROOT, "coverage_drift_debug.txt"), "w", encoding="utf-8") as df:
                df.write("=== CURRENT (from coverage.md) ===\n" + current
                         + "\n\n=== NEW (rendered) ===\n" + new_block + "\n")
        except Exception:
            pass
        print("  运行 `python3 scripts/gen_test_coverage.py` 刷新。", file=sys.stderr)
        return 1

    if current is None:
        # 没有标记则追加到文末
        text = text.rstrip() + "\n\n## 模块 ↔ 测试映射（自动生成）\n\n" + new_block + "\n"
    else:
        text = text[: text.find(START)] + new_block + text[text.find(END) + len(END):]
    with open(COVERAGE, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"已刷新 docs/coverage.md 的测试映射表（{report['summary']['packages']} 个包）。")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        import traceback as _tb
        try:
            with open(os.path.join(ROOT, "gen_test_coverage_trace.txt"), "w", encoding="utf-8") as _f:
                _tb.print_exc(file=_f)
        except Exception:
            pass
        raise
