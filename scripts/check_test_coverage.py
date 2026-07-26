#!/usr/bin/env python3
"""test-coverage 缺口探测（免 Go 工具链）。

本仓库在自驱迭代中沉淀了 120+ 轮功能/可观测性增强，但缺少一条「测试纪律」护栏：
新加的源码文件 / 导出的公共 API 若未被任何 *_test.go 引用，覆盖率会悄悄下降而无人察觉。

本脚本在「不编译」的前提下做两件事：
  1. 包级缺口：扫描 src 下每个含 .go 源码的目录，标记完全没有 _test.go 的包（硬缺口）。
  2. 符号级提示：在每个已有测试的包内，找出「定义但未在其包测试中被引用」的导出函数/方法/类型，
     作为「可能未被直接测试」的软提示（WARN，可能含被间接覆盖的结果结构体，属正常误报）。

输出为信息性报告（默认退出码 0，不阻断 CI），可配合 --json 产出机器可读统计供
scripts/gen_test_coverage.py 消费，生成 docs/coverage.md 的模块↔测试映射表并做漂移校验。

用法：
    python3 scripts/check_test_coverage.py            # 打印报告
    python3 scripts/check_test_coverage.py --json s.json
    python3 scripts/check_test_coverage.py --strict   # 把硬缺口(hard)升级为非零退出(未来 CI 门禁)

退出码：0=通过/仅提示；2=--strict 下存在硬缺口（包无测试）。
"""
import argparse
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")

# 顶层声明（导出符号）匹配
FUNC_RE = re.compile(r"^func\s+(?:\([^)]*\)\s+)?([A-Z]\w*)")
TYPE_RE = re.compile(r"^type\s+([A-Z]\w*)")
VAR_RE = re.compile(r"^var\s+([A-Z]\w*)")
CONST_RE = re.compile(r"^const\s+([A-Z]\w*)")

# 这些后缀的结果/错误/视图类型通常通过字段或构造器间接覆盖，直接命名单测较少，
# 列为「低信号」提示，但仍上报，便于人工巡检。
LOW_SIGNAL_SUFFIX = (
    "Result", "Results", "Error", "Err", "Stats", "Status", "StatusView",
    "View", "Config", "Plan", "Step", "Delta", "Conn", "Codec", "State",
)


def _read(p: str) -> str:
    with open(p, encoding="utf-8", errors="ignore") as f:
        return f.read()


def package_dirs(root=SRC):
    found = []
    for r, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d != ".git"]
        go = [f for f in files if f.endswith(".go") and not f.endswith("_test.go")]
        te = [f for f in files if f.endswith("_test.go")]
        if go:  # 仅统计有源码的目录（即一个 Go 包）
            found.append((r, go, te))
    return found


def exported_symbols(src_blob: str):
    funcs, types, vars_, consts = set(), set(), set(), set()
    for line in src_blob.splitlines():
        s = line.strip()
        m = FUNC_RE.match(s)
        if m:
            funcs.add(m.group(1))
            continue
        m = TYPE_RE.match(s)
        if m:
            types.add(m.group(1))
            continue
        m = VAR_RE.match(s)
        if m:
            vars_.add(m.group(1))
            continue
        m = CONST_RE.match(s)
        if m:
            consts.add(m.group(1))
    return funcs, types, vars_, consts


def analyze(strict: bool = False, root=SRC):
    pkgs = package_dirs(root)
    report = {"packages": [], "summary": {}}
    hard_gaps = 0
    warn_syms = 0
    for pkgdir, go, te in sorted(pkgs):
        try:
            rel = os.path.relpath(pkgdir, ROOT).replace(os.sep, "/")
        except ValueError:
            # 扫描根与仓库根不在同一挂载点（如 fixture 测试目录）时回退到扫描根相对路径
            rel = os.path.relpath(pkgdir, root).replace(os.sep, "/")
        src_blob = "\n".join(_read(os.path.join(pkgdir, f)) for f in go)
        test_blob = "\n".join(_read(os.path.join(pkgdir, f)) for f in te) if te else ""
        funcs, types, vars_, consts = exported_symbols(src_blob)
        all_syms = funcs | types | vars_ | consts

        unref_funcs = sorted(f for f in funcs if f not in test_blob)
        unref_types = sorted(t for t in types if t not in test_blob)
        unref_vars = sorted(v for v in vars_ if v not in test_blob)
        unref_consts = sorted(c for c in consts if c not in test_blob)

        low_signal = [s for s in unref_types if s.endswith(LOW_SIGNAL_SUFFIX)]
        high_signal = (
            unref_funcs + unref_vars + unref_consts
            + [s for s in unref_types if s not in low_signal]
        )
        warn_syms += len(high_signal)

        has_test = bool(te)
        if not has_test:
            hard_gaps += 1
        report["packages"].append({
            "pkg": rel,
            "has_test": has_test,
            "test_files": len(te),
            "src_files": len(go),
            "exported": len(all_syms),
            "unref_funcs": unref_funcs,
            "unref_types": unref_types,
            "unref_vars": unref_vars,
            "unref_consts": unref_consts,
        })

    report["summary"] = {
        "packages": len(pkgs),
        "packages_without_test": hard_gaps,
        "unref_funcs_total": sum(len(p["unref_funcs"]) for p in report["packages"]),
        "unref_types_total": sum(len(p["unref_types"]) for p in report["packages"]),
        "unref_vars_total": sum(len(p["unref_vars"]) for p in report["packages"]),
        "unref_consts_total": sum(len(p["unref_consts"]) for p in report["packages"]),
        "high_signal_untested": warn_syms,
    }
    return report, hard_gaps


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--json", metavar="FILE", help="写出机器可读统计到 JSON")
    ap.add_argument("--strict", action="store_true",
                    help="把『包无测试』硬缺口升级为非零退出(未来 CI 门禁)")
    ap.add_argument("--root", default=SRC,
                    help="扫描根目录(默认仓库 src，测试可重定向到 fixture)")
    args = ap.parse_args(argv)

    report, hard_gaps = analyze(args.strict, args.root)

    # 打印人类可读报告
    print(f"==> [test-coverage] 扫描 src 下 {report['summary']['packages']} 个包")
    for p in report["packages"]:
        if not p["has_test"]:
            print(f"  [HARD] {p['pkg']} :: 无任何 _test.go（硬缺口）")
            continue
        if p["unref_funcs"] or p["unref_types"] or p["unref_vars"] or p["unref_consts"]:
            parts = []
            if p["unref_funcs"]:
                parts.append("funcs=" + ",".join(p["unref_funcs"]))
            if p["unref_types"]:
                parts.append("types=" + ",".join(p["unref_types"]))
            if p["unref_vars"]:
                parts.append("vars=" + ",".join(p["unref_vars"]))
            if p["unref_consts"]:
                parts.append("consts=" + ",".join(p["unref_consts"]))
            print(f"  [WARN] {p['pkg']} :: " + " ; ".join(parts))
    s = report["summary"]
    print("=" * 56)
    print(f"  包总数={s['packages']}  无测试包(HARD)={s['packages_without_test']}")
    print(f"  未引用导出: func={s['unref_funcs_total']} type={s['unref_types_total']} "
          f"var={s['unref_vars_total']} const={s['unref_consts_total']}")
    print(f"  高信号(建议补测)未覆盖符号数={s['high_signal_untested']}")
    print("=" * 56)
    print("说明: WARN 为软提示,可能含被间接覆盖的结果/视图类型;不阻断 CI。")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"已写出 JSON 统计: {args.json}")

    if args.strict and hard_gaps:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
