#!/usr/bin/env python3
"""从 .workbuddy/self-driving/state.json 的迭代日志生成人类可读的 CHANGELOG.md。

state.json 的 log 仅存于隐藏工作目录，对用户不可见。本脚本把它聚合为按模块（area）
分组的变更清单，输出到仓库根 CHANGELOG.md，使「自驱开发的逐轮交付」对使用者透明、
可审计。纯静态处理，不依赖 Go。

用法：
    python3 scripts/gen_changelog.py            # 写 CHANGELOG.md
    python3 scripts/gen_changelog.py --check    # 仅校验(始终返回 0，供 CI 展示)
"""
import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _parse_args():
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--verify", action="store_true")
    return ap.parse_args()
STATE = os.path.join(ROOT, ".workbuddy", "self-driving", "state.json")
OUT = os.path.join(ROOT, "CHANGELOG.md")

AREA_ORDER = [
    "gateway", "kvcli", "util", "raft", "shardkv", "shardmaster",
    "kvraft", "transport", "diagnostics", "metrics", "version",
    "statusfmt", "docs", "scripts", "other",
]


def area_of(path: str) -> str:
    p = path.replace("\\", "/")
    if p.startswith("src/"):
        seg = p.split("/", 2)[1]
        return seg
    if p.startswith("docs/") or p == "README.md":
        return "docs"
    if p.startswith("scripts/"):
        return "scripts"
    return "other"


def main() -> int:
    if not os.path.isfile(STATE):
        print("state.json 不存在，跳过。", file=sys.stderr)
        return 0
    s = json.load(open(STATE, encoding="utf-8"))
    log = s.get("log", [])
    if not log:
        print("log 为空，跳过。")
        return 0

    groups = {a: [] for a in AREA_ORDER}
    for e in log:
        files = e.get("files", []) or ["?"]
        area = area_of(files[0]) if files else "other"
        if area not in groups:
            groups[area] = []
        groups[area].append(e)

    cycles = [e.get("cycle", 0) for e in log]
    cmin, cmax = min(cycles), max(cycles)
    dates = sorted({e.get("ts", "") for e in log if e.get("ts")})

    lines = []
    lines.append("# CHANGELOG（自驱开发迭代交付记录）")
    lines.append("")
    lines.append(f"> 由 `scripts/gen_changelog.py` 从 `.workbuddy/self-driving/state.json` 自动生成。")
    lines.append(f"> 覆盖 cycle {cmin}–{cmax}，共 {len(log)} 轮交付"
                 + (f"；时间跨度 {dates[0]} ~ {dates[-1]}" if len(dates) > 1 else "") + "。")
    lines.append("")
    lines.append("按模块聚合；每条含 `task_id`、新增需求（`new_requirement`）、隐性问题"
                 "（`implicit`）、自评分（`score`）。隐性问题为本轮主动挖掘的非显性缺陷/技术债。")
    lines.append("")

    for area in AREA_ORDER:
        items = groups.get(area)
        if not items:
            continue
        lines.append(f"## {area}")
        lines.append("")
        for e in sorted(items, key=lambda x: x.get("cycle", 0)):
            tid = e.get("task_id", "?")
            cyc = e.get("cycle", "?")
            nr = e.get("new_requirement", "")
            imp = e.get("implicit", "")
            sc = e.get("score", "")
            lines.append(f"- **[{cyc}] `{tid}`** — {nr}（隐性：{imp}；score={sc}）")
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("本文件为生成产物，重新运行 `python3 scripts/gen_changelog.py` 即可刷新。"
                 "如需手工追加说明，请在生成后单独维护，或扩展生成脚本。")
    lines.append("")

    out = "\n".join(lines)
    if _parse_args().check:
        print(out)
        return 0
    if _parse_args().verify:
        if not os.path.isfile(OUT):
            print(f"CHANGELOG.md 不存在，请运行 gen_changelog.py 生成。", file=sys.stderr)
            return 1
        cur = open(OUT, encoding="utf-8").read()
        if cur.strip() == out.strip():
            print("CHANGELOG.md 与 state.json 同步。OK")
            return 0
        print("CHANGELOG.md 与 state.json 不同步，请运行 `python3 scripts/gen_changelog.py` 刷新。",
              file=sys.stderr)
        return 1
    open(OUT, "w", encoding="utf-8").write(out)
    print(f"已生成 {OUT}（{len(log)} 条，覆盖 {len([a for a in AREA_ORDER if groups.get(a)])} 个模块）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
