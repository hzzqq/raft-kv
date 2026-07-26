#!/usr/bin/env python3
"""Go 测试覆盖率门槛门禁（免 Go 工具链，守护「覆盖率不回退」）。

本仓库 CI 的 `coverage` job 会跑 `go test -coverprofile=cover.out` 并把产物上传为
artifact——但**此前只打印一行 summary，从不强制下限**。这意味着一次把整体覆盖率
从 45% 静默砍到 20% 的改动仍能绿过 CI（R2 隐性可观测缺口：有数据无护栏）。

本脚本在「不编译」前提下解析 go 的覆盖率 profile（`cover.out`，格式见
`go tool cover`），计算：
  - 各包（package）覆盖率
  - 整体（total）覆盖率
并对照 `scripts/coverage.config.json` 中配置的门槛：
  - min_total    整体覆盖率下限（低于即 FAIL，非零退出）
  - min_package  单包覆盖率下限（低于即 FAIL；默认 0 表示仅提示不阻断）

纯函数 `parse_profile` / `summarize` / `coverage_pct` 已抽离，供 `scripts/tests/
test_check_go_coverage.py` fixture 驱动回归。

注意：本门禁依赖「已生成的 cover.out」这一 Go 产物，因此**不接入** check_all.py /
docs-links 这类「永远免 Go」的常驻门禁（避免无 go/无产物环境下误 FAIL），而是挂在
CI `coverage` job 与 `make cover` 之后运行。

用法：
  python3 scripts/check_go_coverage.py --profile cover.out
  python3 scripts/check_go_coverage.py --profile cover.out --config scripts/coverage.config.json
  python3 scripts/check_go_coverage.py --profile cover.out --json out.json
  python3 scripts/check_go_coverage.py --profile cover.out --update-baseline   # 以当前实测值刷新门槛

退出码：0=达到门槛；1=低于门槛（应阻断）；2=profile 缺失/解析失败。
"""
import argparse
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_PROFILE = os.path.join(ROOT, "cover.out")
DEFAULT_CONFIG = os.path.join(ROOT, "scripts", "coverage.config.json")
MODULE_PREFIX = "raftkv/"

_BLOCK_RE = re.compile(
    r"^(?P<path>[^:]+):(?P<sl>\d+)\.(?P<sc>\d+),(?P<el>\d+)\.(?P<ec>\d+)\s+"
    r"(?P<stmts>\d+)\s+(?P<count>-?\d+)\s*$"
)


def parse_profile(text: str) -> list:
    """解析 go 覆盖率 profile 文本，返回 block 列表。

    每个 block 为 dict：{pkg, file, stmts, count}。profile 首行 `mode: xxx` 被跳过。
    count>0 表示该语句块被覆盖。
    """
    blocks = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("mode:"):
            continue
        m = _BLOCK_RE.match(line)
        if not m:
            # 非 block 行（如注释/空）忽略
            continue
        path = m.group("path")
        stmts = int(m.group("stmts"))
        count = int(m.group("count"))
        pkg = _pkg_of(path)
        blocks.append({"pkg": pkg, "file": path, "stmts": stmts, "count": count})
    return blocks


def _pkg_of(path: str) -> str:
    """由 profile 中的文件路径推断 Go 包路径（去掉模块前缀）。

    `raftkv/src/metrics/metrics.go` -> `src/metrics`。
    """
    d = os.path.dirname(path)
    if d.startswith(MODULE_PREFIX):
        d = d[len(MODULE_PREFIX):]
    return d or "(root)"


def summarize(blocks: list) -> dict:
    """把 block 列表聚合成 {pkg: (covered_stmts, total_stmts)} 与整体统计。

    返回 dict：
      packages: {pkg: {"covered": int, "total": int}}
      total:    {"covered": int, "total": int}
    """
    packages = {}
    tot_cov = tot_stmt = 0
    for b in blocks:
        covered = b["stmts"] if b["count"] > 0 else 0
        tot_cov += covered
        tot_stmt += b["stmts"]
        agg = packages.setdefault(b["pkg"], {"covered": 0, "total": 0})
        agg["covered"] += covered
        agg["total"] += b["stmts"]
    return {"packages": packages, "total": {"covered": tot_cov, "total": tot_stmt}}


def coverage_pct(covered: int, total: int) -> float:
    """覆盖率百分比；total==0 视为 100.0（无语句可覆盖）。"""
    if total <= 0:
        return 100.0
    return round(covered * 100.0 / total, 2)


def load_config(path: str) -> dict:
    """读取门槛配置；缺失则用内置默认（report-only，min_total=0）。"""
    if not os.path.isfile(path):
        return {"min_total": 0.0, "min_package": 0.0, "packages": {}}
    try:
        cfg = json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return {"min_total": 0.0, "min_package": 0.0, "packages": {}}
    cfg.setdefault("min_total", 0.0)
    cfg.setdefault("min_package", 0.0)
    cfg.setdefault("packages", {})
    return cfg


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default=DEFAULT_PROFILE, help="go 覆盖率 profile 路径（默认 cover.out）")
    ap.add_argument("--config", default=DEFAULT_CONFIG, help="门槛配置文件（默认 scripts/coverage.config.json）")
    ap.add_argument("--json", dest="json_out", help="将结果写入 JSON")
    ap.add_argument("--update-baseline", action="store_true",
                    help="以当前实测整体覆盖率刷新 config.min_total 并写回")
    args = ap.parse_args(argv)

    if not os.path.isfile(args.profile):
        print(f"[FAIL] 覆盖率 profile 不存在：{args.profile}\n"
              f"       请先运行 `go test ./... -coverprofile=cover.out` 生成。",
              file=sys.stderr)
        return 2

    text = open(args.profile, encoding="utf-8", errors="ignore").read()
    blocks = parse_profile(text)
    if not blocks:
        print(f"[FAIL] profile 未解析出任何覆盖率 block：{args.profile}", file=sys.stderr)
        return 2

    summ = summarize(blocks)
    cfg = load_config(args.config)
    total_pct = coverage_pct(summ["total"]["covered"], summ["total"]["total"])

    # 按覆盖率升序排，便于一眼看到最薄弱的包
    pkg_rows = []
    for pkg, agg in summ["packages"].items():
        pct = coverage_pct(agg["covered"], agg["total"])
        floor = cfg["packages"].get(pkg, cfg["min_package"])
        pkg_rows.append((pkg, pct, agg["covered"], agg["total"], floor))
    pkg_rows.sort(key=lambda r: r[1])

    # 输出表格
    print(f"Go 测试覆盖率（profile={os.path.relpath(args.profile, ROOT)}）：")
    print(f"  {'PACKAGE':<28} {'COVER':>7} {'STMTS':>10}  最低门槛")
    for pkg, pct, cov, tot, floor in pkg_rows:
        print(f"  {pkg:<28} {pct:>6.2f}% {cov:>5}/{tot:<5}  {floor:>6.2f}%")
    print(f"  {'—'*52}")
    print(f"  {'TOTAL':<28} {total_pct:>6.2f}% "
          f"{summ['total']['covered']:>5}/{summ['total']['total']:<5}  "
          f"min_total={cfg['min_total']:.2f}%")

    if args.json_out:
        json.dump({
            "total_pct": total_pct,
            "total": summ["total"],
            "packages": {p: {"pct": c, "covered": a["covered"], "total": a["total"],
                             "floor": cfg["packages"].get(p, cfg["min_package"])}
                        for p, c, _, _, _ in
                        [(r[0], r[1], r[2], r[3], r[4]) for r in pkg_rows]},
            "min_total": cfg["min_total"],
            "min_package": cfg["min_package"],
        }, open(args.json_out, "w"), ensure_ascii=False, indent=2)

    if args.update_baseline:
        cfg["min_total"] = total_pct
        json.dump(cfg, open(args.config, "w"), ensure_ascii=False, indent=2)
        print(f"[OK] 已用实测值刷新门槛：min_total <- {total_pct:.2f}%")

    # 门槛判定
    failed = []
    if total_pct < cfg["min_total"]:
        failed.append(f"整体覆盖率 {total_pct:.2f}% < 门槛 {cfg['min_total']:.2f}%")
    for pkg, pct, _, _, floor in pkg_rows:
        if floor and pct < floor:
            failed.append(f"包 {pkg} 覆盖率 {pct:.2f}% < 门槛 {floor:.2f}%")

    if failed:
        print("[FAIL] 覆盖率未达门槛：", file=sys.stderr)
        for f in failed:
            print(f"  - {f}", file=sys.stderr)
        return 1

    print("[PASS] 覆盖率达到门槛。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
