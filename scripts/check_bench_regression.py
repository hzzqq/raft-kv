#!/usr/bin/env python3
"""Go 基准回归门禁（免 Go 工具链，守护「性能不静默回退」）。

cycle 116 已为 metrics/util/transport 热点路径建立 9 个基准（go test -bench），
但**此前无门禁**：某次改动把 WithLabelValues 从 25ns 拉回 200ns 也能绿过 CI
（R2 隐性性能悬崖：有基线无护栏）。

本脚本在「不编译」前提下解析 `go test -bench` 的文本输出（格式见 `go test -h`），
与本仓提交的 `scripts/bench-baseline.json` 对比，若任一基准超过阈值（默认 10%）
即失败。纯函数 `parse_bench` / `compare` 供 `scripts/tests/test_check_bench_regression.py`
fixture 驱动回归。

依赖「已生成的 bench 输出」这一 Go 产物，故**不接入** `check_all` / `docs-links`
常驻门禁（与 `check_go_coverage` 同策略），挂在 CI `bench` job 与 `make bench-check`。

用法：
  go test -run='^$' -bench=. -benchtime=1x ./src/... > bench.out 2>&1
  python3 scripts/check_bench_regression.py --bench bench.out --baseline scripts/bench-baseline.json
  python3 scripts/check_bench_regression.py --bench bench.out --baseline scripts/bench-baseline.json --threshold 0.15
  python3 scripts/check_bench_regression.py --bench bench.out --baseline scripts/bench-baseline.json --update-baseline
退出码：0=无回退(或无可比基线，仅报告)；1=存在回退；2=输入缺失/解析失败。
"""
import argparse
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_BENCH = os.path.join(ROOT, "bench.out")
DEFAULT_BASELINE = os.path.join(ROOT, "scripts", "bench-baseline.json")

# go test -bench 输出行示例：
#   BenchmarkWithLabelValues-8   	1000000	      25.3 ns/op	       0 B/op	       1 allocs/op
_BENCH_RE = re.compile(
    r"^Benchmark(?P<name>\w+)-(?P<procs>\d+)\s+"
    r"(?P<n>\d+)\s+"
    r"(?P<nsop>[\d.]+)\s*ns/op"
    r"(?:\s+(?P<bop>[\d.]+)\s*B/op)?"
    r"(?:\s+(?P<allocs>[\d.]+)\s*allocs/op)?"
)


def parse_bench(text: str) -> dict:
    """解析 `go test -bench` 输出，返回 {基准名: {nsop,bop,allocs,procs,n}}。

    非基准行（goos/goarch/ok/PASS/summary）被忽略。
    """
    rows = {}
    for line in text.splitlines():
        line = line.strip()
        m = _BENCH_RE.match(line)
        if not m:
            continue
        name = m.group("name")
        rows[name] = {
            "nsop": float(m.group("nsop")),
            "bop": float(m.group("bop")) if m.group("bop") else None,
            "allocs": float(m.group("allocs")) if m.group("allocs") else None,
            "procs": int(m.group("procs")),
            "n": int(m.group("n")),
        }
    return rows


def compare(baseline: dict, current: dict, threshold: float = 0.10) -> list:
    """比对当前基准与基线，返回回退列表（按回退幅度降序）。

    仅比对「基线中存在的基准」；新增基准（基线无对应项）不视为回退，仅报告。
    ratio = (cur - base) / base，严格大于 threshold 才判定为回退。
    """
    regressions = []
    for name, cur in current.items():
        if name not in baseline:
            continue  # 新基准，无历史可比，仅报告
        base = baseline[name] or {}
        b_ns = base.get("nsop")
        if not b_ns or b_ns <= 0:
            continue
        ratio = (cur["nsop"] - b_ns) / b_ns
        if ratio > threshold:
            regressions.append({
                "name": name,
                "baseline_nsop": b_ns,
                "current_nsop": cur["nsop"],
                "ratio": round(ratio, 4),
                "threshold": threshold,
            })
    regressions.sort(key=lambda r: -r["ratio"])
    return regressions


def load_baseline(path: str) -> dict:
    """读取基线 JSON；缺失/损坏则视为空基线。"""
    if not os.path.isfile(path):
        return {}
    try:
        d = json.load(open(path, encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return d if isinstance(d, dict) else {}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bench", default=DEFAULT_BENCH, help="go test -bench 输出路径（默认 bench.out）")
    ap.add_argument("--baseline", default=DEFAULT_BASELINE, help="基线 JSON（默认 scripts/bench-baseline.json）")
    ap.add_argument("--threshold", type=float, default=0.10, help="回退判定阈值（默认 10%%）")
    ap.add_argument("--json", dest="json_out", help="将比对结果写入 JSON")
    ap.add_argument("--update-baseline", action="store_true",
                    help="用当前实测值刷新基线（合并，保留基线中未比对的条目）")
    args = ap.parse_args(argv)

    if not os.path.isfile(args.bench):
        print(f"[FAIL] 基准输出不存在：{args.bench}\n"
              f"       请先运行 `go test -run='^$' -bench=. -benchtime=1x ./... > bench.out`",
              file=sys.stderr)
        return 2

    text = open(args.bench, encoding="utf-8", errors="ignore").read()
    current = parse_bench(text)
    if not current:
        print(f"[FAIL] 未从 {args.bench} 解析出任何 Benchmark 行", file=sys.stderr)
        return 2

    baseline = load_baseline(args.baseline)
    print(f"基准回归比对（bench={os.path.relpath(args.bench, ROOT)}, "
          f"baseline={os.path.relpath(args.baseline, ROOT)}, threshold={args.threshold:.0%}）：")
    print(f"  {'BENCH':<28} {'BASE(ns)':>14} {'CUR(ns)':>14} {'Δ':>8}")
    for name in sorted(current):
        cur = current[name]
        base = baseline.get(name)
        if base and base.get("nsop"):
            b = base["nsop"]
            c = cur["nsop"]
            delta = (c - b) / b if b else 0
            print(f"  {name:<28} {b:>14.1f} {c:>14.1f} {delta:>+7.1%}")
        else:
            print(f"  {name:<28} {'—':>14} {cur['nsop']:>14.1f} {'new':>8}")

    if args.json_out:
        json.dump({"current": current, "baseline": baseline, "threshold": args.threshold},
                  open(args.json_out, "w"), ensure_ascii=False, indent=2)

    if args.update_baseline:
        merged = dict(baseline)
        merged.update(current)
        json.dump(merged, open(args.baseline, "w"), ensure_ascii=False, indent=2)
        print(f"[OK] 已用实测值刷新基线：{args.baseline}")

    if not baseline:
        print("[PASS] 无基线（bench-baseline.json 为空），仅报告当前基准，不阻断。")
        return 0

    regressions = compare(baseline, current, args.threshold)
    if regressions:
        print("[FAIL] 检测到基准回退：", file=sys.stderr)
        for r in regressions:
            print(f"  - {r['name']}: {r['current_nsop']:.1f}ns vs baseline "
                  f"{r['baseline_nsop']:.1f}ns (Δ{r['ratio']:.1%} > {r['threshold']:.0%})",
                  file=sys.stderr)
        return 1

    print("[PASS] 无基准回退（或全部为新基准，无可比对项）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
