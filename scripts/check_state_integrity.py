#!/usr/bin/env python3
"""校验自驱开发日志（.workbuddy/self-driving/state.json）的完整性。

自驱迭代把每一轮交付记进 state.json 的 `log` 数组，CHANGELOG.md 与各类审计
均由其派生。一旦 `log` 被重复注入（如某轮被重放后再次 append），审计链即被
悄悄污染：CHANGELOG 出现重复条目、`gen_changelog.py --verify` 失配、CI 的
docs-links job 在无人察觉下失败。

本脚本是「免 Go」的纯静态门禁，用于捕获此类数据完整性缺陷：
  - 重复的 (task_id, cycle) 对（审计链被污染的最直接信号）
  - cycle 字段非整型 / 越界（<0 或 > state.cycle）
  - score 字段越界（不在 0–100）
  - 必填字段（task_id / cycle）缺失
  - log 整体非单调（按出现顺序 cycle 不应回退，除非该轮被回滚）

用法：
  python3 scripts/check_state_integrity.py
返回 0 = 通过；非 0 = 发现完整性缺陷（建议先 `python3 scripts/gen_changelog.py`）。
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE = os.path.join(ROOT, ".workbuddy", "self-driving", "state.json")


def err(msg):
    print(f"  [缺陷] {msg}", file=sys.stderr)


def main() -> int:
    if not os.path.isfile(STATE):
        print("state.json 不存在，跳过（非缺陷）。")
        return 0
    try:
        s = json.load(open(STATE, encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        print(f"state.json 解析失败：{e}", file=sys.stderr)
        return 1

    log = s.get("log")
    if not isinstance(log, list):
        print("state.json `log` 字段缺失或非数组。", file=sys.stderr)
        return 1

    top_cycle = s.get("cycle", 0)
    problems = 0

    seen = {}            # (task_id, cycle) -> first index
    prev_cycle = -1
    for i, e in enumerate(log):
        if not isinstance(e, dict):
            err(f"log[{i}] 不是对象。")
            problems += 1
            continue

        tid = e.get("task_id", "<none>")
        cyc = e.get("cycle")

        # 必填字段
        if "task_id" not in e:
            err(f"log[{i}] 缺少 task_id（cycle={cyc}）。")
            problems += 1
        if "cycle" not in e:
            err(f"log[{i}] 缺少 cycle（task_id={tid}）。")
            problems += 1
        else:
            if not isinstance(cyc, int) or isinstance(cyc, bool):
                err(f"log[{i}] cycle 非整型：{cyc!r}（task_id={tid}）。")
                problems += 1
            elif cyc < 0 or cyc > top_cycle:
                err(f"log[{i}] cycle={cyc} 越界（0..{top_cycle}）（task_id={tid}）。")
                problems += 1

        # 重复 (task_id, cycle) 对
        key = (tid, cyc)
        if cyc is not None and "task_id" in e:
            if key in seen:
                err(f"log[{i}] 与 log[{seen[key]}] 重复 (task_id={tid}, cycle={cyc})"
                     f" —— 审计链已被污染。")
                problems += 1
            else:
                seen[key] = i

        # score 越界
        sc = e.get("score")
        if sc is not None and (not isinstance(sc, (int, float)) or isinstance(sc, bool)
                              or sc < 0 or sc > 100):
            err(f"log[{i}] score={sc} 越界（0..100）（task_id={tid}, cycle={cyc}）。")
            problems += 1

        # 单调（回退）
        if isinstance(cyc, int) and not isinstance(cyc, bool):
            if cyc < prev_cycle:
                err(f"log[{i}] cycle={cyc} 小于前一条 {prev_cycle}（task_id={tid}）"
                     f" —— 除非该轮被回滚，否则异常。")
                problems += 1
            prev_cycle = cyc

    if problems:
        print(f"state.json 完整性校验失败：{problems} 处缺陷。", file=sys.stderr)
        return 1
    print(f"state.json 完整性 OK（log 共 {len(log)} 条，cycle 峰值 {top_cycle}）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
