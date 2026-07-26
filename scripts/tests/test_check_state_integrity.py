#!/usr/bin/env python3
"""check_state_integrity.py 的回归测试（免 Go，守护「日志完整性门禁自身」不退化）。

通过 monkeypatch STATE 指向临时 state.json，断言：
  - 重复 (task_id, cycle) 对 → 返回 1
  - cycle 越界 → 返回 1
  - score 越界 → 返回 1
  - 合法日志 → 返回 0
  - 真实 state.json（本仓库）通过 → 返回 0（集成冒烟）
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check_state_integrity as csi  # noqa: E402

REAL_STATE = csi.STATE  # 模块默认指向真实 state.json，供集成测试还原


def _write_state(log, top_cycle=10):
    d = tempfile.mkdtemp()
    p = os.path.join(d, "state.json")
    json.dump({"cycle": top_cycle, "log": log}, open(p, "w"), ensure_ascii=False)
    return p


def test_valid_state_ok():
    p = _write_state([
        {"task_id": "a", "cycle": 1, "score": 10},
        {"task_id": "b", "cycle": 2, "score": 20},
    ], top_cycle=2)
    csi.STATE = p
    assert csi.main() == 0


def test_duplicate_pair_fails():
    p = _write_state([
        {"task_id": "a", "cycle": 1, "score": 10},
        {"task_id": "a", "cycle": 1, "score": 10},
    ], top_cycle=2)
    csi.STATE = p
    assert csi.main() == 1


def test_cycle_out_of_range_fails():
    p = _write_state([
        {"task_id": "a", "cycle": 99, "score": 10},
    ], top_cycle=5)
    csi.STATE = p
    assert csi.main() == 1


def test_score_out_of_range_fails():
    p = _write_state([
        {"task_id": "a", "cycle": 1, "score": 200},
    ], top_cycle=2)
    csi.STATE = p
    assert csi.main() == 1


def test_real_state_ok():
    # 真实 state.json 当前应合法通过
    csi.STATE = REAL_STATE
    assert csi.main() == 0


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"[PASS] {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"[FAIL] {t.__name__}: {e}")
    if failed:
        print(f"\n{len(tests) - failed}/{len(tests)} 通过，{failed} 失败。")
        return 1
    print(f"\n全部 {len(tests)} 例通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
