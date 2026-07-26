#!/usr/bin/env python3
"""check_all.py 的回归测试（免 Go，守护「编排器自身」不退化）。

断言：
  - _summarize 正确聚合 pass/warn/fail 与 ok 标志
  - _render_json 输出合法 JSON，且失败项令退出码非 0、全部通过则为 0
"""
import io
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check_all as ca  # noqa: E402


def _fake(status):
    return {"name": "x", "desc": "d", "status": status, "rc": 0, "warn_only": False}


def test_summarize():
    rs = [_fake("pass"), _fake("warn"), _fake("fail")]
    s = ca._summarize(rs)
    assert s == {"total": 3, "passed": 1, "warned": 1,
                 "failed": 1, "ok": False}


def test_summarize_ok():
    rs = [_fake("pass"), _fake("pass")]
    assert ca._summarize(rs)["ok"] is True


def test_render_json_ok():
    rs = [_fake("pass")]
    buf, old = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        rc = ca._render_json(rs)
    finally:
        sys.stdout = old
    obj = json.loads(buf.getvalue())
    assert obj["summary"]["ok"] is True
    assert rc == 0


def test_render_json_fail_exit():
    rs = [_fake("fail")]
    buf, old = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        rc = ca._render_json(rs)
    finally:
        sys.stdout = old
    assert rc == 1


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
