#!/usr/bin/env python3
"""check_go_patterns.py 的回归测试（免 Go，守护「Go 反模式门禁自身」不退化）。

断言：
  - CRIT_IOUTIL / CRIT_TIME_AFTER / WARN_TODO 正则正确识别
  - strip_comment 正确剔除 // 行注释且不误伤字符串内 //
  - main() 在本仓库（已根治 ioutil/非测试 time.After）返回 0
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check_go_patterns as gp  # noqa: E402


def test_ioutil_blocked():
    assert gp.CRIT_IOUTIL.search("ioutil.ReadFile(x)")


def test_time_after_blocked():
    assert gp.CRIT_TIME_AFTER.search("time.After(3 * time.Second)")


def test_todo_warn():
    assert gp.WARN_TODO.search("// TODO: clean up")


def test_strip_comment_basic():
    assert gp.strip_comment("a := 1 // comment") == "a := 1 "


def test_strip_comment_keeps_string_url():
    # 字符串内的 // 不应被当注释剔除（避免误伤 http:// 等）
    out = gp.strip_comment('s := "http://x" // real')
    assert out == 's := "http://x" '


def test_main_passes_on_repo():
    assert gp.main() == 0


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
