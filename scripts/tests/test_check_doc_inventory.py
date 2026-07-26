#!/usr/bin/env python3
"""check_doc_inventory.py 的回归测试（免 Go，守护「meta 门禁自身」不退化）。

通过 monkeypatch README 全局指向临时文件，断言：
  - 真实仓库（受管 14 个校验器全部接线）通过 → 0
  - README 漏列某受管校验器 → 1
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check_doc_inventory as ci  # noqa: E402


def test_ok_on_repo():
    assert ci.main() == 0


def test_missing_readme_wiring_fails():
    d = tempfile.mkdtemp()
    bad = os.path.join(d, "README.md")
    # 故意不含任何受管校验器名，制造接线盲区
    open(bad, "w", encoding="utf-8").write("# 文档\n\n未提及任何 check_* 校验器。\n")
    saved = ci.README
    ci.README = bad
    try:
        assert ci.main() == 1
    finally:
        ci.README = saved


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
