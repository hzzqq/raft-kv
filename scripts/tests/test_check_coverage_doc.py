#!/usr/bin/env python3
"""check_coverage_doc.py 的回归测试（免 Go，守护「meta 门禁自身」不退化）。

通过 monkeypatch COVERAGE 全局指向临时文件，断言：
  - 真实仓库（coverage.md 覆盖全部实际校验器）通过 → 0
  - coverage.md 漏列某受管校验器 → 1
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check_coverage_doc as ccd  # noqa: E402


def test_ok_on_repo():
    assert ccd.main() == 0


def test_missing_coverage_entry_fails():
    d = tempfile.mkdtemp()
    bad = os.path.join(d, "coverage.md")
    # 仅提及一个校验器，制造覆盖盲区
    open(bad, "w", encoding="utf-8").write("see check_md_links.py only.\n")
    saved = ccd.COVERAGE
    ccd.COVERAGE = bad
    try:
        assert ccd.main() == 1
    finally:
        ccd.COVERAGE = saved


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
