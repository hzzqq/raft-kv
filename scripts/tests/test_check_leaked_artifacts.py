#!/usr/bin/env python3
"""check_leaked_artifacts.py 的回归测试（免 Go，守护「泄漏护栏自身」不退化）。

通过临时 git 仓库 fixture 断言：
  - 干净仓库（.gitignore 含 covtest_*/ 且无泄漏）→ 0
  - .gitignore 缺 covtest 规则 → 1（根因防护）
  - 未忽略的 covtest_*/cfg.json 泄漏 → 1
  - 已忽略的 covtest_*/cover.out 残留 → WARN，退出 0
"""
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check_leaked_artifacts as cla  # noqa: E402


def _git_init(d: str):
    subprocess.run(["git", "-C", d, "init", "-q"], check=True)
    subprocess.run(["git", "-C", d, "config", "user.email", "t@t"],
                   check=True)
    subprocess.run(["git", "-C", d, "config", "user.name", "t"], check=True)


def _build(gitignore_lines, leaks=None):
    d = tempfile.mkdtemp()
    _git_init(d)
    with open(os.path.join(d, ".gitignore"), "w", encoding="utf-8") as f:
        f.write("\n".join(gitignore_lines) + "\n")
    for rel in (leaks or []):
        full = os.path.join(d, rel)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as f:
            f.write("x")
    return d


def test_clean_ok():
    d = _build(["covtest_*/", "*.out"])
    assert cla.analyze(d) == ([], [])
    assert cla.run(d) == 0


def test_missing_rule_fails():
    d = _build(["*.out"])  # 故意漏掉 covtest_*/
    fails, _ = cla.analyze(d)
    assert any(".gitignore" in r for r, _ in fails)
    assert cla.run(d) == 1


def test_untracked_covtest_fails():
    # .gitignore 缺 covtest 规则时，covtest_abc 目录整体未被忽略，会被提交
    d = _build(["*.out"], leaks=["covtest_abc/cfg.json"])
    fails, _ = cla.analyze(d)
    assert any(r.startswith("covtest_") for r, _ in fails)
    assert cla.run(d) == 1


def test_ignored_covtest_warn_only():
    # 用 **/ 规则忽略目录全部内容
    d = _build(["covtest_*/", "covtest_*/**", "*.out"],
               leaks=["covtest_abc/cover.out"])
    fails, warns = cla.analyze(d)
    assert fails == []
    assert any(r.startswith("covtest_") for r, _ in warns)
    assert cla.run(d) == 0


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
