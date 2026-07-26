#!/usr/bin/env python3
"""check_hooks_installed.py 的回归测试（免 Go，守护「钩子护栏自身」不退化）。

通过 monkeypatch 全局路径指向临时 git 树，断言：
  - 钩子已安装且与源一致（且 POSIX 下含可执行位）→ 0
  - 钩子缺失（warn-only）→ 1；--strict → 2
  - 钩子与源不一致 → 1
"""
import os
import stat
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check_hooks_installed as ch  # noqa: E402


def _build(consistent=True, hook_exists=True):
    d = tempfile.mkdtemp()
    git = os.path.join(d, ".git")
    os.makedirs(os.path.join(git, "hooks"))
    scripts = os.path.join(d, "scripts")
    os.makedirs(scripts)
    src = os.path.join(scripts, "pre-commit.sh")
    open(src, "w", encoding="utf-8").write("echo hi\n")
    if hook_exists:
        hook = os.path.join(git, "hooks", "pre-commit")
        content = "echo hi\n" if consistent else "echo old-version\n"
        open(hook, "w", encoding="utf-8").write(content)
        # 赋予可执行位（POSIX 下 main 会校验；Windows 忽略）
        try:
            os.chmod(hook, 0o755)
        except OSError:
            pass
    ch.ROOT = d
    ch.GIT_DIR = git
    ch.SRC_HOOK = src
    ch.HOOK_PATH = os.path.join(git, "hooks", "pre-commit")
    return d


def test_consistent_ok():
    _build(consistent=True, hook_exists=True)
    assert ch.main() == 0


def test_missing_hook_warn():
    _build(hook_exists=False)
    assert ch.main() == 1


def test_inconsistent_warn():
    _build(consistent=False, hook_exists=True)
    assert ch.main() == 1


def test_strict_missing():
    _build(hook_exists=False)
    saved = sys.argv
    sys.argv = ["x", "--strict"]
    try:
        assert ch.main() == 2
    finally:
        sys.argv = saved


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
