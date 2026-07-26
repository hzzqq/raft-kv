#!/usr/bin/env python3
"""check_md_links.py 的回归测试（免 Go，守护「链接门禁自身」不退化）。

用 fixture .md + subprocess 驱动 check_md_links.main，断言：
  - 锚点 slug 生成规则（含中文/行内格式）
  - collect_anchors 收集标题锚点 + HTML id
  - 失效文件链接 → exit=1；有效链接 → exit=0
"""
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check_md_links as cm  # noqa: E402


def test_slugify_basic():
    assert cm.slugify("Hello World") == "hello-world"
    assert cm.slugify("`Code` Foo!") == "code-foo"


def test_slugify_chinese_kept():
    # 中文不受 \w 过滤影响，且原样保留（仅小写化）
    assert cm.slugify("安全政策") == "安全政策"


def test_collect_anchors():
    d = tempfile.mkdtemp()
    p = os.path.join(d, "a.md")
    with open(p, "w", encoding="utf-8") as f:
        f.write("# Title\n## Sub Heading\n<div id=\"custom\"></div>\n")
    a = cm.collect_anchors(p)
    assert "title" in a
    assert "sub-heading" in a
    assert "custom" in a


def _run_in_dir(files: dict) -> subprocess.CompletedProcess:
    d = tempfile.mkdtemp()
    for name, content in files.items():
        with open(os.path.join(d, name), "w", encoding="utf-8") as f:
            f.write(content)
    return subprocess.run(
        [sys.executable, cm.__file__, d],
        capture_output=True, text=True,
    )


def test_broken_file_link_fails():
    r = _run_in_dir({
        "good.md": "# Good\n[link](bad.md)\n",
        "bad.md": "# Bad\n[missing](nope.md)\n",
    })
    assert r.returncode == 1, r.stdout
    assert "nope.md" in r.stdout


def test_valid_file_link_ok():
    r = _run_in_dir({
        "good.md": "# Good\n[link](other.md)\n",
        "other.md": "# Other\n",
    })
    assert r.returncode == 0, r.stdout


def test_broken_anchor_fails():
    r = _run_in_dir({
        "good.md": "# Good\n[to](#nonexistent)\n",
    })
    assert r.returncode == 1, r.stdout
    assert "nonexistent" in r.stdout


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
