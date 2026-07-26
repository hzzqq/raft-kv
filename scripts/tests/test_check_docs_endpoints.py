#!/usr/bin/env python3
"""check_docs_endpoints.py 的回归测试（免 Go，守护「端点/CLI 文档一致性门禁自身」不退化）。

断言：
  - ENDPOINT_RE 抽取 register("METHOD /path")
  - CLI_RE 抽取 `case "cmd":` 子命令
  - norm_path 正确归并 {key} / /append 后缀
  - main() 在本仓库返回 0
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check_docs_endpoints as ce  # noqa: E402


def test_endpoint_re():
    m = ce.ENDPOINT_RE.search('register("GET /kv/{key}")')
    assert m and m.group(1) == "GET" and m.group(2) == "/kv/{key}", m.groups() if m else None


def test_cli_re():
    assert ce.CLI_RE.findall('case "get":\ncase "put":') == ["get", "put"]


def test_norm_path_brace():
    assert ce.norm_path("/kv/{key}") == "/kv"


def test_norm_path_debug():
    assert ce.norm_path("/debug/shards") == "/debug/shards"


def test_norm_path_append_suffix():
    assert ce.norm_path("/kv/{key}/append") == "/kv"


def test_main_passes_on_repo():
    assert ce.main() == 0


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
