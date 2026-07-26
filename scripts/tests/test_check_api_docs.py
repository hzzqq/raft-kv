#!/usr/bin/env python3
"""check_api_docs.py 的回归测试（免 Go，守护「API 文档一致性门禁自身」不退化）。

用 fixture + 真实源码集成，断言：
  - CLIENT_RE 匹配各类小写接收者名的 Client 方法，且排除 GRPCClient
  - UTIL_TYPE_RE / DOC_CLIENT_REF_RE 正确抽取
  - client_methods() / util_types() 集成真实源码非空
  - main() 在本仓库（文档同步）返回 0
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check_api_docs as ca  # noqa: E402


def test_client_re_basic():
    m = ca.CLIENT_RE.search("func (c *Client) Get(")
    assert m and m.group(1) == "Get", m.group(1) if m else None


def test_client_re_robust_receiver():
    # 宽松接收者名（cli / kc / 任意小写）都应识别；修复前仅硬编码 c|s|kc|cl
    assert ca.CLIENT_RE.search("func (cli *Client) Foo(").group(1) == "Foo"
    assert ca.CLIENT_RE.search("func (kc *Client) Bar(").group(1) == "Bar"
    assert ca.CLIENT_RE.search("func (x Client) Baz(").group(1) == "Baz"


def test_client_re_excludes_grpc():
    # GRPCClient 是独立类型，绝不能混入 Client 方法集合
    assert ca.CLIENT_RE.search("func (c *GRPCClient) Get(") is None


def test_util_type_re():
    m = ca.UTIL_TYPE_RE.search("type BufferPool struct {}")
    assert m and m.group(1) == "BufferPool", m.group(1) if m else None


def test_doc_client_ref_re():
    refs = ca.DOC_CLIENT_REF_RE.findall("see Client.Get and Client.Put")
    assert "Get" in refs and "Put" in refs


def test_client_methods_integration():
    ms = ca.client_methods()
    assert {"Get", "Put", "Append", "MGet", "MSet"} <= ms


def test_util_types_integration():
    assert ca.util_types()  # 非空


def test_main_passes_on_repo():
    assert ca.main() == 0


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
