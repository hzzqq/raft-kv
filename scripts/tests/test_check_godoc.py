#!/usr/bin/env python3
"""check_godoc.py 的回归测试（免 Go，守护「godoc 门禁自身」不退化）。

用 fixture .go 文本直接驱动 check_godoc.scan_file / exported_method，断言：
  - 导出 type / 包级 func / 对外可见 method 缺文档注释 → 必须被检出
  - 已带文档注释 / 非对外 method / 非导出符号 → 不误报
  - //go:build 等编译指令不充当 godoc
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check_godoc as cg  # noqa: E402


def _fixture(body: str) -> str:
    d = tempfile.mkdtemp()
    p = os.path.join(d, "sample.go")
    with open(p, "w", encoding="utf-8") as f:
        f.write("package fixture\n\n" + body)
    return p


def test_exported_type_with_doc_ok():
    p = _fixture("// Foo is a thing.\ntype Foo struct{}\n")
    assert cg.scan_file(p) == [], cg.scan_file(p)


def test_exported_type_missing_doc_finds():
    p = _fixture("type Foo struct{}\n")
    f = cg.scan_file(p)
    assert any(k == "type" and n == "Foo" for _, k, n in f), f


def test_exported_func_missing_doc_finds():
    p = _fixture("// Run does x.\nfunc Run(){}\nfunc Hidden(){}")
    f = cg.scan_file(p)
    assert any(k == "func" and n == "Hidden" for _, k, n in f), f
    assert not any(n == "Run" for _, k, n in f)


def test_method_on_exported_recv_missing_doc_finds():
    p = _fixture("type Foo struct{}\nfunc (f *Foo) Bar() {}\n")
    f = cg.scan_file(p)
    assert any(k == "method" and n == "Bar" for _, k, n in f), f


def test_method_on_unexported_recv_skipped():
    p = _fixture("type foo struct{}\nfunc (f *foo) Bar() {}\n")
    assert cg.scan_file(p) == [], cg.scan_file(p)


def test_method_lowercase_name_skipped():
    p = _fixture("// Foo is x.\ntype Foo struct{}\nfunc (f *Foo) bar() {}\n")
    assert cg.scan_file(p) == [], cg.scan_file(p)


def test_build_directive_not_doc():
    p = _fixture("//go:build ignore\n\n// Foo is x.\ntype Foo struct{}\n")
    assert cg.scan_file(p) == [], cg.scan_file(p)


def test_exported_method_parse():
    assert cg.exported_method("func (r *Foo) Bar() {}") == ((True, True), "Bar")
    assert cg.exported_method("func (r foo) Bar() {}") == ((False, True), "Bar")
    assert cg.exported_method("func (r *Foo) bar() {}") == ((True, False), "bar")


def _pkg_fixture(pkgdoc: bool) -> str:
    d = tempfile.mkdtemp()
    p = os.path.join(d, "sample.go")
    header = "// Package fixture is a sample.\n" if pkgdoc else ""
    with open(p, "w", encoding="utf-8") as f:
        f.write(header + "package fixture\n\n" + "type Foo struct{}\n")
    return p


def test_package_has_doc_true():
    assert cg.package_has_doc(_pkg_fixture(True)) is True


def test_package_has_doc_false():
    assert cg.package_has_doc(_pkg_fixture(False)) is False


def test_file_has_exports_true():
    assert cg.file_has_exports(_fixture("type Foo struct{}\n")) is True


def test_file_has_exports_false():
    assert cg.file_has_exports(_fixture("type foo struct{}\nfunc (f *foo) bar(){}\n")) is False


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
