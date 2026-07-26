#!/usr/bin/env python3
"""check_bench_regression.py 的回归自测（免 Go，fixture 驱动纯函数）。"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check_bench_regression as m

SAMPLE = """
goos: linux
goarch: amd64
BenchmarkWithLabelValues-8   	1000000	      25.3 ns/op	       0 B/op	       1 allocs/op
BenchmarkGaugePut-8          	500000	      233.1 ns/op	       8 B/op	       2 allocs/op
"""


def test_parse():
    rows = m.parse_bench(SAMPLE)
    assert "WithLabelValues" in rows and "GaugePut" in rows
    assert rows["WithLabelValues"]["nsop"] == 25.3
    assert rows["WithLabelValues"]["bop"] == 0.0
    assert rows["WithLabelValues"]["allocs"] == 1.0
    assert rows["GaugePut"]["bop"] == 8.0


def test_parse_ignores_non_bench():
    out = "some random line\nBenchmarkFoo-4  100 10 ns/op\nok  raftkv/src/util 1.2s\n"
    rows = m.parse_bench(out)
    assert list(rows.keys()) == ["Foo"]
    assert rows["Foo"]["nsop"] == 10.0


def test_compare_no_regression():
    base = {"GaugePut": {"nsop": 233.1}}
    cur = {"GaugePut": {"nsop": 240.0}}  # +2.9% < 10%
    assert m.compare(base, cur, 0.10) == []


def test_compare_regression():
    base = {"GaugePut": {"nsop": 233.1}}
    cur = {"GaugePut": {"nsop": 300.0}}  # +28% > 10%
    reg = m.compare(base, cur, 0.10)
    assert len(reg) == 1 and reg[0]["name"] == "GaugePut"
    assert reg[0]["ratio"] > 0.10


def test_compare_new_bench_ignored():
    base = {"A": {"nsop": 10.0}}
    cur = {"A": {"nsop": 10.0}, "B": {"nsop": 9999.0}}
    # B 是新基准，不应判为回退
    assert m.compare(base, cur, 0.10) == []


def test_compare_threshold_edge():
    # 恰好等于阈值不算回退（严格 >）
    base = {"A": {"nsop": 100.0}}
    cur = {"A": {"nsop": 110.0}}
    assert m.compare(base, cur, 0.10) == []


def test_compare_zero_baseline_skipped():
    base = {"A": {"nsop": 0.0}}
    cur = {"A": {"nsop": 9999.0}}
    assert m.compare(base, cur, 0.10) == []


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
