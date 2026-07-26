#!/usr/bin/env python3
"""check_metrics_docs.py 的回归测试（免 Go，守护「指标文档一致性门禁自身」不退化）。

断言：
  - SRC_METRIC_RE 正确抽取 Metrics.Counter/GaugeVec.WithHelp 指标名
  - DOC_METRIC_RE 抽取下划线指标 token
  - walk_sources() 集成真实源码非空且含已知指标
  - main() 在本仓库返回 0
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check_metrics_docs as cm  # noqa: E402


def test_src_metric_re_counter():
    m = cm.SRC_METRIC_RE.search('Metrics.Counter("http_requests_total")')
    assert m and m.group(1) == "http_requests_total", m.group(1) if m else None


def test_src_metric_re_gauge_withhelp():
    # 真实代码用法: Metrics.GaugeWithHelp("name", "help")
    m = cm.SRC_METRIC_RE.search('Metrics.GaugeWithHelp("pending_total", "help")')
    assert m and m.group(1) == "pending_total", m.group(1) if m else None


def test_src_metric_re_counter_vec():
    # 真实代码用法: Metrics.CounterVec("name", "label")
    m = cm.SRC_METRIC_RE.search('Metrics.CounterVec("http_responses_total", "code")')
    assert m and m.group(1) == "http_responses_total", m.group(1) if m else None


def test_src_metric_re_histogram():
    m = cm.SRC_METRIC_RE.search('Metrics.Histogram("latency_seconds")')
    assert m and m.group(1) == "latency_seconds", m.group(1) if m else None


def test_doc_metric_re():
    hits = cm.DOC_METRIC_RE.findall("http_requests_total foo_bar stall_seconds")
    assert "http_requests_total" in hits and "foo_bar" in hits


def test_walk_sources_integration():
    names = cm.walk_sources()
    assert names  # 非空
    assert "http_requests_total" in names  # 网关请求指标(cycle118)


def test_main_passes_on_repo():
    assert cm.main() == 0


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
