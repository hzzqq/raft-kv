#!/usr/bin/env python3
"""校验指标(metric)注册名 与 文档描述 是否一致。

从 src 下非测试源码提取 Metrics.Counter/Gauge/Histogram/GaugeWithHelp/...("name") 的真实
指标名,扫描文档(docs/*.md, README.md)确认每个指标都被至少一个文档提及;反向校验文档中
出现的指标名是否真实存在于代码(捕获文档臆造/过时指标)。

退出码 0=一致, 1=存在漂移。纯静态检查,不依赖 Go。

用法: python scripts/check_metrics_docs.py
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SRC_METRIC_RE = re.compile(
    r'Metrics\.(?:Counter|Gauge|Histogram|Hist|Summary|FuncGauge|CounterVec|GaugeVec)(?:WithHelp)?\(\s*"([^"]+)"'
)
# 文档里"像指标名"的 token:小写字母+下划线,至少两段
DOC_METRIC_RE = re.compile(r"\b[a-z]+(?:_[a-z]+){1,}\b")


def walk_sources():
    names = set()
    for dirpath, _dirs, files in os.walk(os.path.join(ROOT, "src")):
        if ".git" in dirpath:
            continue
        for fn in files:
            if not fn.endswith(".go") or fn.endswith("_test.go"):
                continue
            p = os.path.join(dirpath, fn)
            for m in SRC_METRIC_RE.finditer(open(p, encoding="utf-8").read()):
                names.add(m.group(1))
    return names


def main() -> int:
    code_metrics = walk_sources()

    doc_files = []
    for d in ["README.md", "docs/usage.md", "docs/architecture.md",
              "docs/observability.md", "docs/runbook.md", "docs/coverage.md"]:
        fp = os.path.join(ROOT, d)
        if os.path.isfile(fp):
            doc_files.append(fp)

    doc_text = {fp: open(fp, encoding="utf-8").read() for fp in doc_files}

    # 1) 代码指标是否被文档提及
    missing = [m for m in sorted(code_metrics) if not any(m in t for t in doc_text.values())]

    # 2) 文档中的指标名是否真实存在(排除通用英文词 / 测试文件名 / Prometheus 配置键,
    #    以及文档有意使用的简写)。此项为"提示性"检查——仅打印,不阻断(避免误报阻断 CI)。
    GENERIC = {
        "in", "out", "get", "put", "set", "add", "new", "old", "all", "max", "min",
        "true", "false", "key", "value", "data", "time", "name", "type", "kind",
        "http", "json", "yaml", "kv", "id", "ip", "url", "uri", "tcp", "udp",
        "debug", "info", "warn", "error", "fatal", "trace", "span", "grpc", "rpc",
        "go", "go_id", "goroutine", "uptime", "seconds", "total", "count", "size",
        "scheme", "honor_labels", "job_name", "scrape_configs", "static_configs",
        "metrics_path", "listen_addr", "cors_origins", "request_timeout_sec",
        "max_concurrent", "request_id", "build_time", "metrics_test",
    }
    doc_metric_hits = set()
    for t in doc_text.values():
        for h in DOC_METRIC_RE.findall(t):
            if h in GENERIC or h.endswith("_test"):
                continue
            doc_metric_hits.add(h)
    phantom = sorted(m for m in doc_metric_hits if m not in code_metrics)

    errors = []
    if missing:
        errors.append("代码指标未在文档出现的(硬性缺失):")
        for m in missing:
            errors.append(f"  - {m}")
    if phantom:
        errors.append("文档提及但代码中不存在的指标(提示性,可能为简写/配置键):")
        for m in phantom:
            errors.append(f"  - {m}")

    print(f"代码指标 {len(code_metrics)} 个, 文档指标引用 {len(doc_metric_hits)} 个。")
    if errors:
        print("\n".join(errors))
    # 仅硬性缺失(missing)阻断;phantom 为提示性
    if missing:
        return 1
    print("指标与文档一致(硬性项)。OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
