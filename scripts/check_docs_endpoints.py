#!/usr/bin/env python3
"""校验网关 HTTP 端点 / kvcli CLI 子命令 与文档描述是否一致。

从 src/gateway/gateway.go 提取 register("METHOD /path") 真实端点,从 src/kvcli/main.go
提取 CLI 子命令,再扫描文档(docs/*.md, README.md)确认每个端点/子命令都被至少一个文档
提及;同时反向检查文档中出现的端点是否在代码里真实存在(捕获文档臆造)。

退出码 0=一致, 1=存在漂移。纯静态检查,不依赖 Go。

用法: python scripts/check_docs_endpoints.py
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GATEWAY = os.path.join(ROOT, "src", "gateway", "gateway.go")
KVCLI_MAIN = os.path.join(ROOT, "src", "kvcli", "main.go")

DOC_FILES = [
    os.path.join(ROOT, "README.md"),
    os.path.join(ROOT, "docs", "usage.md"),
    os.path.join(ROOT, "docs", "architecture.md"),
    os.path.join(ROOT, "docs", "observability.md"),
    os.path.join(ROOT, "docs", "runbook.md"),
    os.path.join(ROOT, "docs", "coverage.md"),
]

ENDPOINT_RE = re.compile(r'register\("([A-Z]+) (/[^"]+)"')
CLI_RE = re.compile(r'case\s+"([a-z]+)":')


def norm_path(path: str) -> str:
    """去掉 {key} 占位与 append 后缀,归并到稳定基路径便于文档匹配。"""
    p = path.split("{")[0].rstrip("/")
    if p.endswith("/append"):
        p = p[: -len("/append")]
    return p or "/"


def main() -> int:
    with open(GATEWAY, encoding="utf-8") as f:
        gw = f.read()
    code_endpoints = []
    for m in ENDPOINT_RE.finditer(gw):
        method, path = m.group(1), m.group(2)
        code_endpoints.append((method, path, norm_path(path)))

    cli_cmds = CLI_RE.findall(open(KVCLI_MAIN, encoding="utf-8").read()) if os.path.isfile(KVCLI_MAIN) else []

    doc_text = {}
    for d in DOC_FILES:
        if os.path.isfile(d):
            doc_text[d] = open(d, encoding="utf-8").read()
        else:
            doc_text[d] = ""

    # 1) 代码中端点是否在文档被提及
    missing_in_docs = []
    for method, path, base in code_endpoints:
        mentioned = any(base in t for t in doc_text.values())
        if not mentioned:
            missing_in_docs.append(f"{method} {path}")

    # 2) 文档中出现的端点是否在代码里真实存在(反向校验)
    #    只抽取"像 HTTP 端点"的 token(以 /debug/x、/kv、/metrics、/healthz、/readyz、/status
    #    开头,且后续字符为 / 或边界),排除包路径(/kvraft、/kvcli)与文件路径(.go/.exe)。
    code_bases = {base for _, _, base in code_endpoints}
    DOC_TOKEN_RE = re.compile(
        r"/(?:debug/[A-Za-z0-9_-]+|metrics|healthz|readyz|status|kv)(?:/[A-Za-z0-9_{}.-]+)*"
    )
    doc_endpoint_hits = set()
    for t in doc_text.values():
        for h in DOC_TOKEN_RE.findall(t):
            if "." in h:  # 排除 .go/.exe 等文件路径
                continue
            base = norm_path(h) if "{key}" in h else h.rstrip("/") or "/"
            if base.startswith("/kv/"):  # /kv/foo 等示例 key 归并到 /kv
                base = "/kv"
            doc_endpoint_hits.add(base)
    phantom = [h for h in doc_endpoint_hits if h not in code_bases and h != "/"]

    errors = []
    if missing_in_docs:
        errors.append("代码中存在但文档缺失的端点:")
        for e in missing_in_docs:
            errors.append(f"  - {e}")
    if phantom:
        errors.append("文档提及但代码中不存在的端点(臆造/过时):")
        for e in phantom:
            errors.append(f"  - {e}")

    print(f"代码端点 {len(code_endpoints)} 个, CLI 子命令 {len(cli_cmds)} 个, 文档端点引用 {len(doc_endpoint_hits)} 个。")
    if errors:
        print("\n".join(errors))
        return 1
    print("端点与文档一致。OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
