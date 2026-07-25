#!/usr/bin/env python3
"""校验 kvcli.Client 公开方法与 util 公共类型 在文档中的一致性（不依赖 Go 工具链）。

校验维度：
  1) 前向(硬阻断)：kvcli.Client 的每个导出方法必须在文档中出现，否则视为公开契约缺失文档。
  2) 反向(硬阻断)：文档中以 `Client.<Method>` 形式引用的客户端方法，必须真实存在于代码，
     捕获文档漂移 / 笔误（如方法被改名或删除后文档未同步）。
  3) 提示(不阻断)：util 包导出类型若未在文档出现，仅打印提示，引导后续补全。

退出码：0=硬性项通过；1=存在硬性缺失或漂移。纯静态检查，可接入 CI `docs-links` job。

用法：python3 scripts/check_api_docs.py
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CLIENT_RE = re.compile(r'func \((?:c \*?|s \*?|kc \*?|cl \*?)Client\) ([A-Z]\w*)\(')
UTIL_TYPE_RE = re.compile(r'^type ([A-Z]\w*) ', re.M)
DOC_CLIENT_REF_RE = re.compile(r'Client\.([A-Z]\w*)')


def client_methods() -> set:
    methods = set()
    d = os.path.join(ROOT, "src", "kvcli")
    for fn in os.listdir(d):
        if not fn.endswith(".go") or fn.endswith("_test.go"):
            continue
        src = open(os.path.join(d, fn), encoding="utf-8").read()
        for m in CLIENT_RE.finditer(src):
            methods.add(m.group(1))
    return methods


def util_types() -> set:
    types = set()
    d = os.path.join(ROOT, "src", "util")
    for fn in os.listdir(d):
        if not fn.endswith(".go") or fn.endswith("_test.go"):
            continue
        src = open(os.path.join(d, fn), encoding="utf-8").read()
        for m in UTIL_TYPE_RE.finditer(src):
            types.add(m.group(1))
    return types


def main() -> int:
    methods = client_methods()
    doc_files = [os.path.join(ROOT, "README.md"), os.path.join(ROOT, "docs", "usage.md")]
    doc_text = "\n".join(
        open(f, encoding="utf-8").read() for f in doc_files if os.path.isfile(f)
    )

    errors = []

    # 1) 前向：Client 方法必须被文档提及
    missing = [m for m in sorted(methods) if m not in doc_text]
    if missing:
        errors.append("kvcli.Client 导出方法未在文档出现(硬性缺失):")
        for m in missing:
            errors.append(f"  - Client.{m}")

    # 2) 反向：文档中的 Client.<Method> 必须真实存在
    doc_refs = set(DOC_CLIENT_REF_RE.findall(doc_text))
    phantom = sorted(r for r in doc_refs if r not in methods)
    if phantom:
        errors.append("文档引用了不存在的 Client 方法(漂移/笔误, 硬阻断):")
        for m in phantom:
            errors.append(f"  - Client.{m}")

    # 3) util 提示（不阻断）
    types = util_types()
    undoc_types = [t for t in sorted(types) if t not in doc_text]

    print(f"kvcli.Client 方法 {len(methods)} 个; util 导出类型 {len(types)} 个。")
    if undoc_types:
        print("提示: util 以下导出类型未在文档出现(不阻断, 引导补全):")
        for t in undoc_types:
            print(f"  - {t}")

    if errors:
        print("\n".join(errors))
        return 1
    print("API 文档一致性(硬性项)通过。OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
