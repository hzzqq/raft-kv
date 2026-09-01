#!/usr/bin/env python3
"""抽取 HTML 内联 <script> 块，用 node --check 做静态语法门禁。

目的：防止 R9 类回归——单文件 HTML 控制台里残留一个多余的 `}` 导致整页 <script>
解析失败、所有仪表盘功能静默失效（I178 真实踩坑，控制台自 2026-08-31 起整页失效、
可观测性归零却无人发现）。本脚本把每个 HTML 文件的 inline <script> 逐个抽出来，
跑 `node --check`（纯语法、不执行），任一文件任一脚本块语法错即非零退出，
可直接作为 pre-commit / CI 门禁，把这类「页面全死」回归挡在提交前。

用法：
    python3 scripts/check_html_js_syntax.py
    python3 scripts/check_html_js_syntax.py path/to/a.html path/to/b.html

HTML 文件默认指向 hzz monorepo 下的两个单文件控制台（raft-kv-console / raft-kv-site）。
若某文件不存在（例如只克隆了 raft-kv 子仓），跳过并提示，不视为失败。
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MONO = os.path.dirname(ROOT)  # hzz monorepo root（raft-kv 的上级目录）

DEFAULT_HTML = [
    os.path.join(MONO, "raft-kv-console", "index.html"),
    os.path.join(MONO, "raft-kv-site", "index.html"),
]

SCRIPT_RE = re.compile(r"<script[^>]*>(.*?)</script>", re.DOTALL | re.IGNORECASE)


def find_node():
    cand = r"C:/Users/Administrator/.workbuddy/binaries/node/versions/22.22.2-2/node.exe"
    if os.path.exists(cand):
        return cand
    return shutil.which("node") or "node"


def check_one(path, node):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        html = f.read()
    blocks = SCRIPT_RE.findall(html)
    if not blocks:
        print(f"  [skip] {os.path.basename(path)}: 无 <script> 块")
        return True
    all_ok = True
    for i, js in enumerate(blocks):
        js = js.strip()
        if not js:
            continue  # 空脚本块（如 <script src=...></script>），跳过
        fd, tmp = tempfile.mkstemp(suffix=".js", prefix="__jscheck_")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(js)
            proc = subprocess.run([node, "--check", tmp],
                                  capture_output=True, text=True)
        finally:
            try:
                os.remove(tmp)
            except OSError:
                pass
        if proc.returncode != 0:
            all_ok = False
            print(f"  [FAIL] {os.path.basename(path)} 第 {i+1} 个 <script> 块语法错误：")
            tail = (proc.stderr or proc.stdout).strip().splitlines()[-12:]
            for line in tail:
                print("    " + line)
    if all_ok:
        print(f"  [ok] {os.path.basename(path)}: {len(blocks)} 个 <script> 块语法通过")
    return all_ok


def main():
    paths = sys.argv[1:] or DEFAULT_HTML
    node = find_node()
    print(f"HTML/JS 内联脚本语法门禁（node={node}）")
    ok = True
    checked = 0
    for p in paths:
        if not os.path.exists(p):
            print(f"  [skip] {p}: 文件不存在，跳过")
            continue
        checked += 1
        if not check_one(p, node):
            ok = False
    if checked == 0:
        print("  无可检查文件（默认目标均不存在），视为通过。")
        return 0
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
