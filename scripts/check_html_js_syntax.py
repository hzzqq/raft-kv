#!/usr/bin/env python3
"""抽取 HTML 内联 <script> 块，用 node --check 做静态语法门禁。

目的：防止 R9 类回归——单文件 HTML 控制台里残留一个多余的 `}` 导致整页 <script>
解析失败、所有仪表盘功能静默失效（I178 真实踩坑，控制台自 2026-08-31 起整页失效、
可观测性归零却无人发现）。本脚本把每个 HTML 文件的 inline <script> 逐个抽出来，
跑 `node --check`（纯语法、不执行），任一文件任一脚本块语法错即非零退出，
可直接作为 pre-commit / CI 门禁，把这类「页面全死」回归挡在提交前。

两个防呆设计（I180）：
  1. **行号对齐**：写临时 JS 时前置若干空行，使 node 报出的行号 == HTML 原文件
     真实行号，报错可直接跳到源码对应行（否则只给临时文件行号，无法定位）。
  2. **自带自检（--self-test）**：用合成的好/坏 HTML 反向验证本门禁仍能抓到坏语法、
     且不误报好语法。防止门禁自身静默失效（regex 改坏 / 目标路径变了 → 永远全绿
     却什么都没检查）——这正是 I178「控制台死了没人知道」的重演模式。自检默认随
     每次检查自动跑（可用 --no-selftest 关闭），失败即整体非零退出。

用法：
    python3 scripts/check_html_js_syntax.py                 # 检查默认目标 + 自动自检
    python3 scripts/check_html_js_syntax.py --self-test     # 只跑自检
    python3 scripts/check_html_js_syntax.py --no-selftest   # 只检查，不自检
    python3 scripts/check_html_js_syntax.py path/to/a.html  # 指定文件

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

# 自检用的合成样本：GOOD 必须放行，BAD（末尾多余的 }）必须被拦下。
GOOD_HTML = "<html><body>\n<script>\nfunction f() { return 1; }\n</script>\n</body></html>\n"
BAD_HTML = "<html><body>\n<script>\nfunction f() { return 1; }\n}\n</script>\n</body></html>\n"


def find_node():
    cand = r"C:/Users/Administrator/.workbuddy/binaries/node/versions/22.22.2-2/node.exe"
    if os.path.exists(cand):
        return cand
    return shutil.which("node") or "node"


def _write_aligned(fd, js, start_line):
    """把 js 写入临时文件，并前置空行使 js 第 1 行落在第 start_line 行。

    这样 node 报错的行号就等于 HTML 原文件的真实行号，可直接跳转定位。
    """
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
        if start_line > 1:
            f.write("\n" * (start_line - 1))
        f.write(js)


def check_one(path, node, verbose=True):
    """检查单个 HTML 文件的全部 inline <script> 块。返回 True 表示全部通过。"""
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        html = f.read()
    blocks = list(SCRIPT_RE.finditer(html))
    if not blocks:
        if verbose:
            print(f"  [skip] {os.path.basename(path)}: 无 <script> 块")
        return True
    all_ok = True
    for i, m in enumerate(blocks):
        js = m.group(1)
        if not js.strip():
            continue  # 空脚本块（如 <script src=...></script>），跳过
        # 内容块起始行号（1-based），用于行号对齐
        start_line = html.count("\n", 0, m.start(1)) + 1
        fd, tmp = tempfile.mkstemp(suffix=".js", prefix="__jscheck_")
        try:
            _write_aligned(fd, js, start_line)
            proc = subprocess.run([node, "--check", tmp],
                                  capture_output=True, text=True)
        finally:
            try:
                os.remove(tmp)
            except OSError:
                pass
        if proc.returncode != 0:
            all_ok = False
            if verbose:
                print(f"  [FAIL] {os.path.basename(path)} 第 {i+1} 个 "
                      f"<script> 块语法错误（块起始于 HTML 第 {start_line} 行）：")
                # node 报错的「文件:行号」行必须保留——行号已对齐，把临时文件路径
                # 换回真实 HTML 路径后即可直接点击跳到源码对应行；丢弃 node 调用栈
                # （其路径是临时文件，对定位无意义）。
                err = (proc.stderr or proc.stdout).strip()
                err = err.replace(tmp, path).replace(
                    os.path.basename(tmp), os.path.basename(path))
                kept = [ln for ln in err.splitlines()
                        if not ln.strip().startswith("at ")]
                for line in kept[:6]:
                    print("    " + line)
    if all_ok and verbose:
        print(f"  [ok] {os.path.basename(path)}: {len(blocks)} 个 <script> 块语法通过")
    return all_ok


def self_test(node, verbose=True):
    """反向验证门禁自身健康：好样本放行、坏样本拦下。返回 True 表示门禁有效。"""
    if verbose:
        print("  [自检] 验证门禁自身有效性（合成样本）...")
    ok = True
    for name, content, expect_pass in (("好样本", GOOD_HTML, True),
                                       ("坏样本(多余 })", BAD_HTML, False)):
        fd, tmp_html = tempfile.mkstemp(suffix=".html", prefix="__selftest_")
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
                f.write(content)
            got = check_one(tmp_html, node, verbose=False)
        finally:
            try:
                os.remove(tmp_html)
            except OSError:
                pass
        if got != expect_pass:
            ok = False
            if verbose:
                want = "放行" if expect_pass else "拦下"
                real = "放行" if got else "拦下"
                print(f"    [FAIL] {name}：期望{want}，实际{real} → 门禁自身失效！")
    if verbose and ok:
        print("  [自检] OK：好样本放行、坏样本（多余 }）被拦下，门禁有效")
    return ok


def main():
    args = list(sys.argv[1:])
    only_selftest = "--self-test" in args
    no_selftest = "--no-selftest" in args
    paths = [a for a in args if not a.startswith("--")]
    if only_selftest:
        paths = []
    else:
        paths = paths or DEFAULT_HTML

    node = find_node()
    print(f"HTML/JS 内联脚本语法门禁（node={node}）")

    if only_selftest:
        return 0 if self_test(node) else 1

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

    if not no_selftest:
        if not self_test(node):
            ok = False

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
