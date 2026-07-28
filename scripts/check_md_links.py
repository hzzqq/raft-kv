#!/usr/bin/env python3
"""校验仓库内所有 Markdown 文档的内部链接（相对文件 + 锚点）是否可解析。

用法: python scripts/check_md_links.py [根目录]

- 跳过 http(s)/mailto 等外部链接
- 相对文件链接: 解析相对于当前文件的路径，检查文件存在
- 锚点链接(#anchor 或 file.md#anchor): 检查目标文件中存在对应标题 slug 或 HTML id

退出码 0 = 全部通过, 1 = 存在失效链接。

本脚本为纯静态检查，不依赖 Go 工具链，可在任何环境自检。
"""
import os
import re
import sys

LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$", re.MULTILINE)
HTML_ID_RE = re.compile(r'\bid="([^"]+)"')


def slugify(text: str) -> str:
    """近似 GitHub 的标题锚点生成规则。"""
    s = text.strip().lower()
    # 去 markdown 行内格式
    s = re.sub(r"[`*_~]", "", s)
    s = re.sub(r"[^\w\u4e00-\u9fff\s-]", "", s)
    s = s.strip().replace(" ", "-")
    return s


def collect_anchors(path: str) -> set:
    anchors = set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except OSError:
        return anchors
    for m in HEADING_RE.finditer(content):
        anchors.add(slugify(m.group(2)))
    for m in HTML_ID_RE.finditer(content):
        anchors.add(m.group(1))
    return anchors


def main() -> int:
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    md_files = []
    for dirpath, _dirs, files in os.walk(root):
        # 跳过版本控制、构建产物与本地 Go SDK 目录
        skip_dirs = (".git", "bin", "node_modules", ".go-sdk", ".gopath", ".gocache")
        if any(seg in skip_dirs for seg in dirpath.split(os.sep)):
            continue
        for fn in files:
            if fn.lower().endswith(".md"):
                md_files.append(os.path.join(dirpath, fn))

    errors = []
    checked = 0
    for md in sorted(md_files):
        base = os.path.dirname(md)
        try:
            with open(md, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError as e:
            errors.append(f"{md}: 无法读取 ({e})")
            continue
        for m in LINK_RE.finditer(content):
            target = m.group(1).strip()
            if not target or target.startswith(("http://", "https://", "mailto:", "#")):
                # 纯外部或已单独处理
                if target.startswith("#"):
                    # 同文件锚点
                    anchor = target[1:]
                    if anchor and anchor not in collect_anchors(md):
                        errors.append(f"{md}: 锚点未找到 #{anchor}")
                    checked += 1
                continue
            # 拆文件与锚点
            file_part, _, anchor_part = target.partition("#")
            if not file_part:
                # 形如 "file.md#" 仅锚点但带文件名 —— 已在上面 file_part 非空分支
                if anchor_part and anchor_part not in collect_anchors(md):
                    errors.append(f"{md}: 锚点未找到 #{anchor_part}")
                checked += 1
                continue
            resolved = os.path.normpath(os.path.join(base, file_part))
            if not os.path.isfile(resolved):
                errors.append(f"{md}: 链接文件不存在 -> {target} (解析为 {resolved})")
                checked += 1
                continue
            if anchor_part:
                if anchor_part not in collect_anchors(resolved):
                    errors.append(
                        f"{md}: 跨文件锚点未找到 {target} (文件存在但无 #{anchor_part})"
                    )
            checked += 1

    print(f"校验了 {checked} 个内部链接, 覆盖 {len(md_files)} 个 Markdown 文件。")
    if errors:
        print(f"\n发现 {len(errors)} 处失效链接:")
        for e in errors:
            print("  - " + e)
        return 1
    print("全部内部链接可解析。OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
