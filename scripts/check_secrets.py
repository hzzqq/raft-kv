#!/usr/bin/env python3
"""密钥/凭证泄露静态扫描（免 Go 工具链，安全护栏）。

本仓库此前没有任何「密钥泄露」维度的门禁：若在代码/配置里误提交私钥、云厂商
AK/SK、token 等敏感凭证，会直接落库并被推送到远端，造成不可逆的凭证暴露
（R2 隐性安全缺口——不报错但危害极大）。

本脚本在「不编译」前提下扫描源码与配置中的高置信凭证模式：
  HARD（硬阻断，发现即非零退出，应阻断提交/CI）：
    - PEM 私钥块：-----BEGIN ... PRIVATE KEY-----
    - AWS Access Key ID：AKIA 前缀 + 16 位大写字母数字
    - AWS Secret Access Key 字面量赋值
  WARN（仅提示，不阻断；多为测试 fixture 或文档示例，需人工确认）：
    - 形如 password/secret/api_key/token 的明文赋值
    - Slack token（xoxb-/xoxp-/...）、GitHub token（ghp_/github_pat_）

扫描范围默认 src/ scripts/ .github/（排除 .git / bin / vendor / *.exe / cover.out /
文档 *.md 以避免「说明性示例密钥」误报），可用 --root 重定向或 --path 追加。

用法：
    python3 scripts/check_secrets.py            # 扫描默认范围，HARD 阻断 / WARN 提示
    python3 scripts/check_secrets.py --json r.json
    python3 scripts/check_secrets.py --strict   # 把 WARN 也升级为非零退出

退出码：0=无 HARD（WARN 亦无或仅提示）；2=存在 HARD 凭证（应阻断）。
"""
import argparse
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DEFAULT_DIRS = ("src", "scripts", ".github")
SKIP_DIRS = {".git", "bin", "vendor", "__pycache__", "tests"}
SKIP_EXT = {".exe", ".out", ".png", ".jpg", ".jpeg", ".gif", ".pdf", ".test"}
# 扫描器自身的源文件含「模式定义字面量」，会命中自己的正则，需自排除
SELF_PATH = os.path.join(ROOT, "scripts", "check_secrets.py")

# HARD：高置信凭证
HARD_PATTERNS = [
    ("PEM 私钥块", re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY-----")),
    ("AWS Access Key ID", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("AWS Secret Access Key 字面量",
     re.compile(r"(?i)aws_secret_access_key\s*[=:]\s*['\"][A-Za-z0-9/+=]{40}['\"]")),
    ("私钥内容(含 PRIVATE KEY 与 base64 块)",
     re.compile(r"PRIVATE KEY-----[\s\S]{1,2000}?-----END")),
]
# WARN：启发式，需人工确认
WARN_PATTERNS = [
    ("明文口令/密钥赋值",
     re.compile(r"(?i)(password|passwd|pwd|secret|api[_-]?key|access[_-]?token|auth[_-]?token)\s*[=:]\s*['\"][^'\"\n]{6,}['\"]")),
    ("Slack token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}")),
    ("GitHub token", re.compile(r"\b(ghp_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{20,})")),
]


def _candidate_files(root: str, extra_paths):
    files = []
    for d in DEFAULT_DIRS:
        base = os.path.join(root, d)
        if not os.path.isdir(base):
            continue
        for r, dirs, names in os.walk(base):
            dirs[:] = [x for x in dirs if x not in SKIP_DIRS]
            for n in names:
                if os.path.splitext(n)[1] in SKIP_EXT:
                    continue
                files.append(os.path.join(r, n))
    for p in extra_paths:
        if os.path.isfile(p):
            files.append(p)
    return files


def scan_text(text: str, rel: str = "<text>") -> "tuple[list, list]":
    """对一段文本扫描凭证模式，返回 (hard_hits, warn_hits)。

    每条命中为 (rel, line, label, snippet)。抽离为纯函数以便单元测试 fixture 驱动。
    """
    hard_hits, warn_hits = [], []
    for label, rx in HARD_PATTERNS:
        for m in rx.finditer(text):
            line = text[:m.start()].count("\n") + 1
            hard_hits.append((rel, line, label, m.group(0)[:40]))
    for label, rx in WARN_PATTERNS:
        for m in rx.finditer(text):
            line = text[:m.start()].count("\n") + 1
            warn_hits.append((rel, line, label, m.group(0)[:40]))
    return hard_hits, warn_hits


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=ROOT, help="仓库根目录")
    ap.add_argument("--path", action="append", default=[], help="额外要扫描的文件/目录")
    ap.add_argument("--json", dest="json_out", help="将发现写入 JSON")
    ap.add_argument("--strict", action="store_true", help="WARN 也升级为非零退出")
    args = ap.parse_args()

    files = [f for f in _candidate_files(args.root, args.path) if f != SELF_PATH]
    hard_hits, warn_hits = [], []

    for fp in files:
        try:
            text = open(fp, encoding="utf-8", errors="ignore").read()
        except OSError:
            continue
        rel = os.path.relpath(fp, args.root)
        h, w = scan_text(text, rel)
        hard_hits.extend(h)
        warn_hits.extend(w)

    if args.json_out:
        json.dump({"hard": hard_hits, "warn": warn_hits},
                  open(args.json_out, "w"), ensure_ascii=False, indent=2)

    ok = True
    if hard_hits:
        ok = False
        print(f"[FAIL] 发现 {len(hard_hits)} 处高置信凭证泄露（HARD，应阻断）：", file=sys.stderr)
        for rel, line, label, snip in hard_hits:
            print(f"  - {rel}:{line} [{label}] {snip!r}", file=sys.stderr)
    if warn_hits:
        print(f"[WARN] 发现 {len(warn_hits)} 处疑似明文凭证（需人工确认）：")
        for rel, line, label, snip in warn_hits:
            print(f"  - {rel}:{line} [{label}] {snip!r}")

    if not hard_hits and not warn_hits:
        print("[PASS] 未发现密钥/凭证泄露。")
        return 0
    if not hard_hits and warn_hits:
        return 2 if args.strict else 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
