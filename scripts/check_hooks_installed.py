#!/usr/bin/env python3
"""提交门禁钩子安装状态护栏（免 Go 工具链）。

本仓库的「文档漂移 / CHANGELOG 失配 / 自驱日志污染」等缺陷，依赖
scripts/pre-commit.sh 在每次 git commit 时自动跑 scripts/check_all.py 来阻断。
但钩子需要主动 `make hooks` 安装——若某工作副本从未安装，门禁会**静默失效**，
漂移类缺陷可毫无阻碍地落库（这正是 cycle 110 修复的那类问题在「门禁自身」上的变种）。

本脚本校验：
  1. 是否为 git 仓库（无 .git 则跳过，视为 N/A）。
  2. .git/hooks/pre-commit 是否存在且为可执行文件。
  3. 其内容是否与 scripts/pre-commit.sh 一致（防止钩子被改坏/被旧版本覆盖后无人察觉）。

默认 warn-only：缺失/不一致时退出 1 但仅作为 WARN 提示（不阻断 CI 的干净克隆），
本地开发环境应据提示运行 `make hooks` 修复。一致性校验失败也仅 WARN，避免升级 Go
工具链类改动连带误伤。

用法：
    python3 scripts/check_hooks_installed.py        # 检查并打印状态
    python3 scripts/check_hooks_installed.py --strict   # 缺失即非零退出

退出码：0=已安装且一致(或 N/A)；1=缺失/不一致(warn-only)；2=--strict 且缺失。
"""
import argparse
import os
import stat
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GIT_DIR = os.path.join(ROOT, ".git")
HOOK_PATH = os.path.join(GIT_DIR, "hooks", "pre-commit")
SRC_HOOK = os.path.join(ROOT, "scripts", "pre-commit.sh")


def _read(p: str) -> str:
    with open(p, encoding="utf-8", errors="ignore") as f:
        return f.read()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--strict", action="store_true",
                    help="钩子缺失时以退出码 2 升级为硬失败")
    args = ap.parse_args()

    if not os.path.isdir(GIT_DIR):
        print("[N/A] 非 git 仓库，跳过钩子安装检查。")
        return 0

    if not os.path.isfile(SRC_HOOK):
        print(f"[WARN] 源钩子缺失：{os.path.relpath(SRC_HOOK, ROOT)}，"
              "无法比对（请先恢复 scripts/pre-commit.sh）。")
        return 1

    if not os.path.isfile(HOOK_PATH):
        print(f"[WARN] 提交门禁钩子未安装：.git/hooks/pre-commit 不存在。")
        print("       本地开发请运行 `make hooks` 安装，否则文档漂移缺陷可静默落库。")
        return 2 if args.strict else 1

    # 可执行位检查：仅 POSIX 有意义。Git for Windows 通过 sh 运行 hook，
    # 不依赖文件系统可执行位；NTFS 也不暴露 unix 执行位，故 Windows 跳过此步。
    if os.name != "nt":
        mode = os.stat(HOOK_PATH).st_mode
        if not (mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)):
            print("[WARN] .git/hooks/pre-commit 存在但不可执行，commit 时不会触发。")
            print("       运行 `make hooks` 重装以修正权限。")
            return 2 if args.strict else 1

    # 内容一致性检查（去掉行尾空白与尾部换行差异，避免 CRLF/LF 误报）
    def _norm(b: str) -> str:
        return "\n".join(line.rstrip() for line in b.splitlines())

    try:
        installed = _norm(_read(HOOK_PATH))
        source = _norm(_read(SRC_HOOK))
    except OSError as e:
        print(f"[WARN] 读取钩子失败：{e}")
        return 1

    if installed == source:
        print("[PASS] 提交门禁钩子已安装且与 scripts/pre-commit.sh 一致。")
        return 0

    print("[WARN] .git/hooks/pre-commit 与 scripts/pre-commit.sh 内容不一致"
          "（可能钩子被旧版本覆盖）。")
    print("       建议运行 `make hooks` 重新安装以保持门禁最新。")
    return 1


if __name__ == "__main__":
    sys.exit(main())
