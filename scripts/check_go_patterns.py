#!/usr/bin/env python3
"""Go 源码静态反模式扫描（不依赖 Go 工具链）。

用纯文本/正则对 src 下 .go 源码做轻量静态检查，捕获此前自驱迭代已根治、但极易
在后续改动中悄然回归的反模式；同时提示尚存的隐性技术债。作为 CI `docs-links` job
的免编译静态门禁，保证「已修复的坏味道」不会倒退。

硬阻断项（当前应全空，命中即 CI 失败）：
  1) `ioutil.`：自 Go 1.16 起全面废弃，必须彻底清除。
  2) `time.After(` 出现在非 `_test.go` 文件：生产路径 select 中 time.After 定时器在
     提前退出后直到触发(1~3s)才被 GC，热路径累积造成定时器泄漏（已被 #239 根治，
     仅测试文件允许超时写法）。

提示项（不阻断，仅罗列引导清理）：
  - 非 main / 非测试库代码中的 `fmt.Print*` / `println(`（应走结构化日志）。
  - 非 main / 非测试库代码中的 `log.Fatal` / `os.Exit`（库内不应直接中止进程，应返回 error）。
  - 非测试库代码中的 `panic(`（允许 `Result.Must` 等显式 .unwrap 语义，须人工确认）。
  - `TODO`/`FIXME`/`HACK`/`XXX` 技术债标记。

退出码：0=无硬阻断项；1=存在硬阻断项。
用法：python3 scripts/check_go_patterns.py
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")

CRIT_IOUTIL = re.compile(r'\bioutil\.')
CRIT_TIME_AFTER = re.compile(r'\btime\.After\(')
WARN_FMT = re.compile(r'\bfmt\.(Print|Println|Printf|Fprint|Fprintf|Fprintln)\b|\bprintln\(')
WARN_FATAL = re.compile(r'\b(log\.(Fatal|Fatalf|Fatalln)|os\.Exit)\b')
WARN_PANIC = re.compile(r'\bpanic\(')
WARN_TODO = re.compile(r'(?://|/\*)\s*(TODO|FIXME|HACK|XXX)\b')


def strip_comment(line: str) -> str:
    """删除 // 行注释（不处理块注释 / 字符串内 //，够用）。"""
    in_str = False
    out = []
    i = 0
    while i < len(line):
        c = line[i]
        if c == '"' and (i == 0 or line[i - 1] != '\\'):
            in_str = not in_str
            out.append(c)
        elif c == '/' and not in_str and i + 1 < len(line) and line[i + 1] == '/':
            break
        else:
            out.append(c)
        i += 1
    return ''.join(out)


def iter_go():
    for dirpath, _dirs, files in os.walk(SRC):
        if '.git' in dirpath:
            continue
        for fn in files:
            if not fn.endswith('.go'):
                continue
            yield os.path.join(dirpath, fn)


def main() -> int:
    critical = []   # (file, line_no, kind, text)
    warn_fmt, warn_fatal, warn_panic, warn_todo = [], [], [], []

    for path in iter_go():
        is_test = path.endswith('_test.go')
        is_main = os.path.basename(path) == 'main.go'
        rel = os.path.relpath(path, ROOT).replace(os.sep, '/')
        with open(path, encoding='utf-8') as f:
            for n, raw in enumerate(f, 1):
                code = strip_comment(raw)
                # 硬阻断 1：ioutil.（全部文件）
                if CRIT_IOUTIL.search(code):
                    critical.append((rel, n, 'ioutil', raw.strip()))
                # 硬阻断 2：非测试文件 time.After
                if not is_test and CRIT_TIME_AFTER.search(code):
                    critical.append((rel, n, 'time.After(non-test)', raw.strip()))
                # 提示
                if not is_test and not is_main and WARN_FMT.search(code):
                    warn_fmt.append((rel, n, raw.strip()))
                if not is_test and not is_main and WARN_FATAL.search(code):
                    warn_fatal.append((rel, n, raw.strip()))
                if not is_test and WARN_PANIC.search(code):
                    warn_panic.append((rel, n, raw.strip()))
                if WARN_TODO.search(code):
                    warn_todo.append((rel, n, raw.strip()))

    print(f"扫描 {SRC} 下 .go 文件完成。")
    if warn_fmt:
        print(f"\n[提示] 库代码中的 fmt.Print*（应走结构化日志，{len(warn_fmt)} 处）:")
        for rel, n, t in warn_fmt:
            print(f"  - {rel}:{n}  {t}")
    if warn_fatal:
        print(f"\n[提示] 库代码中的 log.Fatal/os.Exit（库内不应直接中止进程，{len(warn_fatal)} 处）:")
        for rel, n, t in warn_fatal:
            print(f"  - {rel}:{n}  {t}")
    if warn_panic:
        print(f"\n[提示] 库代码中的 panic（含 .unwrap 语义须人工确认，{len(warn_panic)} 处）:")
        for rel, n, t in warn_panic:
            print(f"  - {rel}:{n}  {t}")
    if warn_todo:
        print(f"\n[提示] 技术债标记（{len(warn_todo)} 处）:")
        for rel, n, t in warn_todo:
            print(f"  - {rel}:{n}  {t}")

    if critical:
        print(f"\n[硬阻断] 发现 {len(critical)} 处已根治反模式回归:")
        for rel, n, kind, t in critical:
            print(f"  - [{kind}] {rel}:{n}  {t}")
        return 1
    print("\n无硬阻断项（ioutil / 非测试 time.After 均清）。Go 静态反模式守卫通过。OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
