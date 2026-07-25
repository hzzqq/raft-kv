#!/usr/bin/env python3
"""godoc 覆盖率门禁：扫描 Go 源码，报告缺少文档注释的导出标识符。

规则（贴合 `go doc` 可见性）：
  - 包级 `type Xxx`：Xxx 首字母大写 → 必须紧邻上方有 `//` 文档注释。
  - 包级 `func Xxx(`：Xxx 首字母大写（且非 main/init）→ 必须有文档注释。
  - 方法 `func (r *T) M(` / `func (r T) M(`：仅当 T 与 M 均为大写（对外可见）→ 必须有文档注释。
  - 排除 `_test.go`、`func main`、`func init`、`//go:build` / `// Code generated` 等代码生成指令。

退出码：发现缺失 → 1；全部具备 → 0。纯静态文本扫描，免 Go 工具链。
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")

DOC_DIRECTIVES = ("//go:", "// Code generated", "// +build")

type_re = re.compile(r"^type\s+([A-Z]\w*)\b")
func_re = re.compile(r"^func\s+")
main_re = re.compile(r"^func\s+(main|init)\s*\(")


def exported_method(m):
    """m: 匹配 'func (recv) Name(' 的整行；返回 (receiver_exported, name_exported)。"""
    rm = re.match(r"^func\s*\(([^)]*)\)\s+(\w+)\s*\(", m)
    if not rm:
        return None
    recv = rm.group(1).strip()
    name = rm.group(2)
    # receiver 形如：'r *TypeName' / 'r TypeName' / 'r *pkg.TypeName'
    recv_type = re.sub(r"^\*", "", recv.split()[-1])
    recv_type = recv_type.split(".")[-1]
    return (recv_type[:1].isupper(), name[:1].isupper()), name


def preceding_doc_comment(lines, idx):
    """返回 decl 行上方紧邻非空白行是否为合法文档注释。"""
    i = idx - 1
    while i >= 0 and lines[i].strip() == "":
        i -= 1
    if i < 0:
        return False
    s = lines[i].strip()
    if not s.startswith("//"):
        return False
    if any(s.startswith(d) for d in DOC_DIRECTIVES):
        # 编译/生成指令不算 godoc，继续向上找真正的 doc 注释
        j = i - 1
        while j >= 0 and lines[j].strip() == "":
            j -= 1
        if j >= 0 and lines[j].strip().startswith("//") and not any(
            lines[j].strip().startswith(d) for d in DOC_DIRECTIVES
        ):
            return True
        return False
    return True


def scan_file(path):
    findings = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.read().split("\n")
    in_block = False  # 粗略跳过 /* ... */ 块
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("/*"):
            in_block = True
        if in_block:
            if stripped.endswith("*/"):
                in_block = False
            continue
        if stripped.startswith("//"):
            continue
        # type
        tm = type_re.match(line)
        if tm:
            if not preceding_doc_comment(lines, idx):
                findings.append((idx + 1, "type", tm.group(1)))
            continue
        # func
        if func_re.match(line) and not main_re.match(line):
            # 方法？
            mm = re.match(r"^func\s*\(([^)]*)\)\s+(\w+)\s*\(", line)
            if mm:
                info = exported_method(line)
                if info is None:
                    continue
                (recv_exp, name_exp), _ = info
                if recv_exp and name_exp:
                    if not preceding_doc_comment(lines, idx):
                        findings.append((idx + 1, "method", mm.group(2)))
                continue
            # 包级函数
            fm = re.match(r"^func\s+([A-Z]\w*)\s*\(", line)
            if fm:
                if not preceding_doc_comment(lines, idx):
                    findings.append((idx + 1, "func", fm.group(1)))
    return findings


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else SRC
    total = 0
    files_with = 0
    for dirpath, dirs, files in os.walk(target):
        # 跳过 vendored / 生成目录
        if any(seg in dirpath for seg in ("/vendor/", "\\vendor\\")):
            continue
        for fn in files:
            if not fn.endswith(".go") or fn.endswith("_test.go"):
                continue
            fpath = os.path.join(dirpath, fn)
            rel = os.path.relpath(fpath, ROOT)
            f = scan_file(fpath)
            if f:
                files_with += 1
                total += len(f)
                print("::godoc-gap:: %s" % rel)
                for ln, kind, name in f:
                    print("    L%-4d %-7s %s" % (ln, kind, name))
    if total:
        print("\n发现 %d 处导出标识符缺少 godoc 文档注释（%d 个文件）。" % (total, files_with))
        print("建议：在对应声明上方补 '// <说明>'。")
        return 1
    print("OK: 所有导出标识符均具备 godoc 文档注释。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
