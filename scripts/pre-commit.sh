#!/usr/bin/env bash
# pre-commit.sh —— 提交前门禁：跑完仓库内全部「免 Go」静态自检，
# 任一失败即阻断提交，避免文档漂移 / CHANGELOG 失配 / 日志污染被提交进树
# （这正是 cycle 110 修复的那类「CI docs-links 静默失败却已落库」问题）。
#
# 安装：make hooks  （把本文件软链/复制到 .git/hooks/pre-commit）
# 临时绕过：git commit --no-verify
set -uo pipefail

# 定位仓库根：用 git 权威解析，兼容两种调用位置——
#   * 源脚本 scripts/pre-commit.sh（dirname=$ROOT/scripts）
#   * 安装后 .git/hooks/pre-commit（dirname=$ROOT/.git/hooks，/.. 会错归到 .git）
# 此前用 `cd dirname/$0/..` 在「已安装」状态下把 ROOT 错算成 .git，
# 导致每次提交都以错误路径运行 check_all.py 被误阻断（门禁自身缺陷）。
ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$ROOT" ]; then
  ROOT="$(cd "$(dirname "$0")/.." && pwd)"
fi
cd "$ROOT"

# 优先使用 WorkBuddy 管理的 Python：Git 钩子子进程（sh）的 PATH 可能未包含它，
# 导致 python 解析到错误版本，使 gen_test_coverage 等门禁误报虚假失败（cycle 收官复盘确认）。
WB_PY_BIN="/c/Users/Administrator/.workbuddy/binaries/python/versions/3.13.12"
case ":$PATH:" in
  *":$WB_PY_BIN:"*) ;;
  *) export PATH="$WB_PY_BIN:$PATH" ;;
esac

PY="${PYTHON:-python3}"
command -v "$PY" >/dev/null 2>&1 || PY=python

echo "==> [pre-commit] 运行免 Go 静态自检（scripts/check_all.py）"
if "$PY" scripts/check_all.py; then
  echo "==> [pre-commit] 全部自检通过，允许提交。"
  exit 0
else
  rc=$?
  echo "" >&2
  echo "==> [pre-commit] 自检未通过（exit=$rc），已阻断提交。" >&2
  echo "    请先修复上述缺陷，或运行 \`$PY scripts/check_all.py\` 查看明细。" >&2
  echo "    确认无误且需临时跳过可：git commit --no-verify" >&2
  exit 1
fi
