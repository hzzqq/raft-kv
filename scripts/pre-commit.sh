#!/usr/bin/env bash
# pre-commit.sh —— 提交前门禁：跑完仓库内全部「免 Go」静态自检，
# 任一失败即阻断提交，避免文档漂移 / CHANGELOG 失配 / 日志污染被提交进树
# （这正是 cycle 110 修复的那类「CI docs-links 静默失败却已落库」问题）。
#
# 安装：make hooks  （把本文件软链/复制到 .git/hooks/pre-commit）
# 临时绕过：git commit --no-verify
set -uo pipefail

# 定位仓库根（脚本放在 scripts/ 下）
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

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
