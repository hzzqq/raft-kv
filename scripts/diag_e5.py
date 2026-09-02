"""E5 单独诊断驱动：只跑 Witness 重配置阶段（I192），复用 build() 构建最新二进制。
用于在完整冒烟（7min+）之外快速复现 E5「Join 后 kill 一投票副本仍能写」的真实行为，
并依赖 verify_witness_reconfig 内嵌的 raft 状态 dump 定位根因。
"""
import os
import sys
import tempfile

# 开启 raft 选举诊断日志（RAFT_DEBUG=1 → raft.go 的 dbg() 输出选举关键路径）
os.environ["RAFT_DEBUG"] = "1"

sys.path.insert(0, os.path.join(os.getcwd(), "scripts"))
import deploy_smoke_local as D  # noqa: E402

workdir = tempfile.mkdtemp(prefix="raftkv_diag_e5_")
s = D.Smoke(19900, workdir, quiet=False)
if not s.build():
    print("BUILD FAILED")
    sys.exit(2)

ok, res = D._run_one_witness(s.bin_dir, False, 19900, True, "reconfig")
for r in res:
    print(r)
passed = sum(1 for r in res if r["ok"])
print(f"\nDIAG E5: {passed}/{len(res)} 通过")
s.cleanup(keep=False)
