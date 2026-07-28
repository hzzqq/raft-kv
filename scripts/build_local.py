#!/usr/bin/env python3
# build_local.py —— 本机无 Go 工具链时的「内存下载 + 同进程构建」便捷脚本。
#
# 设计动机：部分 CI / 沙箱环境没有预装 Go，且进程对磁盘的大文件写入会被丢弃，
# 导致 `curl go.zip > 磁盘` 后文件"消失"。本脚本在单次 Python 进程内：
#   1) 把 Go 工具链下载进内存（不落盘 zip）；
#   2) 解压到本次命令的临时磁盘（项目目录内的 .go-sdk，与 cwd 同 FS）；
#   3) 在同一进程内用 subprocess 跑 `go build ./...` / `go vet ./...` / `go test <targets>`。
# 因为 Go 与构建都在同一个 Bash 命令的临时 FS 里，所以全程可见。
#
# 用法：
#   python scripts/build_local.py                 # build + vet 全仓 + 跑 cluster/raft/transport 测试
#   python scripts/build_local.py ./src/...       # build + vet 全仓 + 跑指定包测试
#   python scripts/build_local.py -run TestXxx ./src/cluster/...   # 只跑某个测试
import os
import sys
import io
import zipfile
import urllib.request
import subprocess

GO_VER = "go1.22.12"
GO_URL = "https://golang.google.cn/dl/%s.windows-amd64.zip" % GO_VER


def win(p):
    """把 Git-Bash 风格 /e/... 路径归一为 Windows 盘符路径，避免 CreateProcess WinError 267。"""
    if p.startswith("/e/"):
        return "E:/" + p[len("/e/"):]
    if p.startswith("/c/"):
        return "C:/" + p[len("/c/"):]
    return p


# 项目根 = scripts/ 的上一级（用 __file__ 推导，避免硬编码盘符）。
HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = win(os.path.dirname(HERE))
GO_DEST = os.path.join(PROJECT, ".go-sdk")
GOCACHE = os.path.join(PROJECT, ".gocache")
GOPATH = os.path.join(PROJECT, ".gopath")


def ensure_go():
    go = os.path.join(GO_DEST, "go", "bin", "go.exe")
    if os.path.exists(go):
        return go
    print("[build_local] downloading %s ..." % GO_VER, flush=True)
    req = urllib.request.Request(GO_URL, headers={"User-Agent": "curl/8.19.0"})
    data = urllib.request.urlopen(req, timeout=600).read()
    print("[build_local] got %d bytes, extracting ..." % len(data), flush=True)
    os.makedirs(GO_DEST, exist_ok=True)
    zf = zipfile.ZipFile(io.BytesIO(data))
    zf.extractall(GO_DEST)
    zf.close()
    if not os.path.exists(go):
        raise SystemExit("go binary not found after extract: " + go)
    return go


def main():
    go = ensure_go()
    env = dict(os.environ)
    env["PATH"] = os.path.dirname(go) + ";" + env.get("PATH", "")
    env["GOCACHE"] = GOCACHE
    env["GOPATH"] = GOPATH
    env["GOFLAGS"] = "-mod=mod"
    os.makedirs(GOCACHE, exist_ok=True)
    os.makedirs(GOPATH, exist_ok=True)

    args = sys.argv[1:]
    test_targets = [a for a in args if not a.startswith("-")]
    extra = [a for a in args if a.startswith("-")]
    if not test_targets:
        test_targets = ["./src/cluster/...", "./src/raft/...", "./src/transport/..."]

    print("[build_local] go build ./...", flush=True)
    r = subprocess.run([go, "build", "./..."], env=env, cwd=PROJECT)
    print("BUILD rc=%d" % r.returncode)
    if r.returncode != 0:
        sys.exit(r.returncode)

    print("[build_local] go vet ./...", flush=True)
    r = subprocess.run([go, "vet", "./..."], env=env, cwd=PROJECT)
    print("VET rc=%d" % r.returncode)
    if r.returncode != 0:
        sys.exit(r.returncode)

    print("[build_local] go test %s" % " ".join(test_targets), flush=True)
    r = subprocess.run([go, "test"] + extra + test_targets, env=env, cwd=PROJECT)
    print("TEST rc=%d" % r.returncode)
    sys.exit(r.returncode)


if __name__ == "__main__":
    main()
