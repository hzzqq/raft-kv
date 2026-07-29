#!/usr/bin/env python3
# cross_machine_test.py —— 跨机部署 OS 进程级实测（I25）。
#
# 单机上最接近真实跨机的形态：每个节点一个**独立 OS 进程**（kvnode.exe），
# 节点间 RPC 走真实 TCP；HTTP 网关以 -connect 纯客户端模式接入（进程内不含任何节点）。
# 共 10 个进程：3 ShardMaster + 2 group x 3 副本 + 1 gateway。
#
# 验证面：
#   1) 全部节点进程拉起后 gateway /readyz 就绪；
#   2) HTTP PUT/GET/APPEND 跨全部 10 个分片正确；
#   3) 杀掉每组一个少数派副本进程（taskkill），读写仍正确（quorum 容错）；
#   4) 杀掉 g0 的旧 leader 所在进程也算在 3) 的覆盖内——少数派选择固定 r=2，
#      若恰为 leader 则同时验证 leader failover。
#
# 用法：python scripts/cross_machine_test.py           # 全流程，约 1-2 分钟
# 退出码：0=PASS，非 0=FAIL（stdout 带 [cross] 前缀日志）。
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
import urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(HERE)
GO = os.path.join(PROJECT, ".go-sdk", "go", "bin", "go.exe")
BIN = os.path.join(PROJECT, "bin")

N_GROUPS, N_REPLICAS, N_SM = 2, 3, 3


def log(msg):
    print("[cross] %s" % msg, flush=True)


def free_ports(n):
    socks, ports = [], []
    for _ in range(n):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        socks.append(s)
        ports.append(s.getsockname()[1])
    for s in socks:
        s.close()
    return ports


def build_binaries():
    if not os.path.exists(GO):
        # 复用 build_local.py 的下载逻辑：直接 import。
        sys.path.insert(0, HERE)
        import build_local  # noqa: E402
        build_local.ensure_go()
    os.makedirs(BIN, exist_ok=True)
    env = dict(os.environ)
    env.setdefault("GOCACHE", os.path.join(PROJECT, ".gocache"))
    env.setdefault("GOPATH", os.path.join(PROJECT, ".gopath"))
    env["GOFLAGS"] = "-mod=mod"
    for pkg, out in (("./src/kvnode", "kvnode.exe"), ("./src/gateway", "gateway.exe")):
        rc = subprocess.call([GO, "build", "-o", os.path.join(BIN, out), pkg],
                             cwd=PROJECT, env=env)
        if rc != 0:
            raise SystemExit("go build %s failed rc=%d" % (pkg, rc))
    log("binaries built: bin/kvnode.exe bin/gateway.exe")


def http_req(method, url, data=None, timeout=5):
    req = urllib.request.Request(url, data=data, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode("utf-8", "replace")


def wait_ready(base, deadline_s):
    t0 = time.time()
    while time.time() - t0 < deadline_s:
        try:
            st, _ = http_req("GET", base + "/readyz", timeout=3)
            if st == 200:
                return True
        except Exception:
            pass
        time.sleep(1.0)
    return False


def main():
    build_binaries()

    ports = free_ports(N_SM + N_GROUPS * N_REPLICAS + 1)
    gw_port = ports[-1]
    nodes = []
    idx = 0
    for j in range(N_SM):
        nodes.append({"name": "m%d" % j, "addr": "127.0.0.1:%d" % ports[idx]})
        idx += 1
    for g in range(N_GROUPS):
        for r in range(N_REPLICAS):
            nodes.append({"name": "g%d-%d" % (g, r), "addr": "127.0.0.1:%d" % ports[idx]})
            idx += 1
    cfg = {"n_groups": N_GROUPS, "n_replicas": N_REPLICAS, "n_sm": N_SM,
           "max_raft_state": 0, "data_dir": "", "nodes": nodes}
    cfg_path = os.path.join(PROJECT, "bin", "cross_deploy.json")
    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=1)
    log("deploy config: %s (gateway :%d)" % (cfg_path, gw_port))

    procs = {}  # name -> Popen
    devnull = open(os.devnull, "wb")

    def spawn(name, args):
        p = subprocess.Popen(args, cwd=PROJECT, stdout=devnull,
                             stderr=subprocess.STDOUT)
        procs[name] = p
        return p

    def kill(name):
        p = procs.pop(name, None)
        if p and p.poll() is None:
            subprocess.call(["taskkill", "/PID", str(p.pid), "/F"],
                            stdout=devnull, stderr=devnull)

    fails = []
    try:
        # 1) 拉起 9 个节点进程 + 1 个网关进程。
        for nd in nodes:
            spawn(nd["name"], [os.path.join(BIN, "kvnode.exe"),
                               "-config", cfg_path, "-name", nd["name"]])
        log("9 node processes spawned")
        base = "http://127.0.0.1:%d" % gw_port
        spawn("gateway", [os.path.join(BIN, "gateway.exe"),
                          "-connect", cfg_path, "-addr", ":%d" % gw_port])

        # 2) 就绪探针（网关启动内含 Join+WaitConfig，冷启动选举给足裕量）。
        if not wait_ready(base, 60):
            fails.append("readyz 60s 未就绪")
            raise RuntimeError(fails[-1])
        log("gateway ready (readyz=200)")

        # 3) 跨全部分片读写。
        n_keys = 20
        for i in range(n_keys):
            st, _ = http_req("PUT", "%s/kv/xm-%d" % (base, i),
                             data=(u"值%d" % i).encode("utf-8"), timeout=15)
            if st != 200:
                fails.append("PUT xm-%d status=%d" % (i, st))
        for i in range(n_keys):
            st, body = http_req("GET", "%s/kv/xm-%d" % (base, i), timeout=15)
            if st != 200 or body != u"值%d" % i:
                fails.append("GET xm-%d status=%d body=%r" % (i, st, body))
        if fails:
            raise RuntimeError("基础读写失败: %s" % fails[:3])
        log("PUT/GET x%d across shards OK" % n_keys)

        # 4) 容错：每组杀一个副本进程（少数派）。
        kill("g0-2")
        kill("g1-2")
        log("killed g0-2 & g1-2 (minority per group)")
        time.sleep(2)  # 若恰杀掉 leader，给选举留时间
        for i in range(n_keys):
            st, body = http_req("GET", "%s/kv/xm-%d" % (base, i), timeout=30)
            if st != 200 or body != u"值%d" % i:
                fails.append("宕机后 GET xm-%d status=%d body=%r" % (i, st, body))
        st, _ = http_req("POST", base + "/kv/xm-0/append", data=b"|tail", timeout=30)
        if st != 200:
            fails.append("宕机后 APPEND status=%d" % st)
        st, body = http_req("GET", base + "/kv/xm-0", timeout=30)
        if body != u"值0|tail":
            fails.append("宕机后 APPEND 校验 body=%r" % body)
        if fails:
            raise RuntimeError("容错读写失败: %s" % fails[:3])
        log("minority-kill tolerance OK (GET x%d + APPEND)" % n_keys)

    except Exception as e:
        if not fails:
            fails.append(str(e))
    finally:
        for name in list(procs):
            kill(name)
        devnull.close()

    if fails:
        log("FAIL: %s" % "; ".join(fails[:5]))
        return 1
    log("PASS: 10 进程（9 节点 + 1 网关）真实 TCP 跨机形态全链路验证通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
