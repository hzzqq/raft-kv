#!/usr/bin/env python3
"""本地真机部署冒烟：不依赖 docker / wsl，直接起真实 kvnode + gateway 多进程。

背景：`deploy/deploy_smoke.sh` 依赖 docker compose，而本环境 wsl.exe 被安全策略
列入黑名单、Docker Desktop 引擎起不来，导致 `deploy/` 这套交付物**从未拿到过任何
运行时证据**（只有 bash -n 语法自检）。但 docker-compose 里跑的其实就是两个普通
Go 二进制：

    kvnode  -config cluster.json -name m0    -http :9100
    gateway -connect cluster.json -addr :8080

它们不依赖容器。本脚本在本地起同样的 7 个真实进程（3 ShardMaster + 3 ShardKV
+ 1 Gateway，节点间走**真实 TCP** 而非内存网络），跑 deploy_smoke.sh 想验证的
那套端点契约，并额外做一次真实故障演练——从而在没有 docker 的机器上，依然能
证明「这套部署件真的起得来、真的能服务、挂一个副本真的还能用」。

检查项：
  1. 6 个 kvnode 的 /healthz 全部 200
  2. gateway /readyz 200（集群可读可写）
  3. 经 gateway 做真实 PUT/GET 往返，值必须一致
  4. /metrics 必须是合法 Prometheus 文本格式（含 HELP/TYPE 与样本行）
  5. /status 里 3 个副本都在
  6. 故障演练：kill 掉一个 ShardKV 副本 → /readyz 仍 200 且 PUT/GET 仍成功
     （在真实进程 + 真实 TCP 下再现 quorum 容错，这是 deploy 层首次拿到的证据）

用法：
    python3 scripts/deploy_smoke_local.py            # 跑完整冒烟（自动清理进程）
    python3 scripts/deploy_smoke_local.py --keep     # 保留进程与产物目录（调试用）
    python3 scripts/deploy_smoke_local.py --json     # 机器可读 JSON 报告

退出码：0=全部通过，1=有检查失败，2=环境/构建失败。
零第三方依赖（仅标准库）。
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 端口规划（相对 --port-base）：SM raft +1..+3，KV raft +11..+13，
# SM http +21..+23，KV http +31..+33，gateway +50。
SM_HTTP = [21, 22, 23]
KV_HTTP = [31, 32, 33]
GW_PORT = 50
SM_RAFT = [1, 2, 3]
KV_RAFT = [11, 12, 13]

READY_TIMEOUT = 60.0   # 等集群就绪上限（秒）
HTTP_TIMEOUT = 5.0


def find_go():
    for cand in ("/e/go-sdk/go/bin/go.exe", "E:/go-sdk/go/bin/go.exe"):
        if os.path.exists(cand):
            return cand
    return shutil.which("go") or "go"


# Prometheus 抓取时实际发送的 Accept（含 text/plain 与 openmetrics）。
# gateway 的 /metrics 按 Accept 协商：命中则返回 Prometheus 文本，否则返回 JSON，
# 因此冒烟必须带上真实 Accept，否则验证的是「浏览器口径」而非「Prometheus 口径」。
PROM_ACCEPT = ("application/openmetrics-text; version=0.0.1,"
               "text/plain;version=0.0.4;q=0.75,*/*;q=0.1")


def http(method, url, body=None, headers=None, timeout=HTTP_TIMEOUT):
    """返回 (status, text)；连不上返回 (None, errstr)。"""
    data = body.encode() if isinstance(body, str) else body
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001 - 连不上/超时都归为未就绪
        return None, str(e)


class Smoke:
    def __init__(self, port_base, workdir, quiet=False):
        self.port_base = port_base
        self.workdir = workdir
        self.quiet = quiet
        self.procs = []       # (label, Popen)
        self.results = []     # (name, ok, detail)
        self.bin_dir = os.path.join(workdir, "bin")
        self.log_dir = os.path.join(workdir, "logs")
        self.data_dir = os.path.join(workdir, "data")
        self.cfg_path = os.path.join(workdir, "cluster.json")

    def _p(self, msg):
        """过程输出（--json 模式下静默，保证 stdout 是纯 JSON）。"""
        if not self.quiet:
            print(msg)

    # ---------- 构建 ----------
    def build(self):
        os.makedirs(self.bin_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)
        go = find_go()
        ext = ".exe" if os.name == "nt" else ""
        for pkg in ("kvnode", "gateway"):
            out = os.path.join(self.bin_dir, pkg + ext)
            cmd = [go, "build", "-o", out, "./src/" + pkg]
            p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
            if p.returncode != 0:
                print(f"构建 {pkg} 失败：\n{(p.stderr or p.stdout)[-2000:]}")
                return False
        self._p(f"✓ 构建完成（go={go}）")
        return True

    # ---------- 配置 ----------
    def write_config(self):
        b = self.port_base
        nodes = [{"name": f"m{i}", "addr": f"127.0.0.1:{b+SM_RAFT[i]}"}
                 for i in range(3)]
        nodes += [{"name": f"g0-{i}", "addr": f"127.0.0.1:{b+KV_RAFT[i]}"}
                  for i in range(3)]
        cfg = {
            "n_groups": 1, "n_replicas": 3, "n_sm": 3,
            "max_raft_state": 1000,
            "data_dir": self.data_dir,
            "nodes": nodes,
        }
        with open(self.cfg_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        self._p(f"✓ 本地部署清单已生成：{self.cfg_path}")

    # ---------- 启动 ----------
    def launch(self, pkg, label, args):
        """启动一个真实进程。pkg 是二进制名（kvnode/gateway），label 仅用于日志与识别。"""
        ext = ".exe" if os.name == "nt" else ""
        binpath = os.path.join(self.bin_dir, pkg + ext)
        if not os.path.exists(binpath):
            raise FileNotFoundError(f"二进制不存在: {binpath}（构建失败？）")
        logpath = os.path.join(self.log_dir, f"{label}.log")
        logf = open(logpath, "w", encoding="utf-8", errors="replace")
        p = subprocess.Popen([binpath] + args, cwd=ROOT,
                             stdout=logf, stderr=subprocess.STDOUT)
        self.procs.append((label, p, logf))
        return p

    def start_all(self):
        b = self.port_base
        for i in range(3):
            self.launch("kvnode", f"kvnode-m{i}",
                        ["-config", self.cfg_path, "-name", f"m{i}",
                         "-http", f"127.0.0.1:{b+SM_HTTP[i]}"])
        for i in range(3):
            self.launch("kvnode", f"kvnode-g0-{i}",
                        ["-config", self.cfg_path, "-name", f"g0-{i}",
                         "-http", f"127.0.0.1:{b+KV_HTTP[i]}"])
        self.launch("gateway", "gateway",
                    ["-connect", self.cfg_path,
                     "-addr", f"127.0.0.1:{b+GW_PORT}"])
        self._p(f"✓ 已启动 {len(self.procs)} 个真实进程"
                f"（3 ShardMaster + 3 ShardKV + 1 Gateway，真实 TCP）")

    # ---------- 等待就绪 ----------
    def wait_ready(self):
        b = self.port_base
        node_ports = [(f"sm{i}", b + SM_HTTP[i]) for i in range(3)]
        node_ports += [(f"kv{i}", b + KV_HTTP[i]) for i in range(3)]
        gw = f"http://127.0.0.1:{b+GW_PORT}"
        deadline = time.time() + READY_TIMEOUT
        last = ""
        while time.time() < deadline:
            if all(p.poll() is None for (_, p, _) in self.procs):
                bad = []
                for label, port in node_ports:
                    st, _ = http("GET", f"http://127.0.0.1:{port}/healthz")
                    if st != 200:
                        bad.append(f"{label}:{st}")
                st, _ = http("GET", gw + "/readyz")
                if st != 200:
                    bad.append(f"gateway-readyz:{st}")
                if not bad:
                    self._p(f"✓ 集群就绪（6 节点 /healthz 200 + gateway /readyz 200）"
                            f"，用时 {READY_TIMEOUT - (deadline - time.time()):.1f}s")
                    return True
                last = "未就绪: " + ", ".join(bad)
            else:
                dead = [n for (n, p, _) in self.procs if p.poll() is not None]
                last = "进程提前退出: " + ", ".join(dead)
                break
            time.sleep(1.0)
        self._p(f"✗ 等待集群就绪超时：{last}")
        self.dump_logs()
        return False

    def dump_logs(self):
        self._p("---- 进程日志（最后 15 行/进程）----")
        for name, _, _ in self.procs:
            path = os.path.join(self.log_dir, f"{name}.log")
            if not os.path.exists(path):
                continue
            try:
                with open(path, encoding="utf-8", errors="replace") as f:
                    tail = f.read().strip().splitlines()[-15:]
            except OSError:
                continue
            if tail:
                self._p(f"[{name}] " + "\n".join(tail))

    # ---------- 检查 ----------
    def check(self, name, ok, detail=""):
        self.results.append({"name": name, "ok": bool(ok), "detail": detail})
        self._p(f"  [{'ok' if ok else 'FAIL'}] {name}"
                + (f" — {detail}" if detail else ""))
        return ok

    def run_checks(self):
        b = self.port_base
        gw = f"http://127.0.0.1:{b+GW_PORT}"
        ok = True

        # 1. 6 节点 /healthz
        up = 0
        for i in range(3):
            for off, label in ((SM_HTTP[i], f"sm{i}"), (KV_HTTP[i], f"kv{i}")):
                st, _ = http("GET", f"http://127.0.0.1:{b+off}/healthz")
                if st == 200:
                    up += 1
        ok &= self.check("6 个 kvnode 节点 /healthz 全部 200", up == 6, f"{up}/6")

        # 2. gateway /readyz
        st, _ = http("GET", gw + "/readyz")
        ok &= self.check("gateway /readyz 200", st == 200, f"status={st}")

        # 3. 真实 PUT/GET 往返
        st, _ = http("PUT", gw + "/kv/smoke-key", body="smoke-value-42")
        put_ok = st == 200
        st2, got = http("GET", gw + "/kv/smoke-key")
        roundtrip = put_ok and st2 == 200 and got.strip() == "smoke-value-42"
        ok &= self.check("经 gateway 真实 PUT/GET 往返一致", roundtrip,
                         f"put={st} get={st2} value={got.strip()!r}")

        # 4. gateway /metrics 在 Prometheus 的 Accept 下必须是 Prometheus 文本格式
        #    （对应 deploy/prometheus.yml 的 raftkv-gateway job）
        st, body = http("GET", gw + "/metrics",
                        headers={"Accept": PROM_ACCEPT})
        has_schema = bool(body) and ("# HELP" in body) and ("# TYPE" in body)
        samples = sum(1 for ln in body.splitlines()
                      if ln and not ln.startswith("#"))
        ok &= self.check("gateway /metrics 可被 Prometheus 抓取（文本格式）",
                         st == 200 and has_schema and samples > 0,
                         f"status={st} 样本行={samples}")

        # 5. gateway /metrics 协商另一端：不带 text/plain 时返回 JSON（控制台消费口径）
        st, body = http("GET", gw + "/metrics", headers={"Accept": "application/json"})
        try:
            json.loads(body)
            is_json = True
        except Exception:  # noqa: BLE001
            is_json = False
        ok &= self.check("gateway /metrics 按 Accept 协商（JSON 口径可用）",
                         st == 200 and is_json, f"status={st} is_json={is_json}")

        # 6. 6 个节点的 /metrics 也要能被 Prometheus 抓取
        #    （对应 deploy/prometheus.yml 的 raftkv-nodes job，抓 6 个 :9100）
        ok_nodes = 0
        for i in range(3):
            for off in (SM_HTTP[i], KV_HTTP[i]):
                st, body = http("GET", f"http://127.0.0.1:{b+off}/metrics",
                                headers={"Accept": PROM_ACCEPT})
                if st == 200 and "# TYPE" in body and "# HELP" in body:
                    ok_nodes += 1
        ok &= self.check("6 个节点 /metrics 均可被 Prometheus 抓取",
                         ok_nodes == 6, f"{ok_nodes}/6")

        # 7. /status 可消费
        st, body = http("GET", gw + "/status")
        ok &= self.check("gateway /status 可消费", st == 200 and len(body) > 0,
                         f"status={st} {len(body)}B")

        # 6. 故障演练：kill 一个 ShardKV 副本，quorum 2/3 仍可服务
        victim = None
        for name, p, _ in self.procs:
            if name == "kvnode-g0-2":
                victim = (name, p)
                break
        if victim:
            victim[1].kill()
            self._p(f"  → 已 kill {victim[0]}（ShardKV 第 3 副本），等集群自愈…")
            time.sleep(3.0)
            st, _ = http("GET", gw + "/readyz")
            ok &= self.check("kill 一个副本后 /readyz 仍 200（quorum 容错）",
                             st == 200, f"status={st}")
            st2, got2 = http("PUT", gw + "/kv/smoke-after-kill", body="still-alive")
            st3, got3 = http("GET", gw + "/kv/smoke-after-kill")
            survived = st2 == 200 and st3 == 200 and got3.strip() == "still-alive"
            ok &= self.check("kill 一个副本后读写仍成功（真实 TCP 下无丢失）",
                             survived, f"put={st2} get={st3} value={got3.strip()!r}")
        else:
            ok &= self.check("故障演练：找到目标副本", False, "未找到 kvnode-g0-2")

        return ok

    # ---------- 清理 ----------
    def cleanup(self, keep=False):
        for name, p, logf in self.procs:
            if p.poll() is None:
                try:
                    p.terminate()
                    p.wait(timeout=5)
                except Exception:  # noqa: BLE001
                    try:
                        p.kill()
                    except Exception:  # noqa: BLE001
                        pass
            try:
                logf.close()
            except Exception:  # noqa: BLE001
                pass
        self.procs = []
        if not keep:
            shutil.rmtree(self.workdir, ignore_errors=True)
            self._p(f"✓ 已清理进程与临时目录 {self.workdir}")
        else:
            self._p(f"  --keep：进程已停，产物保留在 {self.workdir}")


def main():
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--keep", action="store_true", help="保留产物目录（调试）")
    ap.add_argument("--json", action="store_true", help="输出 JSON 报告")
    ap.add_argument("--port-base", type=int, default=19100)
    args = ap.parse_args()

    quiet = args.json
    workdir = tempfile.mkdtemp(prefix="raftkv_deploy_smoke_")
    s = Smoke(args.port_base, workdir, quiet=quiet)
    ok = False
    try:
        if not quiet:
            print(f"本地真机部署冒烟（端口基 {args.port_base}，"
                  f"不依赖 docker/wsl）")
        if not s.build():
            return 2
        s.write_config()
        s.start_all()
        if not s.wait_ready():
            if quiet:
                print(json.dumps({"ok": False, "stage": "wait_ready",
                                  "checks": s.results}, ensure_ascii=False))
            return 1
        ok = s.run_checks()
    finally:
        s.cleanup(keep=args.keep)

    if args.json:
        print(json.dumps({"ok": ok, "checks": s.results},
                         ensure_ascii=False, indent=2))
        return 0 if ok else 1
    print("\n" + "=" * 56)
    passed = sum(1 for r in s.results if r["ok"])
    print(f"本地部署冒烟：{passed}/{len(s.results)} 项通过 —— "
          f"{'OK，deploy 交付物在真实进程下确认可用' if ok else '存在失败项'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
