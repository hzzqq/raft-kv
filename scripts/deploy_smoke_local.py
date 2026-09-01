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

检查项（共 12 项，全绿即 deploy 交付物在真实进程 + 真实 TCP 下确认可用）：
  1. 6 个 kvnode 的 /healthz 全部 200
  2. gateway /readyz 200（集群可读可写）
  3. 经 gateway 做真实 PUT/GET 往返，值必须一致
  4. gateway /metrics 在 Prometheus 的 Accept 下是合法文本格式（含 HELP/TYPE 与样本行）
  5. gateway /metrics 按 Accept 协商另一端的 JSON 口径可用
  6. 6 个节点的 /metrics 也都能被 Prometheus 抓取（对应 prometheus.yml 的 raftkv-nodes job）
  7. gateway /status 可消费
  8. 故障演练：kill 一个 ShardKV 副本 → /readyz 仍 200（真实 TCP 下 quorum 容错）
  9. 故障演练：kill 一个 ShardKV 副本 → 读写仍成功（无数据丢失）
 10. alerts.yml 指标名契约：每条告警引用的指标名都真实存在——指标名改错即死指标。
 11. alerts.yml 标签匹配器契约：每条告警 `{label=...}` 引用的标签都真实 emit——指标名在
     但标签没 emit，告警会静默失效（比指标名更深一层的死规则）。
 12. deploy 监控拓扑一致性：prometheus.yml 抓取的端口/副本数，与 docker-compose.yml 里
     真实二进制的 -http/-addr 端口、副本数对齐（防监控配置漂移抓盲）。

产物：始终向 deploy/smoke_report.json 写一份可审计/可展示报告（含全量检查项 +
quorum 容错证据 + 拓扑一致性证据），该文件被 .gitignore 忽略。

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
import re
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
    # 与 Makefile 一致：优先用 WorkBuddy 托管的 Go（C:\Users\Administrator\.workbuddy\binaries\go），
    # 它是本机唯一能正常编译本项目的工具链——E:\go-sdk 与 E:\e\go-sdk 两份安装都带着一份
    # 损坏的 src/vendor，编译标准库会报 "package X is not in std"。
    # nt 下必须用盘符绝对路径：Python 会把 "/e/..." 解析成「当前盘根下的 e/...」（E: 盘 →
    # E:\e\go-sdk\...），正好撞上那份坏的；同时显式传 GOROOT 杜绝推断错位。
    if os.name == "nt":
        cands = (
            "C:/Users/Administrator/.workbuddy/binaries/go/go/bin/go.exe",
            "E:/go-sdk/go/bin/go.exe",
        )
    else:
        cands = (
            "/c/Users/Administrator/.workbuddy/binaries/go/go/bin/go",
            "/e/go-sdk/go/bin/go",
            "E:/go-sdk/go/bin/go",
        )
    for cand in cands:
        if os.path.exists(cand):
            return cand
    return shutil.which("go") or "go"


# Prometheus 抓取时实际发送的 Accept（含 text/plain 与 openmetrics）。
# gateway 的 /metrics 按 Accept 协商：命中则返回 Prometheus 文本，否则返回 JSON，
# 因此冒烟必须带上真实 Accept，否则验证的是「浏览器口径」而非「Prometheus 口径」。
PROM_ACCEPT = ("application/openmetrics-text; version=0.0.1,"
               "text/plain;version=0.0.4;q=0.75,*/*;q=0.1")

# 告警规则指标契约校验用的 PromQL 关键字 / 标签 / 内置指标。
PROM_FUNCS = {"sum", "min", "max", "avg", "count", "rate", "irate", "increase",
              "histogram_quantile", "min_over_time", "max_over_time", "avg_over_time",
              "sum_over_time", "by", "without", "group", "absent", "clamp_max",
              "clamp_min", "delta", "idelta", "deriv", "stddev", "stdvar", "topk",
              "bottomk", "count_values"}
PROM_LABELS = {"code", "job", "node", "method", "le", "quantile", "instance"}
PROM_BUILTIN = {"up"}  # Prometheus 自动生成，必存在（但我们 /metrics 不输出）
# Prometheus 抓取层注入的标签：不论代码是否 emit，{job}/ {instance} 恒存在，
# 故告警 expr 里引用它们作匹配器时不应判为「缺标签」（否则会误报死规则）。
PROM_INJECTED_LABELS = {"job", "instance"}
# 本项目的自定义 Histogram（src/metrics）输出的派生后缀：_p50/_p95/_p99 分位 gauge
# + _sum/_count（注意：不是标准 Prometheus 的 _bucket，故不能当成 histogram 派生规则）。
HIST_SUFFIXES = ("_p50", "_p95", "_p99", "_sum", "_count")


def _extract_metrics_and_labels(body):
    """从 Prometheus 文本格式里抽取 (指标名集合, {指标: 标签名集合})。"""
    names = set()
    labels = {}
    for ln in body.splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        m = re.match(r"([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?", ln)
        if not m:
            continue
        name = m.group(1)
        names.add(name)
        if m.group(2):
            for kv in m.group(2).split(","):
                kv = kv.strip()
                if not kv:
                    continue
                km = re.match(r"([a-zA-Z_][a-zA-Z0-9_]*)", kv)
                if km:
                    labels.setdefault(name, set()).add(km.group(1))
    return names, labels


def _deploy_topo():
    """解析 deploy/ 下 docker-compose.yml 与 prometheus.yml 的拓扑，校验监控配置漂移。
    返回 dict：compose 侧 node_port/node_count/gw_port/gw_count，
              prometheus 侧 p_node_port/p_node_count/p_gw_port/p_gw_count。"""
    out = {}
    compose = os.path.join(ROOT, "deploy", "docker-compose.yml")
    prom = os.path.join(ROOT, "deploy", "prometheus.yml")
    if os.path.exists(compose):
        ct = open(compose, encoding="utf-8").read()
        node_http = re.findall(r'"-http",\s*":(\d+)"', ct)
        gw_addr = re.findall(r'"-addr",\s*":(\d+)"', ct)
        out["node_port"] = int(node_http[0]) if node_http else None
        out["node_count"] = len(node_http)
        out["gw_port"] = int(gw_addr[0]) if gw_addr else None
        out["gw_count"] = len(gw_addr)
    if os.path.exists(prom):
        pt = open(prom, encoding="utf-8").read()
        for blk in re.split(r"job_name:", pt):
            if blk.lstrip().startswith("raftkv-nodes"):
                ports = [int(p) for t in re.findall(r"targets:\s*\[([^\]]*)\]", blk)
                         for p in re.findall(r":(\d+)", t)]
                out["p_node_port"] = ports[0] if ports else None
                out["p_node_count"] = len(ports)
            if blk.lstrip().startswith("raftkv-gateway"):
                ports = [int(p) for t in re.findall(r"targets:\s*\[([^\]]*)\]", blk)
                         for p in re.findall(r":(\d+)", t)]
                out["p_gw_port"] = ports[0] if ports else None
                out["p_gw_count"] = len(ports)
    return out


def _metric_exists(tok, real):
    """指标是否存在：精确匹配，或匹配 histogram 派生后缀（_bucket/_sum/_count）。"""
    if tok in real:
        return True
    for suf in HIST_SUFFIXES:
        if tok.endswith(suf) and tok[:-len(suf)] in real:
            return True
    return False


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
        # 不显式设 GOROOT：Windows 上显式覆盖 GOROOT 会让 go 在子模块（如 experiments
        # 独立 go.mod）里误报 "package X is not in std"；让 go 从 exe 路径自行推断最稳。
        # 但本机 go 工具链在解析标准库时偶发 "package X is not in std"（路径明明存在），
        # 属宿主机 flake。故：①每个脚本用独立干净 GOCACHE（.gocache_smoke）隔离坏安装与
        # 跨模块污染；②构建失败（尤其 not in std）时清空 GOCACHE 重试，最多 3 次，兜住 flake。
        env = dict(os.environ)
        env["GOCACHE"] = os.path.join(ROOT, ".gocache_smoke")
        pkgs = ("kvnode", "gateway")
        for attempt in range(1, 4):
            built = True
            for pkg in pkgs:
                out = os.path.join(self.bin_dir, pkg + ext)
                cmd = [go, "build", "-o", out, "./src/" + pkg]
                p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, env=env)
                if p.returncode != 0:
                    built = False
                    print(f"构建 {pkg} 失败（尝试 {attempt}/3）：\n"
                          f"{(p.stderr or p.stdout)[-1500:]}")
                    break
            if built:
                self._p(f"✓ 构建完成（go={go}）")
                return True
            if attempt < 3:
                shutil.rmtree(env["GOCACHE"], ignore_errors=True)
                os.makedirs(env["GOCACHE"], exist_ok=True)
        return False

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

        # 10. alerts.yml 契约：每条告警引用的「指标名」+「{label=...} 匹配器」都真实存在。
        #     比单纯指标名更深一层——http_responses_total{code=~"5.."} 指标名在、但 code
        #     标签实际没 emit，告警就会永远不命中（静默失效死规则）。
        real_names = set()
        real_labels = {}      # 指标名 -> {真实 emit 的标签名}
        for i in range(3):
            for off in (SM_HTTP[i], KV_HTTP[i]):
                st, body = http("GET", f"http://127.0.0.1:{b+off}/metrics",
                                headers={"Accept": PROM_ACCEPT})
                n, l = _extract_metrics_and_labels(body)
                real_names |= n
                for k, v in l.items():
                    real_labels.setdefault(k, set()).update(v)
        st, body = http("GET", gw + "/metrics", headers={"Accept": PROM_ACCEPT})
        n, l = _extract_metrics_and_labels(body)
        real_names |= n
        for k, v in l.items():
            real_labels.setdefault(k, set()).update(v)

        alerts_path = os.path.join(ROOT, "deploy", "alerts.yml")
        missing_metrics = []
        missing_labels = []
        if os.path.exists(alerts_path):
            text = open(alerts_path, encoding="utf-8").read()
            for ln in text.splitlines():
                if "expr:" not in ln:
                    continue
                expr = ln.split("expr:", 1)[1].strip()
                # (a) 指标名存在性：未暴露即死指标
                for tok in re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", expr):
                    if tok in PROM_BUILTIN or tok in PROM_FUNCS or tok in PROM_LABELS:
                        continue
                    if not self._looks_metric(tok):
                        continue
                    if not _metric_exists(tok, real_names):
                        missing_metrics.append(f"{tok}  ←  {expr}")
                # (b) {label=...} 匹配器契约：被引用指标的标签必须真实 emit
                for m in re.finditer(
                        r"([a-zA-Z_][a-zA-Z0-9_]*)\s*\{([^}]*)\}", expr):
                    metr = m.group(1)
                    if metr in PROM_BUILTIN:    # up 等 Prometheus 内置指标，标签由抓取层注入
                        continue
                    for lm in re.finditer(
                            r"([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=~|!~|=|!=)", m.group(2)):
                        label = lm.group(1)
                        if label in PROM_INJECTED_LABELS:   # job/instance 由 scrape 注入，恒存在
                            continue
                        if label not in real_labels.get(metr, set()):
                            missing_labels.append(f"{metr}{{{label}}}  ←  {expr}")
        missing_metrics = sorted(set(missing_metrics))
        missing_labels = sorted(set(missing_labels))
        ok &= self.check(
            "alerts.yml 指标名契约（无静默失效死指标）",
            len(missing_metrics) == 0,
            f"缺失 {len(missing_metrics)} 个"
            + ("" if not missing_metrics else "：\n      " + "\n      ".join(missing_metrics)))
        ok &= self.check(
            "alerts.yml 标签匹配器契约（引用的标签真实 emit）",
            len(missing_labels) == 0,
            f"缺标签 {len(missing_labels)} 个"
            + ("" if not missing_labels else "：\n      " + "\n      ".join(missing_labels)))

        # 11. deploy 监控拓扑一致性：prometheus.yml 抓取的端口/副本数，必须和
        #     docker-compose.yml 里真实二进制的 -http/-addr 端口、副本数对齐。任一边
        #     改了端口或扩了副本而另一边漏改，监控就会抓盲（静默无数据）。
        topo = _deploy_topo()
        drift = []
        if topo.get("p_node_count") != 6:
            drift.append(f"prometheus nodes 目标数={topo.get('p_node_count')}（应 6）")
        if topo.get("p_node_port") != 9100:
            drift.append(f"prometheus nodes 端口={topo.get('p_node_port')}（应 9100）")
        if topo.get("p_gw_count") != 1:
            drift.append(f"prometheus gateway 目标数={topo.get('p_gw_count')}（应 1）")
        if topo.get("p_gw_port") != 8080:
            drift.append(f"prometheus gateway 端口={topo.get('p_gw_port')}（应 8080）")
        if topo.get("p_node_port") != topo.get("node_port"):
            drift.append(f"节点端口漂移：compose={topo.get('node_port')} vs prom={topo.get('p_node_port')}")
        if topo.get("p_gw_port") != topo.get("gw_port"):
            drift.append(f"网关端口漂移：compose={topo.get('gw_port')} vs prom={topo.get('p_gw_port')}")
        if topo.get("p_node_count") != topo.get("node_count"):
            drift.append(f"节点数漂移：compose={topo.get('node_count')} vs prom={topo.get('p_node_count')}")
        if topo.get("p_gw_count") != topo.get("gw_count"):
            drift.append(f"网关数漂移：compose={topo.get('gw_count')} vs prom={topo.get('p_gw_count')}")
        ok &= self.check("deploy 监控拓扑一致性（prometheus.yml ↔ docker-compose.yml）",
                         len(drift) == 0,
                         "对齐" if not drift else "；".join(drift))
        return ok

    @staticmethod
    def _looks_metric(tok):
        """粗判 token 是否像指标名（非函数/标签/内置）。"""
        return bool(tok) and tok[0].isalpha() and ("_" in tok or tok in PROM_BUILTIN)

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


def _write_smoke_report(smoke, ok, port_base, quiet=False):
    """把全量检查项 + quorum 容错/拓扑一致性证据写成 deploy/smoke_report.json。

    该产物是「真实 TCP 下部署确实可用」的可审计/可展示证据，被 .gitignore 忽略
    （每次跑完重写，不进版本库）。供控制台/CI 直接消费。"""
    def _all(names):
        return all(r["ok"] for r in smoke.results if any(n in r["name"] for n in names))
    report = {
        "ok": ok,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "port_base": port_base,
        "passed": sum(1 for r in smoke.results if r["ok"]),
        "total": len(smoke.results),
        "checks": smoke.results,
        "evidence": {
            "quorum_survived": _all(["kill"]),
            "alerts_metric_contract_ok": _all(["alerts.yml"]),
            "topology_consistent": _all(["拓扑"]),
        },
    }
    report_path = os.path.join(ROOT, "deploy", "smoke_report.json")
    try:
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        if not quiet:
            print(f"✓ 可展示证据已写入 {report_path}")
    except Exception as e:  # noqa: BLE001 - 写证据失败不应影响冒烟结论
        if not quiet:
            print(f"  ! 写 smoke_report.json 失败：{e}")


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
        _write_smoke_report(s, ok, args.port_base, quiet=quiet)
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
