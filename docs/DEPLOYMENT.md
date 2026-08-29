# 部署与定位（Deployment & Positioning）

> 本文件同时回答两个问题：
> 1. **这个项目到底是什么、不是什么**（回应「定位模糊」的批评）。
> 2. **怎么把它真正跑成一个可崩溃恢复的分布式 KV**（真部署化路径）。

---

## 一、项目定位（先说清楚边界）

`raft-kv` 是 **MIT 6.824 Lab4 ShardKV 的一个高完成度实现标本**，外加一套自研的零依赖
gRPC 风格传输层（`src/transport`）与可复用集群框架（`src/cluster`）。它的价值在于：

- **教学/工程标杆**：把 Lab4 的 ShardMaster 分片调度、多 replica group、跨 group 分片迁移、
  快照压缩、日志提交等难点，用一套**进程内 labrpc 网络**跑成了可单测、可演示的闭环。
- **真集群集成测试**：`src/cluster` 能在单进程内拉起「ShardMaster 集群 + 多 group 多副本」
  的完整拓扑，所有既有测试都在真实的多节点交互下验证，而非 mock。

它**不是**：

- ❌ 一个面向生产的 KV 存储（没有鉴权、没有多机房；虽有 per-process 跨机部署形态，
  见 §2.6，但仅供可信网络内演示/压测，不暴露公网）。
- ❌ 一个能直接替换 etcd/Consul 的组件。
- ❌ 一个持续维护的框架库。

一句话：**它是「把 6.824 最难一关做扎实」的标本，不是产品。** 下述部署能力，是把「标本」
推到「可演示真崩溃恢复」的程度，用来证明核心的 raft 持久化与恢复链路是真的能 work，
而不是只活在单元测试的字节比对里。

---

## 二、真部署化能力（v2026-07 起可用）

### 2.1 持久化抽象

`raft.Persister` 已抽象为接口（`SaveRaftState / ReadRaftState / SaveSnapshot / ReadSnapshot`），
提供两种实现：

| 实现 | 用途 | 崩溃恢复 |
| --- | --- | --- |
| `MemoryPersister`（`MakeEmptyPersister()`） | 测试 / 内存演示 | 不支持（进程退出即丢） |
| `FilePersister`（`NewFilePersister(dir)`） | 真部署 | 支持：状态/快照落盘，临时文件 + fsync + 原子 rename |

`FilePersister` 的写入路径是**崩溃安全**的：

```
写临时文件 state.tmp → fsync 临时文件 → 原子 rename 覆盖 state → （可选）fsync 目录
```

进程在任何一步崩溃，磁盘上要么是最新的完整 `state`、要么是上一轮的完整 `state`，
**绝不会留下半截文件**；覆盖成功后也不残留 `.tmp`。

### 2.2 在集群里启用落盘

`src/cluster` 提供可注入的持久化器工厂：

```go
// 内存（默认，向后兼容）
c := cluster.StartCluster(2, 3, 3, 0)

// 真部署：每个节点状态落盘到 ./data/node-<kind>-<g>-<r>
c := cluster.StartClusterWithPersister(
    2, 3, 3, 0,
    cluster.FilePersisterFactory("./data"),
)
```

目录布局（确定性，复用同一目录即可恢复）：

```
data/
  node-sm--1-0/   # ShardMaster 副本 0（g 传 -1）
  node-sm--1-1/   # ShardMaster 副本 1
  node-sm--1-2/   # ShardMaster 副本 2
  node-kv-0-0/    # group0 副本 0
  node-kv-0-1/    # group0 副本 1
  node-kv-0-2/    # group0 副本 2
  node-kv-1-0/    # group1 副本 0
  ...
```

### 2.3 三种启动方式

**(a) demo（开箱即跑，默认内存）**

```bash
cd src/demo
RAFT_KV_DATA_DIR=./data go run .
# 不设 RAFT_KV_DATA_DIR 则为内存演示
```

**(b) HTTP 网关（真部署入口，支持落盘）**

```bash
cd src/gateway
# 用法：gateway <addr> <data-dir>
go run . :8080 ./data
# 不设 data-dir 则为内存集群
```

网关暴露 `PUT/GET/POST /kv/{key}`、`GET /healthz`、`GET /metrics`，
优雅响应 `SIGINT/SIGTERM`（等待在途请求完成后关闭）。

**(c) 直接复用 cluster 包**

作为库 import `raftkv/src/cluster`，按 2.2 注入 `FilePersisterFactory` 即可。

### 2.4 验证崩溃恢复

`src/cluster/cluster_persist_test.go` 的 `TestClusterCrashRecovery` 是端到端证明：

1. 用 `FilePersisterFactory` 起集群，`Put("crashkey","crashval")`；
2. `Cleanup()` 模拟进程崩溃（状态已落盘）；
3. 复用同一 `data` 目录重启集群；
4. 断言 `Get("crashkey") == "crashval"`。

跑它：

```bash
go test ./src/cluster/ -run TestClusterCrashRecovery -v
```

> 注：本机 Windows 环境无 gcc，按项目纪律**不启用 `-race`**；
> 该测试验证的是「持久化 → 重启恢复」链路，不覆盖并发竞态。

### 2.5 跨机部署（真实 TCP）

`src/transport` 是零依赖、gRPC 风格的真实 TCP 传输层（长度前缀帧 + 方法路由 + 错误帧 +
连接池 + optional TLS），此前仅 `gateway` 的 HTTP 面使用。现已把它接到 `cluster` 的
`make_end`，节点间 RPC 走真实网络而非进程内 channel，从而可分布到不同进程/机器。

**核心改动**：
- `raft.ClientEnd` 支持可选 `sendFn`：设置后 `Call` 走自定义发送函数而非内存 `Network`。
- `cluster.StartClusterTCP(nGroups, nReplicas, nSM, maxraftstate, pf, addrs)`：每个节点起一个
  `transport.Server`（注册「方法名 → 类型化 handler」分发），`make_end(name)` 返回走真实 TCP 的
  `ClientEnd`（JSON 序列化 args/reply，完整方法名 `/raft/<Method>`）。
- `cluster.StartClusterFromConfig` / `LoadTCPConfig`：从 JSON 清单加载，`gateway` / `demo` 的
  `--tcp-config` 直接复用。

**配置清单（tcp-config.json）**：

```json
{
  "n_groups": 2, "n_replicas": 3, "n_sm": 3, "max_raft_state": 0,
  "data_dir": "./data-tcp",
  "nodes": [
    {"name": "m0",   "addr": "127.0.0.1:10000"},
    {"name": "m1",   "addr": "127.0.0.1:10001"},
    {"name": "m2",   "addr": "127.0.0.1:10002"},
    {"name": "g0-0", "addr": "127.0.0.1:10010"},
    {"name": "g0-1", "addr": "127.0.0.1:10011"},
    {"name": "g0-2", "addr": "127.0.0.1:10012"},
    {"name": "g1-0", "addr": "127.0.0.1:10020"},
    {"name": "g1-1", "addr": "127.0.0.1:10021"},
    {"name": "g1-2", "addr": "127.0.0.1:10022"}
  ]
}
```

- 命名与内存模式一致：`m<j>` 为 ShardMaster、`g<g>-<r>` 为 ShardKV；`addrs` 必须覆盖全部
  `nSM + nGroups*nReplicas` 个节点，否则 `StartClusterTCP` 直接报错。
- `data_dir` 非空则各节点状态落盘（`FilePersister`），崩溃复用同一目录可恢复。

**启动（多机把 addr 换成各机器 IP 即可）**：

```bash
# 网关跨机模式
go run ./src/gateway --tcp-config tcp-config.json --addr :8080

# 或 demo 跨机演示
go run ./src/demo --tcp-config tcp-config.json
```

**验证**：`src/cluster/cluster_tcp_test.go` 的 `TestClusterTCPTransport` 在单进程内分配多个
localhost 监听地址，用真实 TCP 串起 2-group 集群并跑通 Put/Get/跨组迁移——证明字节确实走
真实网络（而非进程内 channel）。

> TLS：把 `transport.ClientConn.DialTLS` / `Server.ServeTLS` 接入 `newTransportEnd` / `serveNode`
> 即可启用加密传输（证书管理需自行提供），此处未默认开启。

### 2.6 真·跨机：每节点一个独立进程（I21–I25 交付）

§2.5 的 `gateway --tcp-config` 是「单进程内起全部节点、TCP 只在 loopback 上走」——
适合本机验证字节确实走真实网络。要落到「每台机器一个进程」的部署形态，用 **`kvnode`**：
它让**一个 OS 进程只跑地址清单里的一个节点**，全部节点起来后，网关再以 **`-connect`
纯客户端**模式挂到集群前面（本进程不再持有任何节点句柄）。

> **原则**：地址清单（`ClusterTCPConfig`）所有节点共享同一份；每个节点各自带 `-name`
> 只跑清单里的那一个节点；网关用 `-connect` 纯客户端接入。

**多机启动步骤**（以 3 SM + 2 group×3 副本、共 9 节点为例）：

```bash
# 1) 所有机器共用同一份 deploy.json（节点的 addr 填各自机器的可达 IP/端口）
# 2) 在放 ShardMaster 副本的机器上各起一个进程：
machine-A$ go run ./src/kvnode -config deploy.json -name m0
machine-B$ go run ./src/kvnode -config deploy.json -name m1
machine-C$ go run ./src/kvnode -config deploy.json -name m2
# 3) 在放 group 副本的机器上各起一个进程（一台机器可放多个节点）：
machine-D$ go run ./src/kvnode -config deploy.json -name g0-0
machine-D$ go run ./src/kvnode -config deploy.json -name g0-1
machine-E$ go run ./src/kvnode -config deploy.json -name g0-2
# ... g1-0 / g1-1 / g1-2 同理
# 4) 在能连到上述 addr 的任意机器上挂纯客户端网关：
gateway$ go run ./src/gateway -connect deploy.json -addr :8080
# 客户端交互与内存模式完全一致：
curl -X PUT http://<gateway-host>:8080/kv/foo -d 'bar'
```

- `kvnode` 收到 `SIGINT/SIGTERM` 会优雅停止（先关业务状态机 → 关 TCP → 关出向连接）。
- `data_dir` 非空时节点状态落盘（`FilePersister`），崩溃后同目录重启即恢复（与 §2.2 一致）。
- 网关 `-connect` 模式不持本地节点：`/readyz` 退化为直连 ShardMaster 取最新 ConfigNum
  （transport 层 2s 超时），`ConfigNum`/`WaitConfig` 同样走这一远程降级路径，避免空指针/卡死。

### 2.6.1 逐节点诊断端点（`-http`）

跨机形态下每个进程只持有一个节点，而 `gateway` 的 `/debug/shards` 只能看到**它自己**
持有的副本——`-connect` 纯客户端模式下更是一个都看不到。所以哪台机器上的节点落后了、
卡在 `pendingIn` 了、丢了 leader 租约，光靠网关查不出来，只能翻日志。

给 `kvnode` 加 `-http` 即让**该节点自曝状态**，逐台 `curl` 就能巡检：

```bash
machine-D$ go run ./src/kvnode -config deploy.json -name g0-0 -http :9100

# 存活探针（进程在即 200，可直接喂给 k8s livenessProbe / LB 健康检查）
curl -fsS http://machine-D:9100/healthz          # -> ok

# 一行式摘要（人工巡检，无需解析 JSON）
curl -s http://machine-D:9100/
# name=g0-0 kind=shardkv config=3
# raft : role=Leader term=2 commit=41 applied=41 lease=true
# shard: gid=1 leader=true owned=5 pendingIn=[] pendingOut=[] stall=0.0s

# 完整快照 JSON（Raft 状态 + 分片持有/迁移 + diagnostics 不变量自检）
curl -s http://machine-D:9100/status | jq .
```

- 输出结构 `cluster.NodeDiagnostics` 沿用 `gateway.ShardDebugView` 的字段风格
  （PascalCase、可选字段 `omitempty`），运维侧解析逻辑可与 `/debug/shards` 共用。
- `RaftRole` 额外给出角色**文字**形式：`raft.Role` 底层是 `int` 且未实现 `MarshalJSON`，
  裸序列化只有 `0/1/2`，人工判读需查表。
- `RaftCheck` / `ShardCheck` 是 `diagnostics` 包的不变量自检结果（`Score` + `Issues`），
  把「这节点是否健康」从人肉判读裸状态变成可消费信号。
- **排障路径**：集群写不进去时，逐节点 `curl /` 看 ① 有没有 leader（全是 Follower →
  选举失败/网络分区）；② `stall` 是否在涨（分片卡在迁移未决态）；③ `applied` 是否追平
  `commit`（应用层落后）。
- 端点为**只读**、无副作用；`-http` 留空则完全不起 HTTP（默认行为不变）。
- ⚠️ 与业务端口一样**无鉴权**，仅可暴露于可信网络（见 §3）。

**端到端证明**：`scripts/cross_machine_test.py` 在本机拉起 **10 个真实 OS 进程**
（9 个 `kvnode` + 1 个 `gateway -connect`），用 `taskkill /PID /F` 杀掉每组多数派之外的
少数派节点，验证集群在真实进程级故障下仍可读写——复跑稳定。这正是「真·跨机」形态的最小
可复现验证。

---

## 三、已知边界（诚实清单）

- **跨机部署需用真实 TCP 清单驱动**：`demo` / `gateway` 默认仍是进程内集群（基于 labrpc
  内存网络）；要跨机有两种形态——① 单进程 TCP（§2.5 的 `--tcp-config`，loopback 上验证
  字节走真实网络）；② **真·每节点独立进程**（§2.6 的 `kvnode` + `gateway -connect`，适合
  多机部署）。两者都通过把 `src/transport` 接到 `cluster` 的 `make_end` 实现——此能力现已交付。
- **无快照自动触发**：`maxraftstate=0` 时不做日志压缩，恢复靠全量日志重放；
  大状态场景应传入 `maxraftstate>0` 启用 `SaveSnapshot` 压缩。
- **无鉴权 / 限流 / 多租户**：仅为工程标本，切勿直接暴露于公网；跨机部署时请置于可信网络内。
