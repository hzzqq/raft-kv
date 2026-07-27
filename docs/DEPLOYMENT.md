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

- ❌ 一个面向生产的 KV 存储（没有鉴权、没有多机房、没有真正的跨机 gRPC 部署形态）。
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

---

## 三、已知边界（诚实清单）

- **网络层是进程内 labrpc**：`demo` / `gateway` 的「集群」仍在同一进程内，
  不是跨机部署。要跨机需把 `src/transport`（已具备真实 TCP + TLS 能力）接到
  `cluster` 的 `make_end`，替换内存 `ClientEnd`——这是下一步，未在此交付内。
- **无快照自动触发**：`maxraftstate=0` 时不做日志压缩，恢复靠全量日志重放；
  大状态场景应传入 `maxraftstate>0` 启用 `SaveSnapshot` 压缩。
- **无鉴权 / 限流 / 多租户**：仅为工程标本，切勿直接暴露于公网。
