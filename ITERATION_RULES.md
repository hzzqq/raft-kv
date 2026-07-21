# raft-kv 自主迭代规则（self-driving-dev）

> 本文件由 agent 根据 `Handoff-raft-kv-20260719.md`（cycle 1–13 交接）自行制定，
> 作为后续自主迭代（cycle 14+）的过程纪律。用户 2026-07-20 指令：
> 「读取文件夹，根据交接文档，自己制定迭代过程的规则，自主迭代10轮，做完了把代码传到GitHub」。

## 〇、本轮批次（cycle 39–48，2026-07-20 续跑）
- 基线：cycle 38 已落地全栈（cluster/demo/gateway/kvcli/ReadIndex/metrics/docs/CI），全量
  `go test ./...` 绿、覆盖率 74.2%，`origin/master` 同步于 tip `58e36a7`。
- 本批 10 轮聚焦：
  1. **3+ group / ReMigration 冻结根因自愈**（最高优先级）：加「卡死看门狗」重拉 + 可 skip 的
     3-group churn 测试转为可追踪项；
  2. 补 shardkv / kvcli 覆盖率缺口；
  3. 扩展 CI `-race` 覆盖（Concurrent / SnapshotChurn）；
  4. 打磨可观测性（卡滞计数 / `/debug/shards` 卡滞时长）；
  5. 刷新 README + 交接文档；
  6. 全量验收后按授权 `git push origin master`。
- **push 授权已确认**：10 轮完成后执行普通快进 `git push origin master`（不 `--force`）。
- 纪律延续 §一~§五；绿条优先——任何会破坏既有绿条的修复先回退换方向。

## 〇、本轮批次（cycle 68–87，2026-07-21「照刚刚的迭代20次」）
- 基线：cycle 67 tip `b453452`（快照看门狗 / 迁移 RPC 退避 / GC 守卫 / 网关 `/debug/configs` /
  迁移延迟直方图 / kvraft flaky 审计 / 运维 runbook），全量 `go test ./...` 绿。
- 本批 20 轮聚焦（新增功能 / 完善旧功能 / 解决显性与隐性问题）：
  1. raft leader lease（`HasLeaderLease`）+ ShardKV `Get` ReadIndex 快读复活（I1）；
  2. 配置号幂等 `installedCfgNum` + 过期传输安全丢弃（I2/I3），修复 ReMigration 快速 churn 冻结回归；
  3. `pollConfig` 去重（`proposedConfigNum`）避免重复提议（I4/I8）；
  4. 配置变更/快照指标（`config_changes` / `config_num` / 快照压缩含 `NewConfig`）（I5/I9）；
  5. 迁移分片字节观测 `shard_bytes` / `shard_bytes_overflow` + 每跳延迟 `send_shard_latency`（I10/I15）；
  6. shardmaster 最小搬动 `rebalance` + 输入校验 `ErrInvalid`；
  7. 网关并发限流 429 + 优雅退出 `Shutdown` + `/debug/groups`（I 网关层）；
  8. kvraft 客户端会话 GC（TTL）；
  9. metrics 新增 `Gauge` 类型；
  10. 混沌测试 `TestChaosLeaderKillDuringMigration`（I16）/ `TestChaosLongRun`（I18）+ CI `chaos-race`（I17）/ `chaos-long`（I18）job；
  11. README / runbook / 交接文档同步。
- **push 授权已确认**（沿用 §一.5）：20 轮完成后执行普通快进 `git push origin master`（不 `--force`、不 `rm -rf`）。
- 纪律延续 §一~§五；绿条优先——任何会破坏既有绿条的修复先回退换方向。

## 一、安全红线（不可逾越）
1. 每次改动必须先 `go build ./...` + `go vet` + 相关包测试通过，方可提交；绝不把项目留在损坏/不编译状态。
2. 验收不过：先尝试一次修复；一次修复仍失败 → 回滚本次改动（`git checkout -- <files>`），记录原因，换下一个需求，不空转。
3. 禁止 `rm -rf`、`git push --force`、强制推送、删除项目外文件等不可逆/破坏性命令。
4. 跨平台一致性：Go 项目统一 LF（`.gitattributes`/`.editorconfig` 已锁定）；若 `git status` 仅报 CRLF 差异，先 `git config core.autocrlf false` 再 `git checkout -- .` 还原，避免无关换行混入提交。
5. **本次特例（用户 2026-07-20 显式授权）**：全部 10 轮完成后执行 `git push origin master`（仅普通推送、不 `--force`）。若远端不可达或非快进，则报告并停止，绝不强推。

## 二、既有代码约定（来自交接 §5，长期有效）
- `InstallShard` 必须深拷贝 `MigrateData` 再入本组状态（防与 Raft `persist()` 的 gob 编码并发读写同一 map）。
- `installSnapshot` 在调用方已持 `kv.mu` 下执行，自身**不可再**加锁（防嵌套死锁）。
- 客户端写用独立 `Clerk`（独立 `clientId`+`seq`），避免跨 key 的 `seq` 串扰导致去重把写入当陈旧重放丢弃。
- `Kill()` 必须关闭 `killCh`，让 applier 及时退出，否则每实例泄漏一个 goroutine。
- metrics 接入只做纯增量原子操作（`atomic.AddUint64`），不可在热路径加锁或分配，违背「零开销可观测性」。
- 绿条纪律：绝不提交会破坏既有绿条的修复（cycle 9 的 reconcile、cycle 13 的 Clerk 缓存均因此回退）。
- 迁移 liveness 看门狗（本轮新增）：`pollConfig` 在 `hasPending` 持续 > 阈值（3s）时，对每个卡滞分片
  以最新 ShardMaster 配置重算 owner 并**重拉取（bump `fetchEpoch` 让陈旧 fetcher 自退）**；仅卡死时触发，
  不扰正常快路径。看门狗每触发一次 `Metrics.Counter("config_stalls").Inc()`，由 `/metrics` 暴露。

## 三、本地环境约束
- 工具链：`C:/Users/Administrator/.workbuddy/binaries/go/go/bin/go.exe`（go1.22.5），绝对 GOPATH/GOCACHE（见 `run-tests.sh`）。
- 本地无 gcc，无法跑 `go test -race`；用高频 churn + 多轮循环测试 + 并发 map 检测器暴露竞态。`-race` 仅 GitHub Actions 跑（runner 有 gcc）。

## 四、需求优先级（来自交接 §6/§7）
1. 3+ group 整体再平衡卡死（最高优先级未解风险）——修前先确认不破坏绿条。
2. cycle 14–20 计划：cluster 包 → demo → HTTP gateway → kvcli → ReadIndex → 文档。
3. 工程化：Clerk 单 RPC 超时、CI -race 扩展、metrics 暴露。

## 五、每轮记录
- 完成后 `cycle += 1`，向 `.workbuddy/self-driving/state.json` 的 `log` 追加 `{task_id, files, validation, score, ts}`。
- 评分 `score` 0–100 为真实质量收益；连续两轮 <10 或同 `task_id` 连续 3 次无进展 → 自然收尾/换方向。
- 提交信息格式：`self-driving dev [cycle N/48]: <task 简述>`（N 为绝对轮次，本轮 39–48）。
- 跨调用续跑：先读 `state.json`；若 `paused=true` 直接停下汇报。
