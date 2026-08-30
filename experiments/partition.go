// partition.go —— 场景 B：网络分区脑裂（2+3 分裂）。
//
// 要证明的三条不变量：
//  1. 多数派（3/5）继续选出新 leader 并正常读写；
//  2. 少数派（2/5）里的旧 leader 仍自认 leader、客户端也确实连得上它，但因拿不到
//     quorum 而无法提交——客户端拿到的是超时错误，绝不会拿到"写成功"；
//  3. 愈合后少数派自动降级、追平数据；分区期间被塞进少数派日志的未提交写被覆盖，
//     不会出现在最终状态里（无脑裂双写、无已确认写丢失）。
//
// 与 EnableKV(false) 的区别：这里用 PartitionKV 做真网络分裂，两个少数派节点仍存活、
// 彼此仍能通信，旧 leader 因此保持 Leader 角色——这正是双写风险最大的场景。
package main

import (
	"fmt"
	"time"

	"raftkv/src/cluster"
	"raftkv/src/raft"
	"raftkv/src/shardkv"
)

// probeMinorityWrite 绕过 Clerk 的自动重定向，直接给少数派 leader 发一次写请求，
// 让「客户端连得上但写不进」变成可观测结果。返回服务端应答的 Err 与耗时。
// 端点 owner 取 9500（未注册 → 不受分区限制），模拟一个网络上没被隔离的客户端。
func probeMinorityWrite(c *cluster.Cluster, g, r int, key, val string) (shardkv.Err, time.Duration) {
	const endName, owner = 7777, 9500
	end := c.Net.MakeEnd(endName, owner)
	c.Net.Connect(endName, 1000+g*100+r)

	args := &shardkv.PutAppendArgs{
		Key: key, Value: val, OpType: "Put",
		ClientId: 990001, Seq: 1,
	}
	reply := &shardkv.PutAppendReply{}
	t0 := time.Now()
	if ok := end.Call("ShardKV.PutAppend", args, reply); !ok {
		return "RPC-unreachable", time.Since(t0)
	}
	return reply.Err, time.Since(t0)
}

func runPartition() {
	const (
		nG, nR, nSM, mrs = 1, 5, 3, -1
		g                = 0
	)
	resetClock()
	log("场景 B：网络分区脑裂（1 组 × %d 副本，2+3 分裂）", nR)

	c := cluster.StartCluster(nG, nR, nSM, mrs)
	defer c.Cleanup()
	bootstrap(c, nG)

	ck := c.Clerk()
	ck.Put("acked-before", "yes")
	for i := 0; i < 5; i++ {
		ck.Put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}

	oldLead, oldSt := findLeader(c, g, nR)
	if oldLead < 0 {
		log("✗ 没找到初始 leader，实验中止")
		return
	}
	log("分区前 leader = g%d-%d | term=%d commit=%d", g, oldLead, oldSt.Term, oldSt.CommitIndex)

	// 2+3 分裂：旧 leader 落在少数派，少数派内部仍互通。
	minority := []int{oldLead, (oldLead + 1) % nR}
	majority := []int{}
	for r := 0; r < nR; r++ {
		if r != minority[0] && r != minority[1] {
			majority = append(majority, r)
		}
	}
	c.PartitionKV(g, minority, majority)
	log("网络分裂生效：少数派 %v（含旧 leader）| 多数派 %v", minority, majority)

	// ---- 不变量 1：多数派换主并继续服务 ----
	nl, nst := waitLeaderIn(c, g, majority, oldSt.Term, 8*time.Second)
	if nl < 0 {
		log("✗ 多数派未在 8s 内选出新 leader")
		return
	}
	log("✓ 多数派选出新 leader = g%d-%d | term=%d（旧 term=%d）", g, nl, nst.Term, oldSt.Term)

	if err := ck.PutE("during-partition", "ok"); err != shardkv.OK {
		log("✗ 多数派应可写，实得 err=%v", err)
		return
	}
	if got, err := ck.GetE("during-partition"); err != shardkv.OK || got != "ok" {
		log("✗ 多数派读回失败: got=%q err=%v", got, err)
		return
	}
	log("✓ 多数派读写正常（during-partition=ok），客户端未感知故障")

	// ---- 不变量 2：少数派连得上但写不进 ----
	mst := c.KVRaftStatus(g, oldLead)
	log("少数派旧 leader 自述: role=%v term=%d commit=%d leaderLease=%v",
		mst.Role, mst.Term, mst.CommitIndex, mst.HasLeaderLease)
	perr, cost := probeMinorityWrite(c, g, oldLead, "ghost-write", "should-never-commit")
	if perr == shardkv.OK {
		log("✗ 少数派竟然写成功了（脑裂双写！）")
		return
	}
	log("✓ 客户端直连少数派 leader 写入被拒: err=%v（耗时 %.1fs），未拿到任何成功确认",
		perr, cost.Seconds())

	after := c.KVRaftStatus(g, oldLead)
	if after.CommitIndex > oldSt.CommitIndex {
		log("✗ 少数派推进了提交 %d -> %d（quorum 判定有 bug）", oldSt.CommitIndex, after.CommitIndex)
		return
	}
	majCommit := c.KVRaftStatus(g, nl).CommitIndex
	log("✓ 少数派提交进度冻结在 commit=%d，同期多数派已推进到 commit=%d",
		after.CommitIndex, majCommit)

	// ---- 不变量 3：愈合后收敛，未提交写被覆盖 ----
	c.PartitionKV(g)
	log("网络愈合，等待少数派降级追平…")
	target := c.KVRaftStatus(g, nl).LastApplied
	if !waitCaughtUp(c, g, oldLead, target, 15*time.Second) {
		st := c.KVRaftStatus(g, oldLead)
		log("✗ 旧 leader 未追平: applied=%d < %d", st.LastApplied, target)
		return
	}
	healed := c.KVRaftStatus(g, oldLead)
	if healed.Role == raft.Leader {
		log("✗ 旧 leader 愈合后仍自认 leader（任期未对齐）")
		return
	}
	log("✓ 旧 leader 已降级为 %v、任期对齐到 term=%d、applied=%d（追平多数派）",
		healed.Role, healed.Term, healed.LastApplied)

	if got := ck.Get("ghost-write"); got != "" {
		log("✗ 少数派的未提交写竟进入最终状态: ghost-write=%q", got)
		return
	}
	log("✓ 少数派未提交的 ghost-write 已被新 leader 日志覆盖，最终状态里不存在")

	if got := ck.Get("acked-before"); got != "yes" {
		log("✗ 分区前已确认的写丢失: acked-before=%q", got)
		return
	}
	if got := ck.Get("during-partition"); got != "ok" {
		log("✗ 分区期间多数派已确认的写丢失: during-partition=%q", got)
		return
	}
	log("✓ 所有已确认写入全部存活（分区前 + 分区中），零丢失")
	log("场景 B 结论：quorum 而非连通性决定可写性 —— 少数派活着也写不进，故不会脑裂双写")
}
