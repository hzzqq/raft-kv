package cluster

import (
	"fmt"
	"testing"
	"time"

	"raftkv/src/raft"
	"raftkv/src/shardkv"
)

// waitCaughtUp 轮询直到第 g 组第 r 个副本的 LastApplied 追上 target（最多 timeout）。
// 返回最终状态与是否追上。
func waitCaughtUp(c *Cluster, g, r, target int, timeout time.Duration) (raft.RaftStatus, bool) {
	deadline := time.Now().Add(timeout)
	var st raft.RaftStatus
	for time.Now().Before(deadline) {
		st = c.KVRaftStatus(g, r)
		if st.LastApplied >= target {
			return st, true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return st, false
}

// leaderOf 返回第 g 组当前 leader 的副本下标（找不到返回 -1）。
func leaderOf(c *Cluster, g, nReplicas int) int {
	for r := 0; r < nReplicas; r++ {
		if c.KVRaftStatus(g, r).Role == raft.Leader {
			return r
		}
	}
	return -1
}

// TestClusterFollowerSnapshotCatchUp（I12）：验证「掉线副本经 InstallSnapshot 追赶」的
// 集群级链路。步骤：单 group 三副本 + 小 maxraftstate（写入必触发快照与日志截断）→
// 分区掉一个非 leader 副本 → 灌入足量写（leader 端快照化、被截断的日志段无法用
// AppendEntries 回放）→ 愈合分区 → 断言掉线副本 LastApplied 追平且数据可读。
// 若 InstallSnapshot 链路（发送/接收/CondInstallSnapshot/状态机重置）有任何断裂，
// 该副本将永远追不上，测试超时失败。
func TestClusterFollowerSnapshotCatchUp(t *testing.T) {
	const (
		g         = 0
		nReplicas = 3
		writes    = 120
	)
	// maxraftstate=256：每条写入几十字节 raft 状态，120 条写入期间必多次触发快照。
	c := StartCluster(1, nReplicas, 3, 256)
	defer c.Cleanup()

	c.Join(g)
	c.WaitConfig(g, 0, 1)
	ck := c.Clerk()
	ck.Put("seed", "v0")

	// 选一个非 leader 副本掉线（掉 leader 会引发换主，那是 I13 的场景）。
	lead := leaderOf(c, g, nReplicas)
	if lead < 0 {
		t.Fatalf("no leader in group %d", g)
	}
	victim := (lead + 1) % nReplicas
	c.EnableKV(g, victim, false)

	for i := 0; i < writes; i++ {
		ck.Put(fmt.Sprintf("snapk-%d", i), fmt.Sprintf("val-%d-%s", i, "0123456789abcdef"))
	}
	// 记录 leader 应用进度作为追赶目标（此时 victim 远落后且日志已被截断）。
	target := c.KVRaftStatus(g, leaderOf(c, g, nReplicas)).LastApplied
	before := c.KVRaftStatus(g, victim).LastApplied
	if before >= target {
		t.Fatalf("victim 未真正落后: before=%d target=%d（分区未生效？）", before, target)
	}

	c.EnableKV(g, victim, true)
	st, ok := waitCaughtUp(c, g, victim, target, 20*time.Second)
	if !ok {
		t.Fatalf("victim r%d 愈合后未追平: LastApplied=%d < target=%d (Role=%v Term=%d)",
			victim, st.LastApplied, target, st.Role, st.Term)
	}
	// 数据面复核：新旧键都可读。
	if got := ck.Get("snapk-0"); got != "val-0-0123456789abcdef" {
		t.Fatalf("snapk-0 = %q", got)
	}
	ck.Put("after-heal", "yes")
	if got := ck.Get("after-heal"); got != "yes" {
		t.Fatalf("after-heal = %q", got)
	}
}

// TestClusterPartitionKVNoSplitBrain（I149）：验证 PartitionKV 造出的「真脑裂」下不会双写。
// 与 TestClusterLeaderPartitionRecovery 的差别是这里用真网络分裂（少数派节点仍存活、
// 少数派内部仍互通）而非整节点掉线，因此旧 leader 会保持 Leader 角色——正是双写风险
// 最大的场景。断言：少数派 CommitIndex 冻结（拿不到 quorum）、多数派照常提交、
// 愈合后少数派降级追平，且分区期间打进少数派日志的未提交写不会出现在最终状态里。
func TestClusterPartitionKVNoSplitBrain(t *testing.T) {
	const (
		g         = 0
		nReplicas = 5
	)
	c := StartCluster(1, nReplicas, 3, -1)
	defer c.Cleanup()

	c.Join(g)
	c.WaitConfig(g, 0, 1)
	ck := c.Clerk()
	ck.Put("k-before", "v1")

	oldLead := leaderOf(c, g, nReplicas)
	if oldLead < 0 {
		t.Fatalf("no leader in group %d", g)
	}
	oldSt := c.KVRaftStatus(g, oldLead)

	// 2+3 分裂：旧 leader 落在少数派。
	minority := []int{oldLead, (oldLead + 1) % nReplicas}
	majority := []int{}
	for r := 0; r < nReplicas; r++ {
		if r != minority[0] && r != minority[1] {
			majority = append(majority, r)
		}
	}
	c.PartitionKV(g, minority, majority)

	// 多数派换主并继续服务。
	newLead := -1
	for wait := time.Now().Add(10 * time.Second); time.Now().Before(wait) && newLead < 0; {
		for _, r := range majority {
			if st := c.KVRaftStatus(g, r); st.Role == raft.Leader && st.Term > oldSt.Term {
				newLead = r
				break
			}
		}
		if newLead < 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if newLead < 0 {
		t.Fatalf("多数派未选出新 leader（旧 leader=%d term=%d）", oldLead, oldSt.Term)
	}
	if err := ck.PutE("k-during", "v2"); err != shardkv.OK {
		t.Fatalf("多数派应可写，实得 err=%v", err)
	}

	// 少数派：旧 leader 仍自认 leader，但提交进度冻结（拿不到 quorum）。
	if st := c.KVRaftStatus(g, oldLead); st.CommitIndex > oldSt.CommitIndex {
		t.Fatalf("少数派 leader 竟推进了提交: %d -> %d（quorum 判定有 bug）",
			oldSt.CommitIndex, st.CommitIndex)
	}

	// 愈合：旧 leader 必须降级并追平，且分区期间少数派未提交的写不得出现。
	c.PartitionKV(g)
	target := c.KVRaftStatus(g, newLead).LastApplied
	if st, ok := waitCaughtUp(c, g, oldLead, target, 15*time.Second); !ok {
		t.Fatalf("旧 leader r%d 愈合后未追平: LastApplied=%d < %d", oldLead, st.LastApplied, target)
	}
	if got := ck.Get("k-during"); got != "v2" {
		t.Fatalf("愈合后分区期间数据丢失: k-during=%q", got)
	}
	if got := ck.Get("k-before"); got != "v1" {
		t.Fatalf("愈合后历史数据丢失: k-before=%q", got)
	}
}

// TestClusterLeaderPartitionRecovery（I13）：验证「leader 被分区 → 剩余多数派换主继续
// 服务 → 旧 leader 愈合后降级追平」的网络分区恢复链路。若旧 leader 愈合后仍自认 leader
// （任期对齐失败）或数据回滚，断言会失败。
func TestClusterLeaderPartitionRecovery(t *testing.T) {
	const (
		g         = 0
		nReplicas = 3
	)
	c := StartCluster(1, nReplicas, 3, -1)
	defer c.Cleanup()

	c.Join(g)
	c.WaitConfig(g, 0, 1)
	ck := c.Clerk()
	ck.Put("k-before", "v1")

	oldLead := leaderOf(c, g, nReplicas)
	if oldLead < 0 {
		t.Fatalf("no leader in group %d", g)
	}
	c.EnableKV(g, oldLead, false)

	// 剩余两副本应换主并继续服务（clerk 自动换端点重试）。
	ck.Put("k-during", "v2")
	if got := ck.Get("k-during"); got != "v2" {
		t.Fatalf("分区期间写入失败: k-during=%q", got)
	}
	// 找新 leader 必须排除 oldLead：被分区的旧 leader 收不到更高任期的心跳，
	// 本地 GetState 仍会自称 leader（这正是分区的语义，不是 bug）。
	newLead := -1
	for wait := time.Now().Add(10 * time.Second); time.Now().Before(wait) && newLead < 0; {
		for r := 0; r < nReplicas; r++ {
			if r != oldLead && c.KVRaftStatus(g, r).Role == raft.Leader {
				newLead = r
				break
			}
		}
		if newLead < 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if newLead < 0 {
		t.Fatalf("多数派未选出新 leader: old=%d", oldLead)
	}

	// 愈合：旧 leader 必须降级为 follower 并追平数据。
	c.EnableKV(g, oldLead, true)
	target := c.KVRaftStatus(g, newLead).LastApplied
	st, ok := waitCaughtUp(c, g, oldLead, target, 15*time.Second)
	if !ok {
		t.Fatalf("旧 leader r%d 愈合后未追平: LastApplied=%d < %d", oldLead, st.LastApplied, target)
	}
	// 给角色收敛留一个心跳周期，然后断言旧 leader 已不再自认 leader。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c.KVRaftStatus(g, oldLead).Role != raft.Leader {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if role := c.KVRaftStatus(g, oldLead).Role; role == raft.Leader {
		t.Fatalf("旧 leader r%d 愈合后仍自认 leader（任期对齐失败）", oldLead)
	}
	ck.Put("k-after", "v3")
	if got := ck.Get("k-after"); got != "v3" {
		t.Fatalf("愈合后写入失败: k-after=%q", got)
	}
	if got := ck.Get("k-before"); got != "v1" {
		t.Fatalf("历史数据丢失: k-before=%q", got)
	}
}
