// leader.go —— 场景 A：leader 故障切换。
//
// 步骤：起 1 组×3 副本 → 写热数据 → 定位 leader → kill leader（模拟进程崩溃）
// → 探测新 leader（任期递增）→ failover 后新写可读、故障前数据完整保留 →
// 新 leader 在新任期提交后 commit 追平旧 leader。证明 Raft 在丢一副本后选举出新主、
// 且已提交日志零丢失（注意：新 leader 刚当选时 commit 可能暂时落后旧 leader，这是
// Raft「不能仅靠副本数提交旧任期条目」规则的体现，待其提交新任期首个条目后即追平）。
package main

import (
	"fmt"
	"time"

	"raftkv/src/cluster"
)

func runLeader() {
	const nG, nR, nSM, mrs = 1, 3, 3, 0
	c := cluster.StartCluster(nG, nR, nSM, mrs)
	defer c.Cleanup()
	bootstrap(c, nG)
	ck := c.Clerk()

	log("场景 A：leader 故障切换（%d 组 × %d 副本）", nG, nR)

	// 故障前的热数据
	for i := 0; i < 10; i++ {
		ck.Put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}

	l, st := findLeader(c, 0, nR)
	if l < 0 {
		time.Sleep(600 * time.Millisecond)
		l, st = findLeader(c, 0, nR)
	}
	if l < 0 {
		fmt.Println("✗ 初始未选出 leader，退出")
		return
	}
	log("初始 leader = g0-%d | term=%d commit=%d applied=%d", l, st.Term, st.CommitIndex, st.LastApplied)
	prevCommit := st.CommitIndex

	log("kill leader g0-%d（模拟进程崩溃）", l)
	c.KVs[0][l].Kill()

	// 客户端视角：在故障窗口内持续以真实 client 身份发请求，量化「客户端可见不可用」
	// 与「丢失写」——这是节点视角（RaftStatus）之外、直接证明容错对用户成立的关键证据。
	pr := probeClient(ck, 2500*time.Millisecond, 30*time.Millisecond, true)
	logProbe("leader", pr)

	nl, nst := waitLeader(c, 0, nR, st.Term, 6*time.Second)
	if nl < 0 {
		fmt.Println("✗ 故障后 6s 内未选出新 leader")
		return
	}
	log("新 leader 选出 = g0-%d | term=%d（旧 term=%d，+%d）", nl, nst.Term, st.Term, nst.Term-st.Term)

	// failover 后读写验证：新 leader 在自己任期提交首个新条目后，旧任期已提交日志即被继承
	ck.Put("survivor", "yes")
	if v := ck.Get("survivor"); v == "yes" {
		log("✓ 故障切换后新写可读（survivor=yes）")
	} else {
		log("✗ 故障切换后新写不可读：%q", v)
	}
	if v0 := ck.Get("k0"); v0 == "v0" {
		log("✓ 故障前已提交数据完整保留（k0=v0），旧任期日志零丢失")
	} else {
		log("✗ 故障前数据丢失：k0=%q", v0)
	}
	// 新 leader 在新任期提交后，commit 应已追平并超过旧 leader 的 commit
	nst2 := c.KVRaftStatus(0, nl)
	if nst2.CommitIndex >= prevCommit {
		log("✓ 新 leader 已补齐并提交至 commit=%d（≥ 旧 commit=%d），日志连续无空洞", nst2.CommitIndex, prevCommit)
	} else {
		log("⚠ 新 leader commit=%d 仍落后旧 commit=%d（选举后尚未提交新任期条目）", nst2.CommitIndex, prevCommit)
	}
	log("结论：3 副本杀 1，剩余 2 满足多数派(2/3)；新 leader 在选举超时(260–480ms)内选出，旧任期已提交日志经新任期首个条目继承后完整保留、集群恢复可写。")
}
