// raft_witness_test.go —— Witness（见证者）副本的 raft 层不变量（I189）。
//
// witness 语义（CockroachDB 式「存储节省型」witness）：
//   1. 持有完整 Raft 日志、参与投票与提交 quorum；
//   2. 但**不持有状态机数据、永不被选为 leader**（applier 只推进 lastApplied，
//      不把 ApplyMsg 投递给上层状态机；ticker 不发起选举；TimeoutNow 拒绝夺权）。
// 这样「2 投票 + 1 witness」（peers=3，quorum=2）用 2 份存储获得 3 副本容错。
package raft

import (
	"testing"
	"time"
)

// makeConfigWitness 同 makeConfig，但 witnesses 集合内的节点用 MakeWitness 构造，
// 其余用 Make。测试框架其余方法（connectAll/addServerHandler/monitor/kill 等）对
// witness 与普通节点一视同仁——witness 仍是 *Raft 实例。
// 可选参数 initialVoters 在节点启动、参与选举前设定初始投票成员集合（I192 动态重配置
// 测试用）：传入如 []int{0,1} 可让下标 2 的 witness 初始「不投票」，从而演示运行时 Join。
func makeConfigWitness(t testing.TB, n int, witnesses map[int]bool, initialVoters ...[]int) *config {
	net := MakeNetwork()
	cfg := &config{net: net, n: n, t: t}
	cfg.rafts = make([]*Raft, n)
	cfg.endnames = make([][]*ClientEnd, n)
	cfg.applyCh = make([]chan ApplyMsg, n)
	cfg.persisters = make([]Persister, n)
	cfg.logs = make([][]interface{}, n)
	cfg.connected = make([]bool, n)

	for i := 0; i < n; i++ {
		cfg.connected[i] = true
	}
	for i := 0; i < n; i++ {
		cfg.endnames[i] = make([]*ClientEnd, n)
		for j := 0; j < n; j++ {
			cfg.endnames[i][j] = net.MakeEnd(i*n+j, i)
		}
	}
	initCfg := []int(nil)
	if len(initialVoters) > 0 {
		initCfg = initialVoters[0]
	}
	for i := 0; i < n; i++ {
		cfg.applyCh[i] = make(chan ApplyMsg, 4000)
		go cfg.monitor(i, cfg.applyCh[i])
		cfg.persisters[i] = MakeEmptyPersister()
		var rf *Raft
		if witnesses[i] {
			rf = MakeWitness(cfg.endnames[i], i, cfg.persisters[i], cfg.applyCh[i])
		} else {
			rf = Make(cfg.endnames[i], i, cfg.persisters[i], cfg.applyCh[i])
		}
		if initCfg != nil {
			// 初始投票配置必须在选举前设定（仅 I192 测试场景；正常运行中变更走
			// ProposeConfChange）。所有节点设定为同一集合，保证初始 quorum 一致。
			rf.SetInitialConfig(initCfg)
		}
		cfg.rafts[i] = rf
		cfg.connectAll(i)
		cfg.addServerHandler(i, rf)
	}
	return cfg
}

// TestWitnessReplicatesButDoesNotApply 验证 witness 复制完整日志但不把任何命令应用
// 到状态机——这是「存储节省」的根本：witness 的本地状态机恒空。
func TestWitnessReplicatesButDoesNotApply(t *testing.T) {
	const w = 2 // 第 2 个节点为 witness
	cfg := makeConfigWitness(t, 3, map[int]bool{w: true})
	defer cfg.cleanup()

	idx := cfg.one(101, 2) // 2 个投票副本提交即可（witness 不计入 apply 集合）

	// 给复制一点时间追上
	time.Sleep(500 * time.Millisecond)

	st := cfg.rafts[w].Status()
	if st.LastLogIndex < idx {
		t.Fatalf("witness 未复制完整日志：LastLogIndex=%d < 已提交索引 %d", st.LastLogIndex, idx)
	}
	// 核心不变量：witness 的状态机（cfg.logs[w]，由 monitor 从 applyCh 落盘）必须为空——
	// 因为 witness 的 applier 只推进 lastApplied、从不把 ApplyMsg 投递给上层。
	cfg.mu.Lock()
	witnessApplied := len(cfg.logs[w])
	voterApplied := len(cfg.logs[0])
	cfg.mu.Unlock()
	if witnessApplied != 0 {
		t.Fatalf("witness 不应向状态机 apply 任何命令，但 cfg.logs[w] 有 %d 条", witnessApplied)
	}
	// witness 的 applier 仍推进 lastApplied（仅不投递），不得卡在 0。
	if cfg.rafts[w].LastApplied() < idx {
		t.Fatalf("witness 的 lastApplied 未推进：%d < %d", cfg.rafts[w].LastApplied(), idx)
	}
	// 对照：投票副本确实把命令应用进状态机
	if voterApplied < idx {
		t.Fatalf("投票副本应已应用 >=%d 条命令到状态机，实际 %d", idx, voterApplied)
	}
}

// TestWitnessNeverLeads 验证 witness 全程不参选、不夺权（LeaderElections 恒 0）。
func TestWitnessNeverLeads(t *testing.T) {
	const w = 2
	cfg := makeConfigWitness(t, 3, map[int]bool{w: true})
	defer cfg.cleanup()

	// 先触发一次正常选举
	l, _ := cfg.checkOneLeader()
	if l < 0 {
		t.Fatalf("初始未选出 leader")
	}
	if l == w {
		t.Fatalf("witness 竟然当选初始 leader")
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := cfg.rafts[w].Status()
		if st.Role == Leader {
			t.Fatalf("witness 在运行期成为 Leader（Role=Leader）")
		}
		time.Sleep(30 * time.Millisecond)
	}
	if cfg.rafts[w].Status().LeaderElections != 0 {
		t.Fatalf("witness LeaderElections=%d，应恒为 0（永不被选为 leader）",
			cfg.rafts[w].Status().LeaderElections)
	}
}

// TestWitnessQuorumBenefit 验证「2 投票 + 1 witness」的容错收益：kill 一个投票副本后，
// 剩 1 投票 + 1 witness 仍达 quorum=2，可继续提交；而纯 2 投票（quorum=2）kill 一个后
// 完全不可提交——证明 witness 用 2 份存储换来了 3 副本容错。
func TestWitnessQuorumBenefit(t *testing.T) {
	// (a) 2 投票 + 1 witness：kill 1 投票后仍可提交
	const w = 2
	cfg := makeConfigWitness(t, 3, map[int]bool{w: true})
	defer cfg.cleanup()
	l, _ := cfg.checkOneLeader()
	if l < 0 {
		t.Fatalf("witness 组未选出 leader")
	}
	voter := (l + 1) % 3 // 一个投票副本（非 leader、非 witness）
	if voter == w {
		voter = (l + 2) % 3
	}
	cfg.kill(voter) // 杀掉一个投票副本，剩 leader(投票) + witness
	idx := cfg.start1(202)
	// 等待 surviving 投票副本应用该命令（quorum=2 由 leader+witness 满足）
	to := time.Now().Add(6 * time.Second)
	committed := false
	for time.Now().Before(to) {
		if cfg.rafts[l].LastApplied() >= idx {
			committed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !committed {
		t.Fatalf("kill 1 投票副本后，leader+witness 应仍能提交（quorum=2），但命令 %d 未提交", idx)
	}

	// (b) 对照：纯 2 投票（quorum=2）kill 1 后不可提交
	cfg2 := makeConfig(t, 2)
	defer cfg2.cleanup()
	l2, _ := cfg2.checkOneLeader()
	if l2 < 0 {
		t.Fatalf("纯2组未选出 leader")
	}
	f2 := (l2 + 1) % 2
	cfg2.kill(f2)
	idx2 := cfg2.start1(303)
	time.Sleep(2 * ElectionTimeoutMax)
	if cfg2.rafts[l2].LastApplied() >= idx2 {
		t.Fatalf("纯 2 投票集群 kill 1 后竟能提交（quorum 应不足）——与 witness 收益对比失效")
	}
}
