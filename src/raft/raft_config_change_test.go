// raft_config_change_test.go —— Witness 动态重配置 Join/Leave（I192）。
//
// 验证「2 数据 + 1 witness」集群可在不停服的情况下，运行时把 witness 加入/移出投票
// 集合，且 quorum 计票随之实时变化（Ongaro 论文 §6 单服变更）。这是 witness 设计的
// 运维闭环：今天加一个廉价 witness 把 2 副本组升级成「可容忍单数据副本故障」的 3 路
// 容错，明天移除又回到省存储的 2 副本形态——全程无需停集群、无需重建。
package raft

import (
	"sort"
	"testing"
	"time"
)

// waitVoterConfig 轮询直到节点投票配置等于 want（按集合比较，忽略顺序），超时返回 false。
func waitVoterConfig(t *testing.T, rf *Raft, want []int, timeout time.Duration) bool {
	w := append([]int(nil), want...)
	sort.Ints(w)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got := rf.VoterConfig()
		g := append([]int(nil), got...)
		sort.Ints(g)
		if len(g) == len(w) {
			ok := true
			for i := range w {
				if g[i] != w[i] {
					ok = false
					break
				}
			}
			if ok {
				return true
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	return false
}

// waitApplied 轮询直到节点的 LastApplied 推进到 >= idx（即该条目已提交并应用到状态机）。
func waitApplied(t *testing.T, rf *Raft, idx int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if rf.LastApplied() >= idx {
			return true
		}
		time.Sleep(40 * time.Millisecond)
	}
	return false
}

// TestWitnessDynamicJoinLeave 端到端验证 witness 动态重配置 Join/Leave：
//  1. 3 节点（2 数据 + 1 witness），初始投票配置仅 [0,1]（witness 暂不投票，quorum=2）；
//  2. 运行时 Join(2)：配置变 [0,1,2]，quorum 仍为 2，但现含 witness 一票；
//  3. kill 一个数据副本后，剩 leader(数据) + witness = 2 票 ≥ quorum，仍能提交
//     —— 证明动态加入的 witness 实时提供 quorum（存储减半型容错的运行时兑现）；
//  4. Leave(2)：配置回 [0,1]，quorum=2；此时唯一存活数据副本凑不齐 quorum，新写不再
//     提交——证明 Leave 真把 witness 移出了投票集合（quorum 计票随配置实时变化）。
func TestWitnessDynamicJoinLeave(t *testing.T) {
	const w = 2 // 下标 2 为 witness
	// 初始投票集合仅 [0,1]：witness 出生但不投票（演示「动态 Join 前」的状态）。
	cfg := makeConfigWitness(t, 3, map[int]bool{w: true}, []int{0, 1})
	defer cfg.cleanup()

	l, _ := cfg.checkOneLeader()
	if l < 0 {
		t.Fatalf("初始未选出 leader")
	}
	if l == w {
		t.Fatalf("witness 竟成为初始 leader")
	}
	voter := (l + 1) % 3
	if voter == w {
		voter = (l + 2) % 3
	}

	// 初始配置 [0,1]：一条写应能提交（0,1 两个投票副本都在）。
	idx0 := cfg.start1(101)
	if !waitApplied(t, cfg.rafts[l], idx0, 6*time.Second) {
		t.Fatalf("初始 [0,1] 配置下写未提交（quorum 应达成）")
	}

	// ---- Join(2)：运行时把 witness 加入投票集合 ----
	if _, ok := cfg.rafts[l].ProposeConfChange([]int{0, 1, 2}); !ok {
		t.Fatalf("leader 提议 Join(2) 失败（非 leader 或 pendingConf 卡住）")
	}
	if !waitVoterConfig(t, cfg.rafts[l], []int{0, 1, 2}, 6*time.Second) {
		t.Fatalf("Join(2) 后 leader 投票配置未切换到 [0,1,2]：%v", cfg.rafts[l].VoterConfig())
	}
	for i := 0; i < 3; i++ {
		if !waitVoterConfig(t, cfg.rafts[i], []int{0, 1, 2}, 3*time.Second) {
			t.Fatalf("节点 %d 未应用 Join(2) 配置：%v", i, cfg.rafts[i].VoterConfig())
		}
	}

	// ---- kill 一个数据副本，验证动态 witness 实时提供 quorum ----
	cfg.kill(voter)
	idx1 := cfg.start1(202)
	committed := false
	to := time.Now().Add(6 * time.Second)
	for time.Now().Before(to) {
		if cfg.rafts[l].LastApplied() >= idx1 {
			committed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !committed {
		t.Fatalf("Join(2) 后 kill 数据副本，leader+witness 应仍能提交（quorum=2），但写 %d 未提交", idx1)
	}
	// witness 必须已复制该写（证明它实时参与投票/复制）。
	if cfg.rafts[w].Status().LastLogIndex < idx1 {
		t.Fatalf("witness 未复制 Join 后的写：LastLogIndex=%d < %d", cfg.rafts[w].Status().LastLogIndex, idx1)
	}
	if cfg.rafts[w].Status().LeaderElections != 0 {
		t.Fatalf("witness 竟参与选举（LeaderElections=%d），应恒为 0", cfg.rafts[w].Status().LeaderElections)
	}

	// ---- Leave(2)：把 witness 移出投票集合 ----
	if _, ok := cfg.rafts[l].ProposeConfChange([]int{0, 1}); !ok {
		t.Fatalf("leader 提议 Leave(2) 失败")
	}
	if !waitVoterConfig(t, cfg.rafts[l], []int{0, 1}, 6*time.Second) {
		t.Fatalf("Leave(2) 后 leader 投票配置未回退到 [0,1]：%v", cfg.rafts[l].VoterConfig())
	}

	// 现在仅 leader(数据) 存活，quorum=2 不可达：新写不应提交（证明 Leave 生效）。
	l2, _ := cfg.checkOneLeader()
	if l2 != l {
		t.Fatalf("Leave 后 leader 发生变化（%d -> %d），测试前提被破坏", l, l2)
	}
	li, _, ok := cfg.rafts[l].Start(303)
	if !ok {
		t.Fatalf("Leave 后 leader 不再是 leader，无法验证不提交")
	}
	time.Sleep(2 * ElectionTimeoutMax)
	if cfg.rafts[l].LastApplied() >= li {
		t.Fatalf("Leave(2) 后仅剩 1 数据副本（quorum=2 不可达），写 %d 竟被提交——Join/Leave 未真正改变 quorum", li)
	}
}

// TestConfChangeSingleInFlight 验证 pendingConf 守卫：一次成员变更未提交前，leader 拒绝
// 再堆叠第二个变更（保证单服变更安全性，新旧配置多数派必重叠）。
func TestConfChangeSingleInFlight(t *testing.T) {
	const w = 2
	cfg := makeConfigWitness(t, 3, map[int]bool{w: true})
	defer cfg.cleanup()
	l, _ := cfg.checkOneLeader()
	if l < 0 {
		t.Fatalf("未选出 leader")
	}
	// 第一次变更（witness 已在初始配置，这里演示一次合法的移除再恢复）应成功。
	if _, ok := cfg.rafts[l].ProposeConfChange([]int{0, 1}); !ok {
		t.Fatalf("第一次 ProposeConfChange 应成功")
	}
	// 在第一次提交前再提议第二次：必须被 pendingConf 守卫拒绝。
	if _, ok := cfg.rafts[l].ProposeConfChange([]int{0, 1, 2}); ok {
		t.Fatalf("pendingConf 守卫失效：上一次变更在途时竟允许再提议")
	}
	// 等第一次提交后，应可再次提议。
	if !waitVoterConfig(t, cfg.rafts[l], []int{0, 1}, 6*time.Second) {
		t.Fatalf("第一次变更未提交")
	}
	if _, ok := cfg.rafts[l].ProposeConfChange([]int{0, 1, 2}); !ok {
		t.Fatalf("第一次变更提交后应允许第二次提议")
	}
	if !waitVoterConfig(t, cfg.rafts[l], []int{0, 1, 2}, 6*time.Second) {
		t.Fatalf("第二次变更未提交")
	}
}
