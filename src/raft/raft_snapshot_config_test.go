// raft_snapshot_config_test.go —— 快照 × 成员配置的交互（「快照吞配置」bug 回归）。
//
// 背景：InstallSnapshot / Snapshot 会把 <=lastIncludedIndex 的日志整段丢弃。若被丢弃的
// 段里含 ConfChange（成员变更）条目，接收方/本节点的 cfg 与 committedCfg 将永久停在旧
// 配置——因为这些条目既走不到 AppendEntries 的配置切换（条目已不存在），也走不到 applier
// 的提交切换（applier 会把 lastApplied 直接跳到快照点，整段跳过）。后果分两类：
//
//	· 扩容（Join）后节点仍持旧的小配置 -> 用旧 quorum 计票，可能与新配置多数派各选
//	  一个 leader，即**脑裂**（安全性破坏）；
//	· 缩容（Leave）后节点仍持旧的大配置 -> 要求已移除成员投票，永远选不出主（活性破坏）。
//
// 修复：快照自带「快照点已提交配置」（InstallSnapshotArgs.LastIncludedConfig 与
// Raft.snapshotCfg），且随持久化落盘。本文件三个测试分别覆盖接收、持久化、端到端。
package raft

import (
	"sort"
	"testing"
	"time"
)

// sameVoters 按集合比较两份投票配置（忽略顺序）。
func sameVoters(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	x := append([]int(nil), a...)
	y := append([]int(nil), b...)
	sort.Ints(x)
	sort.Ints(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

// TestInstallSnapshotCarriesVoterConfig 白盒验证快照必须自带配置：给一个初始配置 [0,1]
// 的节点投递一份「快照点配置已是 [0,1,2]」的快照（ConfChange 条目已被丢弃），它必须
// 据此把 VoterConfig 切到 [0,1,2]，而不是停在 [0,1] 用旧 quorum 计票。
func TestInstallSnapshotCarriesVoterConfig(t *testing.T) {
	const w = 2 // 下标 2 为 witness：初始不在投票集合 [0,1] 内
	cfg := makeConfigWitness(t, 3, map[int]bool{w: true}, []int{0, 1})
	defer cfg.cleanup()
	if l, _ := cfg.checkOneLeader(); l < 0 {
		t.Fatalf("初始未选出 leader")
	}
	// 隔离该节点：下面会用高任期直接投递快照，避免它拿高任期去扰动集群。
	cfg.disconnect(w)

	rf := cfg.rafts[w]
	if got := rf.VoterConfig(); !sameVoters(got, []int{0, 1}) {
		t.Fatalf("前置条件不成立：witness 初始配置应为 [0,1]，实际 %v", got)
	}

	term, _ := rf.GetState()
	args := &InstallSnapshotArgs{
		Term:               term + 10,
		LeaderId:           0,
		LastIncludedIndex:  50, // 远在该节点日志之后：其现有日志整段被丢弃
		LastIncludedTerm:   term + 10,
		Data:               []byte("snap-with-join"),
		LastIncludedConfig: []int{0, 1, 2}, // 快照点处 Join(2) 已提交
	}
	var reply InstallSnapshotReply
	rf.InstallSnapshot(args, &reply)

	if got := rf.VoterConfig(); !sameVoters(got, []int{0, 1, 2}) {
		t.Fatalf("安装携带配置的快照后 VoterConfig 应为 [0,1,2]，实际 %v —— 快照吞掉了 ConfChange 条目却未补上配置，该节点此后用旧配置 [0,1] 计 quorum", got)
	}
}

// TestSnapshotVoterConfigSurvivesRestart 验证快照点配置随持久化落盘：装完快照后重启，
// 配置不能退回 Make 的默认值（全部 peer）。用 [0,1] 而非 [0,1,2] 作期望值，正是为了与
// 「全部 peer」默认值区分开——否则测试会假绿。
func TestSnapshotVoterConfigSurvivesRestart(t *testing.T) {
	const w = 2
	cfg := makeConfigWitness(t, 3, map[int]bool{w: true}, []int{0, 1})
	defer cfg.cleanup()
	if l, _ := cfg.checkOneLeader(); l < 0 {
		t.Fatalf("初始未选出 leader")
	}
	cfg.disconnect(w)

	rf := cfg.rafts[w]
	term, _ := rf.GetState()
	args := &InstallSnapshotArgs{
		Term:               term + 10,
		LeaderId:           0,
		LastIncludedIndex:  50,
		LastIncludedTerm:   term + 10,
		Data:               []byte("snap-after-leave"),
		LastIncludedConfig: []int{0, 1}, // 快照点处已把 2 移出投票集合
	}
	var reply InstallSnapshotReply
	rf.InstallSnapshot(args, &reply)

	// 重启：Make 的默认配置是「全部 peer」= [0,1,2]。若快照点配置没落盘，重启后会被
	// 默认值覆盖成 [0,1,2]，成员变更在重启瞬间丢失——比不落盘更隐蔽。
	cfg.restart(w)
	if !waitVoterConfig(t, cfg.rafts[w], []int{0, 1}, 6*time.Second) {
		t.Fatalf("重启后配置应为 [0,1]（快照点已提交配置须持久化），实际 %v", cfg.rafts[w].VoterConfig())
	}
}

// TestSnapshotDeliversVoterConfigToLaggingWitness 端到端验证：witness 在 Join(2) 期间
// 断连、错过配置变更条目，leader 随后把该条目连同日志一起快照压缩掉；witness 重连后
// 只能靠 InstallSnapshot 追赶，因此**必须**从快照里拿到新配置。
// 修复前该 witness 的 VoterConfig 永久停在 [0,1]（用旧 quorum 计票，扩容后可致双主）。
func TestSnapshotDeliversVoterConfigToLaggingWitness(t *testing.T) {
	const w = 2
	cfg := makeConfigWitness(t, 3, map[int]bool{w: true}, []int{0, 1})
	defer cfg.cleanup()

	l, _ := cfg.checkOneLeader()
	if l < 0 {
		t.Fatalf("初始未选出 leader")
	}
	if l == w {
		t.Fatalf("witness 竟成为初始 leader")
	}

	// witness 断连：它将错过随后的 ConfChange 条目与全部后续日志。
	cfg.disconnect(w)

	if _, ok := cfg.rafts[l].ProposeConfChange([]int{0, 1, 2}); !ok {
		t.Fatalf("leader 提议 Join(2) 失败（非 leader 或 pendingConf 卡住）")
	}
	if !waitVoterConfig(t, cfg.rafts[l], []int{0, 1, 2}, 8*time.Second) {
		t.Fatalf("Join(2) 未在 leader 上提交：%v", cfg.rafts[l].VoterConfig())
	}

	// 追加一批条目，把 leader 日志推远，确保 witness 重连后只能靠快照追赶
	// （其 nextIndex 停留在断连前的旧值，远小于压缩点 -> 直接走 InstallSnapshot）。
	var last int
	for i := 0; i < 12; i++ {
		last = cfg.start1(300 + i)
	}
	if !waitApplied(t, cfg.rafts[l], last, 8*time.Second) {
		t.Fatalf("追加的写未在 leader 上提交（last=%d）", last)
	}

	// 快照压缩点覆盖 ConfChange 条目：该条目随日志一起被丢弃，witness 此后
	// 再也无从通过日志/applier 学到它——只能靠快照自带配置。
	cfg.rafts[l].Snapshot(last, []byte("snap-after-join"))

	cfg.connect(w)
	if !waitVoterConfig(t, cfg.rafts[w], []int{0, 1, 2}, 12*time.Second) {
		t.Fatalf("落后 witness 经快照追赶后配置仍为 %v（应为 [0,1,2]）：快照吞掉了 ConfChange 条目且未携带配置，该 witness 此后用旧配置 [0,1] 计 quorum，扩容后可致双主", cfg.rafts[w].VoterConfig())
	}
}
