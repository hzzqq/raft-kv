// snapshot_rpc_regress_test.go —— I15：InstallSnapshot RPC 死锁 + 状态机失联回归测试。
//
// 背景（follower 快照追赶 600s 挂死复盘，两个历史 bug）：
//  1. 死锁：InstallSnapshot RPC 持 rf.mu 后调用导出版 CondInstallSnapshot（内部再次
//     Lock）——sync.Mutex 不可重入，任何需要快照追赶的 follower 会把整个 raft 实例
//     锁死，后续所有 RPC（含心跳/投票）全部堆积在 rf.mu 上（dump 中 1346 个 goroutine）。
//  2. 状态机失联：快照安装曾直接把 lastApplied 顶到 lastIncludedIndex，applier 永远
//     不会走「idx <= lastIncludedIndex」分支，SnapshotValid 消息永远不发——即便不
//     死锁，KV 状态机也拿不到快照数据。
//
// 修复后语义（本测试钉死，防重构回退）：
//  a. InstallSnapshot 处理函数必须在有限时间内返回（不能自锁挂死）；
//  b. 安装后 applier 必须向 applyCh 投递 SnapshotValid 且 SnapshotIndex 正确；
//  c. 更旧的重复 InstallSnapshot 幂等无害（不回退 lastIncludedIndex）。
package raft

import (
	"testing"
	"time"
)

// TestInstallSnapshotRPCNoDeadlock 直接调用 RPC 处理函数（白盒），旧实现在此处
// 100% 自锁挂死；新实现应立即返回，并经 applier 把快照投递给状态机。
func TestInstallSnapshotRPCNoDeadlock(t *testing.T) {
	net := MakeNetwork()
	defer net.Cleanup()
	applyCh := make(chan ApplyMsg, 100)
	// 两个端点但只启动节点 0：LeaderId=1 合法（lastContact 按 peer 数定长），
	// 且节点 0 拿不到多数派选票、不会自选举产生 noop 干扰 applyCh 断言。
	ends := []*ClientEnd{net.MakeEnd(0, 0), net.MakeEnd(1, 1)}
	rf := Make(ends, 0, MakeEmptyPersister(), applyCh)
	defer rf.Kill()

	snap := []byte("regress-snapshot-payload")
	args := &InstallSnapshotArgs{
		Term:              100, // 远大于当前任期，强制接受
		LeaderId:          1,
		LastIncludedIndex: 50,
		LastIncludedTerm:  100,
		Data:              snap,
	}

	// (a) 处理函数必须有限时间返回：旧实现（持锁再调导出版 CondInstallSnapshot）
	// 在这里永久阻塞，3s 超时即回归。
	done := make(chan struct{})
	go func() {
		var reply InstallSnapshotReply
		rf.InstallSnapshot(args, &reply)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("InstallSnapshot RPC handler deadlocked (mutex re-entry regression)")
	}

	// (b) applier 必须投递 SnapshotValid，且索引/内容正确（状态机失联回归）。
	deadline := time.After(5 * time.Second)
	for {
		select {
		case msg := <-applyCh:
			if !msg.SnapshotValid {
				continue // 单节点自选举可能先投递 noop 等命令，跳过
			}
			if msg.SnapshotIndex != 50 {
				t.Fatalf("SnapshotIndex=%d want 50", msg.SnapshotIndex)
			}
			if string(msg.Snapshot) != string(snap) {
				t.Fatalf("snapshot payload mismatch")
			}
			// (c) 更旧的重复快照幂等无害：处理函数立刻返回且状态不回退。
			var reply2 InstallSnapshotReply
			done2 := make(chan struct{})
			go func() {
				rf.InstallSnapshot(&InstallSnapshotArgs{
					Term: 100, LeaderId: 1,
					LastIncludedIndex: 30, LastIncludedTerm: 99,
					Data: []byte("stale"),
				}, &reply2)
				close(done2)
			}()
			select {
			case <-done2:
			case <-time.After(3 * time.Second):
				t.Fatalf("stale InstallSnapshot deadlocked")
			}
			rf.mu.Lock()
			lii := rf.lastIncludedIndex
			rf.mu.Unlock()
			if lii != 50 {
				t.Fatalf("lastIncludedIndex regressed to %d after stale install, want 50", lii)
			}
			return
		case <-deadline:
			t.Fatalf("applier never delivered SnapshotValid to state machine (state-machine disconnect regression)")
		}
	}
}
