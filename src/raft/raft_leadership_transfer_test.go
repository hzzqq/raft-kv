// raft_leadership_transfer_test.go —— 领导权转移 × 在途成员变更交互（I192 收尾）。
//
// 验证 LeadershipTransfer 在有一次尚未提交的成员变更（pendingConf）进行中时必须拒绝，
// 否则该未提交 ConfChange 条目会被新 leader 的 AppendEntries 截断丢弃，导致一次"成功"
// 提议的拓扑变更静默消失。这是 etcd/rqlite 等成熟实现一致采用的护栏。
//
// 采用确定性单元测试（而非依赖集群提交时序的集成测试）：构造 2 节点网络，强制 node0 为
// leader 且 pendingConf=true（模拟一次在途成员变更），冻结双方选举/心跳计时器排除后台
// 自发选举干扰，target 保持可达。这样"无护栏"时转移会真的成功并把配置变更弄丢，从而能
// 真正证明护栏生效；护栏也不能过度拒绝——清掉 pendingConf 后向可达 target 转移应正常进行。

package raft

import (
	"testing"
)

func TestLeadershipTransferRefusesWithPendingConf(t *testing.T) {
	const n = 2
	net := MakeNetwork()
	defer net.Cleanup()

	applyCh := make([]chan ApplyMsg, n)
	persisters := make([]Persister, n)
	endnames := make([][]*ClientEnd, n)
	rfs := make([]*Raft, n)
	for i := 0; i < n; i++ {
		applyCh[i] = make(chan ApplyMsg, 4000)
		persisters[i] = MakeEmptyPersister()
		endnames[i] = make([]*ClientEnd, n)
		for j := 0; j < n; j++ {
			endnames[i][j] = net.MakeEnd(i*n+j, i)
		}
	}
	for i := 0; i < n; i++ {
		rf := Make(endnames[i], i, persisters[i], applyCh[i])
		rfs[i] = rf
		ii := i
		net.AddServer(i, func(method string, args, reply interface{}) {
			switch method {
			case "RequestVote":
				rf.RequestVote(args.(*RequestVoteArgs), reply.(*RequestVoteReply))
			case "RequestPreVote":
				rf.RequestPreVote(args.(*RequestPreVoteArgs), reply.(*RequestPreVoteReply))
			case "TimeoutNow":
				rf.TimeoutNow(args.(*TimeoutNowArgs), reply.(*TimeoutNowReply))
			case "AppendEntries":
				rf.AppendEntries(args.(*AppendEntriesArgs), reply.(*AppendEntriesReply))
			case "InstallSnapshot":
				rf.InstallSnapshot(args.(*InstallSnapshotArgs), reply.(*InstallSnapshotReply))
			default:
				panic("unknown RPC " + method)
			}
			_ = ii
		})
		for j := 0; j < n; j++ {
			net.Connect(i*n+j, j)
		}
	}
	// 关闭全部节点（defer 在 net.Cleanup 之后）。
	defer func() {
		for i := 0; i < n; i++ {
			rfs[i].Kill()
		}
	}()

	// 冻结双方选举/心跳计时器，排除后台自发选举对确定性的干扰。
	freezeTimers := func(rf *Raft) {
		rf.timerMu.Lock()
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Stop()
		rf.timerMu.Unlock()
	}
	for i := 0; i < n; i++ {
		freezeTimers(rfs[i])
	}

	// 强制 node0 为 leader 且持有一个在途成员变更。
	rfs[0].mu.Lock()
	rfs[0].role = Leader
	rfs[0].leaderId = 0
	rfs[0].currentTerm = 1
	rfs[0].pendingConf = true
	rfs[0].commitIndex = 0
	rfs[0].matchIndex = make([]int, n)
	rfs[0].mu.Unlock()

	// 护栏生效：在途成员变更期间必须拒绝转移。
	if rfs[0].LeadershipTransfer(1) {
		t.Fatalf("LeadershipTransfer must refuse while a membership change is in flight")
	}
	if !rfs[0].HasPendingConf() {
		t.Fatalf("pendingConf must remain true after refused transfer")
	}

	// 护栏不过度拒绝：清掉在途变更后，向可达 target 转移应正常进行。
	rfs[0].mu.Lock()
	rfs[0].pendingConf = false
	rfs[0].mu.Unlock()
	if !rfs[0].LeadershipTransfer(1) {
		t.Fatalf("LeadershipTransfer should succeed when no membership change is in flight and target is reachable")
	}
}
