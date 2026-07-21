package raft

import (
	"testing"
	"time"
)

// TestPreVoteStillElects 是回归守卫：引入 Pre-Vote 后，普通 3 节点集群仍能正常选出
// 唯一 leader（验证预投票 → 正式选举的端到端路径未被破坏）。
func TestPreVoteStillElects(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers)
	defer cfg.cleanup()
	cfg.checkOneLeader()
}

// TestPreVoteDeniesStaleLog 直接验证 Pre-Vote 的核心安全不变量：一个"日志落后但意向
// 任期很高"的候选人，向所有节点发送预投票都应被拒绝（VoteGranted=false）。这保证了
// 落后节点永远拿不到多数预投票，从而永远不会抬升任期去扰动稳定 leader。
func TestPreVoteDeniesStaleLog(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers)
	defer cfg.cleanup()
	_, term := cfg.checkOneLeader()
	// 先做一次真实写入，确保所有节点日志非空（含 no-op + 本条目），
	// 这样"空日志候选人"才真正落后于它们、预投票应被拒绝。
	cfg.one("x", servers)

	// 模拟一个日志严重落后（LastLogIndex=0,LastLogTerm=0）但意向任期很高的候选人。
	args := &RequestPreVoteArgs{Term: term + 5, CandidateId: 99, LastLogIndex: 0, LastLogTerm: 0}
	for i := 0; i < servers; i++ {
		reply := &RequestPreVoteReply{}
		cfg.rafts[i].RequestPreVote(args, reply)
		if reply.VoteGranted {
			t.Fatalf("node %d granted pre-vote to a stale-log candidate (term=%d,lastIdx=0)", i, term+5)
		}
	}
}

// TestPreVoteNoDisrupt 验证 Pre-Vote 阻止"被隔离过、日志落后的旧 leader"在重连后
// 以高任期扰动多数派主：旧 leader 分区期间任期被抬高，但重连后因其日志落后，预投票
// 被多数派拒绝，因而不会夺回领导权、也不会迫使多数派主退位。
func TestPreVoteNoDisrupt(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers)
	defer cfg.cleanup()
	leader1, _ := cfg.checkOneLeader()

	// 把当前 leader 与其余节点隔离：其余两节点构成多数派，会重新选主。
	cfg.disconnect(leader1)
	// 等隔离节点（旧 leader）因反复选举超时而抬高任期，同时多数派选出新主。
	time.Sleep(1500 * time.Millisecond)
	// 多数派侧做一次写，使多数派日志领先于被隔离的旧 leader（其日志停留在分区点）。
	cfg.one("x", servers-1)
	// 重新连通。
	cfg.connectAll(leader1)
	// 让集群稳定。
	time.Sleep(ElectionTimeoutMax + HeartbeatInterval + 300*time.Millisecond)

	leader2, _ := cfg.checkOneLeader()
	if leader2 == leader1 {
		t.Fatalf("pre-vote failed to prevent stale leader %d from regaining leadership after partition", leader1)
	}
}
