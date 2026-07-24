package raft

import "testing"

// TestRaftStatusLeader 验证 Status() 对 leader 的只读快照：role=Leader 时 LeaderID 回报 me，
// 日志索引/任期经 lastLogIndex/lastLogTerm 统一计入快照偏移。直接构造最小 Raft（role=Leader）
// 驱动，无需真实选举网络（cluster-free），验证共识层自此对运维可观测（R6 可验证收益）。
func TestRaftStatusLeader(t *testing.T) {
	rf := &Raft{
		me:           3,
		role:         Leader,
		currentTerm:  2,
		leaderId:     0,
		log:          []LogEntry{{Term: 1}, {Term: 2}},
		commitIndex:  1,
		lastApplied:  0,
		persister:    &Persister{},
	}
	st := rf.Status()
	if st.Role != Leader {
		t.Fatalf("Role 应为 Leader，实际 %v", st.Role)
	}
	if st.Term != 2 {
		t.Fatalf("Term 应为 2，实际 %d", st.Term)
	}
	if st.LeaderID != 3 {
		t.Fatalf("role=Leader 时 LeaderID 应等于 me=3，实际 %d", st.LeaderID)
	}
	if st.LastLogIndex != 2 {
		t.Fatalf("LastLogIndex 应为 2，实际 %d", st.LastLogIndex)
	}
	if st.LastLogTerm != 2 {
		t.Fatalf("LastLogTerm 应为 2，实际 %d", st.LastLogTerm)
	}
	if st.CommitIndex != 1 || st.LastApplied != 0 {
		t.Fatalf("CommitIndex/LastApplied 应为 1/0，实际 %d/%d", st.CommitIndex, st.LastApplied)
	}
}

// TestRaftStatusFollowerLeaderID 验证 follower 认知的 leader 编号直接来自 leaderId 字段
// （收到合法 AppendEntries 时设置，退位时清零），供诊断在脑裂/任期翻滚时判断「我在跟谁」。
func TestRaftStatusFollowerLeaderID(t *testing.T) {
	rf := &Raft{
		me:        1,
		role:      Follower,
		currentTerm: 1,
		leaderId:  5,
		log:       []LogEntry{},
		persister: &Persister{},
	}
	st := rf.Status()
	if st.Role != Follower {
		t.Fatalf("Role 应为 Follower，实际 %v", st.Role)
	}
	if st.LeaderID != 5 {
		t.Fatalf("follower 的 LeaderID 应反映认知的 leader=5，实际 %d", st.LeaderID)
	}
}

// TestRaftStatusNoDeadlockOnLease 验证 Status() 内部复用了无锁版 hasLeaderLeaseLocked，
// 不会因重复获取 rf.mu 而死锁（R3 并发安全：此前 HasLeaderLease 自带加锁，Status 持锁调用会死锁）。
func TestRaftStatusNoDeadlockOnLease(t *testing.T) {
	rf := &Raft{
		me:        0,
		role:      Leader,
		currentTerm: 1,
		log:       []LogEntry{{Term: 1}},
		persister: &Persister{},
		// peers/lastContact 留空：hasLeaderLeaseLocked 对空集群返回 false（contacted=0），不应 panic。
	}
	done := make(chan RaftStatus, 1)
	go func() { done <- rf.Status() }()
	select {
	case st := <-done:
		// 空集群无多数派，租约必为 false（role=Leader 但 contacted=0）。
		if st.HasLeaderLease {
			t.Fatalf("空集群 leader 不应拥有租约")
		}
	case <-make(chan struct{}):
		t.Fatal("Status() 疑似死锁")
	}
}
