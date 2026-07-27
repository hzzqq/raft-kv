package raft

import "testing"

// TestRaftLogAppendsMetric 验证共识层可观测性埋点：leader 经 Start 追加日志条目时，
// raft_log_appends_total 递增。直接构造最小 Raft 实例（role=Leader）驱动 Start，
// 无需真实选举网络（cluster-free），验证埋点与写入路径一致（R6 可验证收益）。
func TestRaftLogAppendsMetric(t *testing.T) {
	rf := &Raft{
		role:       Leader,
		currentTerm: 1,
		log:        []LogEntry{},
		persister:  MakeEmptyPersister(),
	}
	before := Metrics.Counter("raft_log_appends_total").Value()
	if _, _, ok := rf.Start(nil); !ok {
		t.Fatalf("Start(role=Leader) 应成功")
	}
	if _, _, ok := rf.Start(nil); !ok {
		t.Fatalf("Start(role=Leader) 应成功")
	}
	if got := Metrics.Counter("raft_log_appends_total").Value(); got < before+2 {
		t.Fatalf("raft_log_appends_total 增量不足: before=%d after=%d", before, got)
	}
}
