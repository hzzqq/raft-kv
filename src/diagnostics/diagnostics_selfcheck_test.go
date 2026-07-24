package diagnostics

import (
	"testing"

	"raftkv/src/raft"
	"raftkv/src/shardmaster"
)

func TestSelfCheck(t *testing.T) {
	// 空历史 -> 0 分
	if d := SelfCheck(nil); d.Score != 0 {
		t.Fatalf("empty history should be 0, got %d", d.Score)
	}

	// 合法链：0 -> 1 -> 2，均应 100/ok
	chain := []shardmaster.Config{
		{Num: 0, Shards: [shardmaster.NShards]int{1, 1, 2, 2, 1, 1, 2, 2, 1, 1}, Groups: map[int][]string{1: {"a"}, 2: {"b"}}},
		{Num: 1, Shards: [shardmaster.NShards]int{1, 1, 2, 2, 1, 1, 2, 2, 1, 1}, Groups: map[int][]string{1: {"a"}, 2: {"b"}}},
		{Num: 2, Shards: [shardmaster.NShards]int{2, 1, 2, 2, 1, 1, 2, 2, 1, 1}, Groups: map[int][]string{1: {"a"}, 2: {"b"}}},
	}
	if d := SelfCheck(chain); d.Score != 100 || d.Issues[0] != "ok" {
		t.Fatalf("valid chain should be 100/ok, got %d %v", d.Score, d.Issues)
	}

	// 非法链：跳号 -> 扣分
	bad := []shardmaster.Config{
		{Num: 0, Shards: [shardmaster.NShards]int{1, 1, 2, 2, 1, 1, 2, 2, 1, 1}, Groups: map[int][]string{1: {"a"}, 2: {"b"}}},
		{Num: 5, Shards: [shardmaster.NShards]int{1, 1, 2, 2, 1, 1, 2, 2, 1, 1}, Groups: map[int][]string{1: {"a"}, 2: {"b"}}},
	}
	if d := SelfCheck(bad); d.Score >= 100 {
		t.Fatalf("bad chain (num jump) should lose points, got %d", d.Score)
	}

	// 非法链：孤儿分片（gid2 被删但分片仍指向）-> 扣分
	orphan := []shardmaster.Config{
		{Num: 0, Shards: [shardmaster.NShards]int{1, 1, 2, 2, 1, 1, 2, 2, 1, 1}, Groups: map[int][]string{1: {"a"}, 2: {"b"}}},
		{Num: 1, Shards: [shardmaster.NShards]int{1, 1, 2, 2, 1, 1, 2, 2, 1, 1}, Groups: map[int][]string{1: {"a"}}},
	}
	if d := SelfCheck(orphan); d.Score >= 100 {
		t.Fatalf("orphan chain should lose points, got %d", d.Score)
	}
}

// TestRaftCheckHealthy 验证健康快照：commitIndex<=lastLogIndex 且 lastApplied<=commitIndex
// 时应得满分（follower 视角，无租约警告）。RaftCheck 是纯函数、零副作用，可直接单测。
func TestRaftCheckHealthy(t *testing.T) {
	st := raft.RaftStatus{
		Me:           1,
		Role:         raft.Follower,
		Term:         3,
		LeaderID:     5,
		LastLogIndex: 10,
		LastLogTerm:  3,
		CommitIndex:  8,
		LastApplied:  8,
	}
	d := RaftCheck(st)
	if d.Score != 100 || d.Issues[0] != "ok" {
		t.Fatalf("healthy status should be 100/ok, got %d %v", d.Score, d.Issues)
	}
}

// TestRaftCheckCommitBeyondLog 验证 commitIndex 越过 lastLogIndex：可能丢写，重扣 30。
func TestRaftCheckCommitBeyondLog(t *testing.T) {
	st := raft.RaftStatus{
		Role:         raft.Follower,
		LastLogIndex: 5,
		CommitIndex:  9,
		LastApplied:  4,
	}
	d := RaftCheck(st)
	if d.Score > 70 {
		t.Fatalf("commitIndex>lastLogIndex should lose >=30, got %d", d.Score)
	}
}

// TestRaftCheckApplyBeyondCommit 验证 lastApplied 越过 commitIndex：违反 Raft 安全性，重扣 30。
func TestRaftCheckApplyBeyondCommit(t *testing.T) {
	st := raft.RaftStatus{
		Role:         raft.Follower,
		LastLogIndex: 10,
		CommitIndex:  4,
		LastApplied:  7,
	}
	d := RaftCheck(st)
	if d.Score > 70 {
		t.Fatalf("lastApplied>commitIndex should lose >=30, got %d", d.Score)
	}
}

// TestRaftCheckLeaderNoLease 验证 leader 无多数派租约仅提示不扣分（新当选/丢心跳属正常）。
func TestRaftCheckLeaderNoLease(t *testing.T) {
	st := raft.RaftStatus{
		Role:           raft.Leader,
		LastLogIndex:   10,
		CommitIndex:    8,
		LastApplied:    8,
		HasLeaderLease: false,
	}
	d := RaftCheck(st)
	if d.Score != 100 {
		t.Fatalf("leader without lease should still score 100 (warning only), got %d", d.Score)
	}
	if len(d.Issues) == 0 || d.Issues[0] == "ok" {
		t.Fatalf("leader without lease should emit a warning issue, got %v", d.Issues)
	}
}
