package diagnostics

import (
	"testing"

	"raftkv/src/raft"
	"raftkv/src/shardkv"
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

// TestShardCheck 锁死数据面不变量自检（#230）：健康态满分；自相矛盾
// (pendingIn∩pendingOut)、重复 Owned、迁移卡滞(StallSeconds>60) 分别扣分。
func TestShardCheck(t *testing.T) {
	healthy := shardkv.ShardDebug{
		GID:       1,
		ConfigNum: 7,
		Owned:     []int{3, 4},
		PendingIn: []int{}, PendingOut: []int{},
	}
	if d := ShardCheck(healthy); d.Score != 100 || d.Issues[0] != "ok" {
		t.Fatalf("healthy shard should be 100/ok, got %d %v", d.Score, d.Issues)
	}

	// pendingIn ∩ pendingOut 自相矛盾 -> 重扣。
	conflict := shardkv.ShardDebug{GID: 1, Owned: []int{3}, PendingIn: []int{5}, PendingOut: []int{5}}
	if d := ShardCheck(conflict); d.Score > 80 {
		t.Fatalf("pendingIn∩pendingOut should lose >=30, got %d", d.Score)
	}

	// Owned 重复分片号 -> 中扣。
	dup := shardkv.ShardDebug{GID: 1, Owned: []int{3, 3}, PendingIn: []int{}, PendingOut: []int{}}
	if d := ShardCheck(dup); d.Score > 95 {
		t.Fatalf("duplicate Owned should lose points, got %d", d.Score)
	}

	// 迁移卡滞：StallSeconds 超 60s -> 中扣；未超阈值仅提示不扣分。
	stuck := shardkv.ShardDebug{GID: 1, Owned: []int{3}, PendingIn: []int{3}, PendingOut: []int{}, StallSeconds: 120}
	if d := ShardCheck(stuck); d.Score > 85 {
		t.Fatalf("stuck migration (120s) should lose >=20, got %d", d.Score)
	}
	inflight := shardkv.ShardDebug{GID: 1, Owned: []int{3}, PendingIn: []int{3}, PendingOut: []int{}, StallSeconds: 5}
	if d := ShardCheck(inflight); d.Score != 100 {
		t.Fatalf("in-flight migration (5s) should not lose points, got %d", d.Score)
	}
}
