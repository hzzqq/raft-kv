package raft

import (
	"sync"
	"testing"
	"time"
)

// TestLeaderLease：白盒、确定式验证 HasLeaderLease 的多数派语义（无网络/无 goroutine，
// 避免依赖心跳时序导致 flaky）。集成层（leader 租约是否真的被 AE 应答维护、并支撑
// ShardKV 的 ReadIndex 快路径）由 shardkv 的 TestLinearizableReadLease 集群测试覆盖。
func TestLeaderLease(t *testing.T) {
	mk := func(role Role, contacts []time.Time) *Raft {
		return &Raft{
			mu:          sync.Mutex{},
			peers:       make([]*ClientEnd, len(contacts)),
			me:          0,
			role:        role,
			lastContact: contacts,
		}
	}
	now := time.Now()
	stale := now.Add(-time.Hour)

	// 3 节点，leader 且多数派近期有接触 -> 租约有效。
	rf := mk(Leader, []time.Time{now, now, now})
	if !rf.HasLeaderLease() {
		t.Fatalf("leader 接触多数派时应持有租约")
	}
	// 仅自身（follower 全部陈旧）-> 租约失效。
	rf = mk(Leader, []time.Time{now, stale, stale})
	if rf.HasLeaderLease() {
		t.Fatalf("多数派长时间未接触时租约应失效")
	}
	// 非 leader -> 永远无租约。
	rf = mk(Follower, []time.Time{now, now, now})
	if rf.HasLeaderLease() {
		t.Fatalf("follower 不应持有租约")
	}
	// 单节点：self 即多数派 -> 租约有效。
	rf = mk(Leader, []time.Time{now})
	if !rf.HasLeaderLease() {
		t.Fatalf("单节点 leader 应持有租约")
	}
	// 边界：偶数节点恰好半数接触（不算多数派，需 > n/2）。
	rf = mk(Leader, []time.Time{now, now, stale, stale}) // 4 节点，2/4 接触
	if rf.HasLeaderLease() {
		t.Fatalf("4 节点仅 2/4 接触（恰好半数）不应算多数派")
	}
	rf = mk(Leader, []time.Time{now, now, now, stale}) // 4 节点，3/4 接触
	if !rf.HasLeaderLease() {
		t.Fatalf("4 节点 3/4 接触应持有租约")
	}
}
