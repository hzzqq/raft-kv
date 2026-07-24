package shardmaster

import "testing"

// TestMetricsConfigApplied 验证控制面可观测性埋点：applyOp 应用配置变更时，
// 累计应用次数、按 kind 细分的次数、rebalance 搬运分片数、当前配置版本 gauge 都正确递增。
// 直接驱动 applyOp（不启动 Raft），属于 cluster-free 的纯埋点单测。
func TestMetricsConfigApplied(t *testing.T) {
	sm := &ShardMaster{
		configs:  []Config{{Num: 0, Groups: map[int][]string{}}},
		lastSeq:  map[int64]int64{},
		notified: map[int64]chan struct{}{},
	}

	bJoin := Metrics.Counter("sm_join_total").Value()
	bApplied := Metrics.Counter("sm_config_applied_total").Value()
	bMoves := Metrics.Counter("sm_rebalance_moves_total").Value()

	// 加入一个 group：rebalance 会把全部 NShards 个分片从默认 gid 0 搬到 gid 1。
	sm.applyOp(Op{Kind: "Join", Servers: map[int][]string{1: {"s1", "s2", "s3"}}})

	if got := Metrics.Counter("sm_join_total").Value(); got <= bJoin {
		t.Fatalf("sm_join_total 未递增: %d", got)
	}
	if got := Metrics.Counter("sm_config_applied_total").Value(); got <= bApplied {
		t.Fatalf("sm_config_applied_total 未递增: %d", got)
	}
	if got := Metrics.Gauge("sm_config_num").Value(); got != 1 {
		t.Fatalf("sm_config_num gauge = %v, want 1", got)
	}
	if got := Metrics.Counter("sm_rebalance_moves_total").Value(); got <= bMoves {
		t.Fatalf("sm_rebalance_moves_total 未递增: %d", got)
	}

	// 再 Leave 掉该 group：分片应搬回默认 gid 0，rebalance 再次搬运。
	bLeave := Metrics.Counter("sm_leave_total").Value()
	bMoves2 := Metrics.Counter("sm_rebalance_moves_total").Value()
	sm.applyOp(Op{Kind: "Leave", Gids: []int{1}})
	if got := Metrics.Counter("sm_leave_total").Value(); got <= bLeave {
		t.Fatalf("sm_leave_total 未递增: %d", got)
	}
	if got := Metrics.Counter("sm_rebalance_moves_total").Value(); got <= bMoves2 {
		t.Fatalf("sm_rebalance_moves_total 二次未递增: %d", got)
	}
	if got := Metrics.Gauge("sm_config_num").Value(); got != 2 {
		t.Fatalf("sm_config_num gauge = %v, want 2", got)
	}
}
