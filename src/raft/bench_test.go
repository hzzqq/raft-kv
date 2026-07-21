package raft

import "testing"

// BenchmarkRaftAgree 测量在 3 副本集群上达成一致的吞吐（ops/sec）。
// 这是 raft 提交路径的量化基线：commitIndex 持久化（cycle 87 末 / n=21）会在每次
// 提交推进时触发一次 gob 全量序列化，本基准可捕获由此带来的吞吐变化，防止后续
// 改动无意中拖慢提交路径。
func BenchmarkRaftAgree(b *testing.B) {
	cfg := makeConfig(b, 3)
	defer cfg.cleanup()

	// 先达成一次初始一致以选出稳定的 leader，避免把选举抖动算进基准。
	cfg.one(0, 3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cfg.one(i, 3)
	}
}
