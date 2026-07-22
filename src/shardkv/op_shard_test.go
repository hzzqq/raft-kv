package shardkv

import "testing"

func TestOpShard(t *testing.T) {
	// OpShard 必须与直接用 Key2Shard 算出的分片一致。
	for _, k := range []string{"a", "user:42", "zzz"} {
		op := Op{Key: k, OpType: "Get"}
		if got := OpShard(op); got != Key2Shard(k) {
			t.Fatalf("OpShard(%q)=%d 但 Key2Shard=%d", k, got, Key2Shard(k))
		}
	}
	// 不同 key 通常落在不同分片（至少大部分）。
	if OpShard(Op{Key: "alpha"}) == OpShard(Op{Key: "beta"}) &&
		OpShard(Op{Key: "alpha"}) == OpShard(Op{Key: "gamma"}) {
		t.Fatal("多个不同 key 全部撞同一分片，映射质量可疑")
	}
}
