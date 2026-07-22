// key2shard_test.go —— demo key 分片映射的不变量白盒单测（cluster-free）。
// key2shard 是迁移/分片路由的基础，必须稳定且不越界、能把不同 key 摊到多分片。
package main

import (
	"testing"

	"raftkv/src/shardmaster"
)

func TestKey2ShardDeterministic(t *testing.T) {
	// 同一 key 必须稳定映射到同一 shard（演示迁移依赖此不变量）。
	a := key2shard("hello")
	b := key2shard("hello")
	if a != b {
		t.Fatalf("key2shard not deterministic: %d vs %d", a, b)
	}
	if a < 0 || a >= shardmaster.NShards {
		t.Fatalf("key2shard out of range: %d (NShards=%d)", a, shardmaster.NShards)
	}
	// 不同 key 应摊到多分片（抽样验证不是恒等映射）。
	seen := map[int]bool{}
	for _, k := range []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"} {
		s := key2shard(k)
		if s < 0 || s >= shardmaster.NShards {
			t.Fatalf("key2shard(%q) out of range: %d", k, s)
		}
		seen[s] = true
	}
	if len(seen) < 2 {
		t.Fatalf("key2shard collapses all keys to %d shard(s), expected spread", len(seen))
	}
}
