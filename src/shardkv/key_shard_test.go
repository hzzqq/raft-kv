package shardkv

import "testing"

func TestKey2Shard(t *testing.T) {
	// 范围不变量：所有 key 都必须落在 [0, NShards) 内。
	for _, k := range []string{"", "a", "foo", "user:123", "shard-key-long-name-99999"} {
		s := Key2Shard(k)
		if s < 0 || s >= NShards {
			t.Fatalf("Key2Shard(%q)=%d 超出 [0,%d)", k, s, NShards-1)
		}
	}
	// 同一 key 必须稳定映射到同一分片（确定性）。
	if Key2Shard("abc") != Key2Shard("abc") {
		t.Fatal("Key2Shard 必须确定性：同一 key 两次结果不同")
	}
	// 映射应把一批 key 分散到多个分片（避免把所有流量压到单一分片）。
	seen := map[int]bool{}
	for i := 0; i < 200; i++ {
		seen[Key2Shard(string(rune('a'+i%26))+string(rune('0'+i%10))+string(rune('A'+i%26)))] = true
	}
	if len(seen) < 3 {
		t.Fatalf("期望 key 分散到多个分片，实际仅 %d 个", len(seen))
	}
}
