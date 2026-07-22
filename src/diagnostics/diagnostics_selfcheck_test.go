package diagnostics

import (
	"testing"

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
