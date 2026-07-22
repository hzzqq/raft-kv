package shardmaster

import "testing"

func TestConfigValid(t *testing.T) {
	good := &Config{Num: 1, Shards: [NShards]int{1, 1, 2, 2, 1, 1, 2, 2, 1, 1}, Groups: map[int][]string{1: {"a"}, 2: {"b"}}}
	if !good.Valid() {
		t.Fatal("good config should be valid")
	}
	orphan := &Config{Num: 2, Shards: [NShards]int{1, 1, 2, 2, 1, 1, 2, 2, 1, 1}, Groups: map[int][]string{1: {"a"}}} // shard 2/3/6/7 -> gid 2 missing
	if orphan.Valid() {
		t.Fatal("orphan shard config should be invalid")
	}
	emptyGrp := &Config{Num: 3, Shards: [NShards]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, Groups: map[int][]string{1: {}}}
	if emptyGrp.Valid() {
		t.Fatal("empty group config should be invalid")
	}
}

func TestIsValidTransition(t *testing.T) {
	cur := &Config{Num: 1, Shards: [NShards]int{1, 1, 2, 2, 1, 1, 2, 2, 1, 1}, Groups: map[int][]string{1: {"a"}, 2: {"b"}}}
	// 合法：Move 一个分片到 gid2，Num+1，结构合法
	next := &Config{Num: 2, Shards: [NShards]int{2, 1, 2, 2, 1, 1, 2, 2, 1, 1}, Groups: map[int][]string{1: {"a"}, 2: {"b"}}}
	if ok, why := cur.IsValidTransition(next); !ok {
		t.Fatalf("expected valid transition: %s", why)
	}
	// 非法：Num 不连续
	if ok, _ := cur.IsValidTransition(&Config{Num: 5, Shards: cur.Shards, Groups: cur.Groups}); ok {
		t.Fatal("non-consecutive num must fail")
	}
	// 非法：next 含孤儿分片（gid2 被删但分片仍指向它）
	if ok, _ := cur.IsValidTransition(&Config{Num: 2, Shards: cur.Shards, Groups: map[int][]string{1: {"a"}}}); ok {
		t.Fatal("orphan shard in next must fail")
	}
}
