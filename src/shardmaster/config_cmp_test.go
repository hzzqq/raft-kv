// config_cmp_test.go —— 验证 shardmaster 配置比较辅助（#76），全程 cluster-free 纯函数测试。
package shardmaster

import "testing"

func mkConfig(num int, shards [NShards]int, groups map[int][]string) *Config {
	return &Config{Num: num, Shards: shards, Groups: groups}
}

func TestConfigsEqual(t *testing.T) {
	shards := [NShards]int{1, 1, 2, 2, 0, 0, 3, 3, 3, 3}
	a := mkConfig(2, shards, map[int][]string{1: {"s1", "s2"}, 2: {"s3"}, 3: {"s4"}})
	b := mkConfig(2, shards, map[int][]string{1: {"s1", "s2"}, 2: {"s3"}, 3: {"s4"}})
	if !ConfigsEqual(a, b) {
		t.Fatal("identical configs should be equal")
	}
	// 顺序不同的 server 列表应视为相等（集合语义）。
	c := mkConfig(2, shards, map[int][]string{1: {"s2", "s1"}, 2: {"s3"}, 3: {"s4"}})
	if !ConfigsEqual(a, c) {
		t.Fatal("server list order should not matter")
	}
	// Num 不同 -> 不等
	d := mkConfig(3, shards, map[int][]string{1: {"s1", "s2"}, 2: {"s3"}, 3: {"s4"}})
	if ConfigsEqual(a, d) {
		t.Fatal("different Num should not be equal")
	}
	// Shards 不同 -> 不等
	var shards2 [NShards]int
	copy(shards2[:], shards[:])
	shards2[0] = 2
	e := mkConfig(2, shards2, map[int][]string{1: {"s1", "s2"}, 2: {"s3"}, 3: {"s4"}})
	if ConfigsEqual(a, e) {
		t.Fatal("different Shards should not be equal")
	}
	// Groups 不同 -> 不等
	f := mkConfig(2, shards, map[int][]string{1: {"s1"}, 2: {"s3"}, 3: {"s4"}})
	if ConfigsEqual(a, f) {
		t.Fatal("different Groups should not be equal")
	}
	// nil 处理
	if ConfigsEqual(a, nil) || ConfigsEqual(nil, a) {
		t.Fatal("nil configs must not equal non-nil")
	}
	if !ConfigsEqual(nil, nil) {
		t.Fatal("nil == nil")
	}
}

func TestIsNewer(t *testing.T) {
	old := mkConfig(1, [NShards]int{}, map[int][]string{1: {"s1"}})
	newer := mkConfig(2, [NShards]int{}, map[int][]string{1: {"s1"}})
	if !IsNewer(newer, old) {
		t.Fatal("Num 2 should be newer than Num 1")
	}
	if IsNewer(old, newer) {
		t.Fatal("Num 1 should not be newer than Num 2")
	}
	if IsNewer(old, old) {
		t.Fatal("same Num not newer")
	}
	if !IsNewer(newer, nil) {
		t.Fatal("non-nil newer than nil prev")
	}
	if IsNewer(nil, old) {
		t.Fatal("nil not newer than non-nil")
	}
}

func TestNextConfigNum(t *testing.T) {
	c := mkConfig(4, [NShards]int{}, nil)
	if got := NextConfigNum(c); got != 5 {
		t.Fatalf("NextConfigNum = %d, want 5", got)
	}
	if got := NextConfigNum(nil); got != 0 {
		t.Fatalf("NextConfigNum(nil) = %d, want 0", got)
	}
}

func TestOwnedShards(t *testing.T) {
	var shards [NShards]int
	for i := 0; i < NShards; i++ {
		shards[i] = i%3 + 1 // gid 1,2,3 轮转
	}
	c := mkConfig(1, shards, nil)
	owned1 := OwnedShards(c, 1) // 分片 0,3,6,9
	want := []int{0, 3, 6, 9}
	if len(owned1) != len(want) {
		t.Fatalf("OwnedShards(1) = %v, want %v", owned1, want)
	}
	for i := range want {
		if owned1[i] != want[i] {
			t.Fatalf("OwnedShards(1) = %v, want %v", owned1, want)
		}
	}
	// 不归 gid 99 的任何分片
	if got := OwnedShards(c, 99); len(got) != 0 {
		t.Fatalf("OwnedShards(99) = %v, want empty", got)
	}
	if OwnedShards(nil, 1) != nil {
		t.Fatal("OwnedShards(nil) should be nil")
	}
}
