// rebalance_test.go —— shardmaster rebalance / 输入校验纯函数的白盒单测（cluster-free）。
// 直接对包级纯函数 rebalance / validateJoin / validateLeave / validateMove 做确定性断言，
// 比经 ShardMaster RPC 方法的集成测试更底层、更稳定，守护配置再平衡与输入守卫生成。
package shardmaster

import (
	"testing"
)

func TestRebalanceNoUnownedShards(t *testing.T) {
	cfg := &Config{
		Shards: [NShards]int{},
		Groups: map[int][]string{0: {"a"}, 1: {"b"}, 2: {"c"}},
	}
	for i := range cfg.Shards {
		cfg.Shards[i] = 0
	}
	rebalance(cfg)
	for i := 0; i < NShards; i++ {
		if cfg.Shards[i] < 0 || cfg.Shards[i] > 2 {
			t.Fatalf("shard %d unowned or out of range after rebalance: %d", i, cfg.Shards[i])
		}
	}
}

func TestRebalanceEvenSpread(t *testing.T) {
	cfg := &Config{
		Shards: [NShards]int{},
		Groups: map[int][]string{0: {"a"}, 1: {"b"}},
	}
	for i := range cfg.Shards {
		cfg.Shards[i] = 0
	}
	rebalance(cfg)
	load := map[int]int{}
	for i := 0; i < NShards; i++ {
		load[cfg.Shards[i]]++
	}
	diff := load[0] - load[1]
	if diff < 0 {
		diff = -diff
	}
	if diff > 1 {
		t.Fatalf("rebalance uneven: load0=%d load1=%d (diff>1)", load[0], load[1])
	}
}

func TestRebalanceEmptyGroups(t *testing.T) {
	cfg := &Config{
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}
	for i := range cfg.Shards {
		cfg.Shards[i] = 1 // 原指向一个不存在的组
	}
	rebalance(cfg)
	for i := 0; i < NShards; i++ {
		if cfg.Shards[i] != 0 {
			t.Fatalf("with no groups, all shards must be 0, got shard %d=%d", i, cfg.Shards[i])
		}
	}
}

func TestValidateJoin(t *testing.T) {
	groups := map[int][]string{1: {"a"}}
	if !validateJoin(groups, map[int][]string{2: {"b"}}) {
		t.Fatalf("valid join should pass")
	}
	if validateJoin(groups, map[int][]string{1: {"b"}}) { // 重复 gid
		t.Fatalf("duplicate gid join should fail")
	}
	if validateJoin(groups, map[int][]string{}) { // 空 servers
		t.Fatalf("empty join should fail")
	}
	if validateJoin(groups, map[int][]string{0: {"b"}}) { // gid<=0
		t.Fatalf("gid<=0 join should fail")
	}
}

func TestValidateLeave(t *testing.T) {
	groups := map[int][]string{1: {"a"}, 2: {"b"}}
	if !validateLeave(groups, []int{1}) {
		t.Fatalf("valid leave should pass")
	}
	if validateLeave(groups, []int{}) { // 空
		t.Fatalf("empty leave should fail")
	}
	if validateLeave(groups, []int{9}) { // 未知
		t.Fatalf("unknown gid leave should fail")
	}
	if validateLeave(groups, []int{0}) { // <=0
		t.Fatalf("gid<=0 leave should fail")
	}
}

func TestValidateMove(t *testing.T) {
	groups := map[int][]string{1: {"a"}, 2: {"b"}}
	if !validateMove(groups, 0, 1) {
		t.Fatalf("valid move should pass")
	}
	if validateMove(groups, -1, 1) { // 越界
		t.Fatalf("negative shard move should fail")
	}
	if validateMove(groups, NShards, 1) { // 越界
		t.Fatalf("shard>=NShards move should fail")
	}
	if validateMove(groups, 0, 9) { // 未知 gid
		t.Fatalf("unknown gid move should fail")
	}
}
