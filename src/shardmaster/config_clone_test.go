package shardmaster

import (
	"testing"
)

// TestCloneConfigDeep 验证：克隆后修改副本的 Shards 与 Groups 不影响原配置（深拷贝）。
func TestCloneConfigDeep(t *testing.T) {
	orig := &Config{
		Num:    2,
		Shards: [NShards]int{1, 1, 2, 2},
		Groups: map[int][]string{1: {"s1", "s2"}, 2: {"s3"}},
	}
	clone := CloneConfig(orig)
	if clone == nil {
		t.Fatalf("克隆应非 nil")
	}
	if !ConfigsEqual(orig, clone) {
		t.Fatalf("克隆应与原配置相等")
	}

	// 改副本的 Shards（值类型，应不影响原）。
	clone.Shards[0] = 99
	if orig.Shards[0] != 1 {
		t.Fatalf("改副本 Shards 不应影响原，原.Shards[0]=%d", orig.Shards[0])
	}

	// 改副本 Groups（引用类型，应不影响原）。
	clone.Groups[1][0] = "HACK"
	if orig.Groups[1][0] != "s1" {
		t.Fatalf("改副本 Groups 不应影响原，原.Groups[1][0]=%s", orig.Groups[1][0])
	}
	clone.Groups[9] = []string{"x"} // 加新 gid
	if _, ok := orig.Groups[9]; ok {
		t.Fatalf("副本新增 gid 不应影响原")
	}
}

// TestCloneConfigNil 验证：nil 输入安全返回 nil。
func TestCloneConfigNil(t *testing.T) {
	if CloneConfig(nil) != nil {
		t.Fatalf("nil 输入应返回 nil")
	}
}

// TestCloneConfigEmptyGroups 验证：空 Groups 配置也能正确克隆（不 panic）。
func TestCloneConfigEmptyGroups(t *testing.T) {
	orig := &Config{Num: 0, Shards: [NShards]int{}, Groups: map[int][]string{}}
	clone := CloneConfig(orig)
	if clone.Groups == nil {
		t.Fatalf("克隆后 Groups 不应为 nil（应为空 map）")
	}
	if len(clone.Groups) != 0 {
		t.Fatalf("克隆后 Groups 应为空，实际 %d", len(clone.Groups))
	}
}
