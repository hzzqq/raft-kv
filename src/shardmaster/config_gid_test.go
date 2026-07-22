package shardmaster

import (
	"testing"
)

// TestGidListSorted 验证：返回 gid 升序。
func TestGidListSorted(t *testing.T) {
	c := &Config{
		Num:    1,
		Shards: [NShards]int{},
		Groups: map[int][]string{3: {"s3"}, 1: {"s1"}, 2: {"s2"}},
	}
	gids := GidList(c)
	if len(gids) != 3 {
		t.Fatalf("期望 3 个 gid，实际 %d", len(gids))
	}
	want := []int{1, 2, 3}
	for i, g := range gids {
		if g != want[i] {
			t.Fatalf("期望升序 %v，实际 %v", want, gids)
		}
	}
}

// TestGidListEmpty 验证：空 Groups 返回空切片（非 nil）。
func TestGidListEmpty(t *testing.T) {
	c := &Config{Groups: map[int][]string{}}
	gids := GidList(c)
	if len(gids) != 0 {
		t.Fatalf("期望空，实际 %v", gids)
	}
	if gids == nil {
		t.Fatalf("期望非 nil 空切片")
	}
}

// TestGidListNil 验证：nil 输入安全返回 nil。
func TestGidListNil(t *testing.T) {
	if GidList(nil) != nil {
		t.Fatalf("nil 输入应返回 nil")
	}
}
