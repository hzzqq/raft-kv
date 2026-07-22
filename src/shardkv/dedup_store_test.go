package shardkv

import "testing"

func TestDedupStoreBasic(t *testing.T) {
	d := NewDedupStore()
	if d.Seen(1, 1) {
		t.Fatal("fresh store should not have seen (1,1)")
	}
	d.Mark(1, 1)
	if !d.Seen(1, 1) {
		t.Fatal("(1,1) should be seen after Mark")
	}
	// 同客户端更低 seq 视为重复
	if !d.Seen(1, 1) {
		t.Fatal("(1,1) duplicate must be seen")
	}
	// 同客户端更高 seq 视为新
	if d.Seen(1, 5) {
		t.Fatal("(1,5) should be new before Mark")
	}
	d.Mark(1, 5)
	if !d.Seen(1, 5) {
		t.Fatal("(1,5) should be seen after Mark")
	}
	if d.MaxSeq(1) != 5 {
		t.Fatalf("MaxSeq(1)=%d, want 5", d.MaxSeq(1))
	}
	// 不同客户端互不影响
	if d.Seen(2, 1) {
		t.Fatal("client 2 must be independent")
	}
}

func TestDedupStoreSnapshotRestore(t *testing.T) {
	d := NewDedupStore()
	d.Mark(1, 3)
	d.Mark(2, 7)
	d.Mark(9, 1)

	snap := d.Snapshot()
	// 破坏原 store，确认快照是独立拷贝
	d.Mark(1, 100)
	if snap[1] != 3 {
		t.Fatalf("snapshot must be a copy, got %d want 3", snap[1])
	}

	// 模拟 rebalance：新副本用快照重建去重簿
	target := NewDedupStore()
	target.Restore(snap)

	if !target.Seen(1, 3) {
		t.Fatal("after restore, (1,3) must be seen")
	}
	if !target.Seen(2, 7) {
		t.Fatal("after restore, (2,7) must be seen")
	}
	// 迁移前未见过的高 seq 仍可执行
	if target.Seen(1, 4) {
		t.Fatal("(1,4) should still be new on target")
	}
	target.Mark(1, 4)
	if !target.Seen(1, 4) {
		t.Fatal("(1,4) should be seen after Mark on target")
	}
}
