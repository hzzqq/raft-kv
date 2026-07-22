package shardkv

import "testing"

func mkOp(clientID, seq int64, opType, key, value string) Op {
	return Op{ClientId: clientID, Seq: seq, OpType: opType, Key: key, Value: value}
}

func TestApplyDedupBasic(t *testing.T) {
	d := NewDedupStore()
	op := mkOp(1, 1, "Put", "a", "1")
	if !d.ApplyDedup(op) {
		t.Fatal("first op should execute")
	}
	// 同序号重发（即便 Value 不同）必须判重
	dup := mkOp(1, 1, "Put", "a", "DIFFERENT")
	if d.ApplyDedup(dup) {
		t.Fatal("duplicate (same ClientId+Seq) must be skipped even if Value differs")
	}
	// 更高序号正常执行
	if !d.ApplyDedup(mkOp(1, 2, "Append", "a", "2")) {
		t.Fatal("seq 2 should execute")
	}
	// 不同客户端独立
	if !d.ApplyDedup(mkOp(2, 1, "Put", "b", "9")) {
		t.Fatal("client 2 seq 1 should execute")
	}
}

func TestApplyDedupAcrossMigration(t *testing.T) {
	src := NewDedupStore()
	// 源副本执行了客户端 1 的 seq 1..3
	for i := int64(1); i <= 3; i++ {
		if !src.ApplyDedup(mkOp(1, i, "Put", "k", "v")) {
			t.Fatalf("src op %d should execute", i)
		}
	}
	// rebalance：去重簿随分片迁移到目标副本
	dst := NewDedupStore()
	dst.Restore(src.Snapshot())

	// 迁移后，旧 seq 的重发必须被跳过（不重复执行）
	for i := int64(1); i <= 3; i++ {
		if dst.ApplyDedup(mkOp(1, i, "Put", "k", "v")) {
			t.Fatalf("post-migration duplicate seq %d must be skipped", i)
		}
	}
	// 客户端 1 推进新 seq
	if !dst.ApplyDedup(mkOp(1, 4, "Put", "k", "w")) {
		t.Fatal("seq 4 should execute on dst")
	}
	// 全新客户端仍独立
	if !dst.ApplyDedup(mkOp(7, 1, "Put", "z", "1")) {
		t.Fatal("new client seq 1 should execute")
	}
	// 身份串格式正确
	if id := OpIdentity(mkOp(9, 5, "Get", "x", "")); id != "9:5" {
		t.Fatalf("OpIdentity = %q, want 9:5", id)
	}
}
