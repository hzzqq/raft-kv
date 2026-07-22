package shardkv

import "testing"

func TestOpClassify(t *testing.T) {
	cases := []struct {
		op    Op
		read  bool
		write bool
		kind  string
	}{
		{Op{OpType: "Get"}, true, false, "read"},
		{Op{OpType: "Put"}, false, true, "write"},
		{Op{OpType: "Append"}, false, true, "write"},
		{Op{OpType: "Delete"}, false, false, "unknown"},
		{Op{OpType: ""}, false, false, "unknown"},
	}
	for _, c := range cases {
		if IsReadOp(c.op) != c.read {
			t.Fatalf("IsReadOp(%q) 期望 %v", c.op.OpType, c.read)
		}
		if IsWriteOp(c.op) != c.write {
			t.Fatalf("IsWriteOp(%q) 期望 %v", c.op.OpType, c.write)
		}
		if OpKind(c.op) != c.kind {
			t.Fatalf("OpKind(%q) 期望 %q 实际 %q", c.op.OpType, c.kind, OpKind(c.op))
		}
	}
}
