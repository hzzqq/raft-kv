package shardkv

import "testing"

func TestDedupKey(t *testing.T) {
	a := Op{ClientId: 7, Seq: 3, Key: "k", OpType: "Put"}
	b := Op{ClientId: 7, Seq: 3, Key: "other", OpType: "Get"} // 同 client+seq，不同 key/type
	// 去重键只认 client+seq，与 key/type 无关
	if DedupKey(a) != DedupKey(b) {
		t.Fatalf("同 ClientId:Seq 应得相同去重键，实际 %q vs %q", DedupKey(a), DedupKey(b))
	}
	if DedupKey(a) != "7:3" {
		t.Fatalf("去重键格式应为 \"7:3\"，实际 %q", DedupKey(a))
	}
	// 不同 seq → 不同键
	c := Op{ClientId: 7, Seq: 4}
	if DedupKey(c) == DedupKey(a) {
		t.Fatal("不同 Seq 应得不同去重键")
	}
	// 不同 client → 不同键
	d := Op{ClientId: 8, Seq: 3}
	if DedupKey(d) == DedupKey(a) {
		t.Fatal("不同 ClientId 应得不同去重键")
	}
}
