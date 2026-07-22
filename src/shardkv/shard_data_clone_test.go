package shardkv

import "testing"

func TestShardDataClone(t *testing.T) {
	orig := &ShardData{
		Data:       map[string]string{"k": "v1"},
		LastSeq:    map[int64]int64{10: 3},
		LastResult: map[int64]string{10: "r"},
	}
	clone := orig.Clone()
	if clone == nil {
		t.Fatal("Clone 不应返回 nil")
	}
	// 内容一致
	if clone.Data["k"] != "v1" || clone.LastSeq[10] != 3 || clone.LastResult[10] != "r" {
		t.Fatal("Clone 内容不一致")
	}
	// 隔离性：修改 clone 不应影响 orig
	clone.Data["k"] = "v2"
	clone.LastSeq[10] = 99
	if orig.Data["k"] != "v1" || orig.LastSeq[10] != 3 {
		t.Fatal("Clone 与原实例未隔离：修改 clone 影响了 orig")
	}
	// nil 安全
	if (&ShardData{}).Clone() == nil {
		t.Fatal("非空接收者 Clone 不应返回 nil")
	}
	if (*ShardData)(nil).Clone() != nil {
		t.Fatal("nil 接收者 Clone 应返回 nil")
	}
}
