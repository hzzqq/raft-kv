package shardkv

import (
	"reflect"
	"testing"
)

// TestShardDataSnapshotRestore 验证 gob 序列化往返一致。
func TestShardDataSnapshotRestore(t *testing.T) {
	sd := &ShardData{
		Data:       map[string]string{"a": "1", "b": "2"},
		LastSeq:    map[int64]int64{1: 5, 2: 3},
		LastResult: map[int64]string{1: "ok"},
	}
	b, err := sd.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sd2 := &ShardData{}
	if err := sd2.Restore(b); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sd.Data, sd2.Data) {
		t.Fatalf("data mismatch: %v vs %v", sd.Data, sd2.Data)
	}
	if !reflect.DeepEqual(sd.LastSeq, sd2.LastSeq) {
		t.Fatalf("lastseq mismatch")
	}
	if !reflect.DeepEqual(sd.LastResult, sd2.LastResult) {
		t.Fatalf("lastresult mismatch")
	}
}

// TestShardDataMerge 验证接收分片时 other 赢、seq 取 max。
func TestShardDataMerge(t *testing.T) {
	sd := &ShardData{
		Data:       map[string]string{"a": "old"},
		LastSeq:    map[int64]int64{1: 2},
		LastResult: map[int64]string{1: "x"},
	}
	other := &ShardData{
		Data:       map[string]string{"a": "new", "c": "3"},
		LastSeq:    map[int64]int64{1: 5, 2: 1},
		LastResult: map[int64]string{1: "y", 2: "z"},
	}
	sd.Merge(other)
	if sd.Data["a"] != "new" {
		t.Fatalf("other should win on data conflict")
	}
	if sd.Data["c"] != "3" {
		t.Fatalf("c missing after merge")
	}
	if sd.LastSeq[1] != 5 {
		t.Fatalf("seq should take max: got %d", sd.LastSeq[1])
	}
	if sd.LastSeq[2] != 1 {
		t.Fatalf("seq 2 missing after merge")
	}
	if sd.LastResult[1] != "y" {
		t.Fatalf("result other wins")
	}
}

// TestShardDataSubtract 验证迁出分片时移除键及对应去重身份。
func TestShardDataSubtract(t *testing.T) {
	sd := &ShardData{
		Data:       map[string]string{"a": "1", "b": "2", "c": "3"},
		LastSeq:    map[int64]int64{1: 5, 2: 3},
		LastResult: map[int64]string{1: "ok", 2: "x"},
	}
	other := &ShardData{
		Data:    map[string]string{"a": "1"},
		LastSeq: map[int64]int64{1: 5},
	}
	sd.Subtract(other)
	if _, ok := sd.Data["a"]; ok {
		t.Fatalf("a should be removed")
	}
	if _, ok := sd.Data["b"]; !ok {
		t.Fatalf("b should remain")
	}
	if _, ok := sd.LastSeq[1]; ok {
		t.Fatalf("client 1 dedup should move away with shard")
	}
	if _, ok := sd.LastSeq[2]; !ok {
		t.Fatalf("client 2 dedup should remain")
	}
}
