// persister_test.go —— Persister 内存持久化层的确定性白盒单测（cluster-free）。
// Persister 是崩溃恢复（n=21 commitIndex 持久化等）的底座，必须保证 Save 拷贝输入、
// Read 往返一致、Copy 完全隔离，否则恢复路径会读到被外部篡改的脏数据。
package raft

import (
	"bytes"
	"testing"
)

func TestPersisterRaftStateRoundTrip(t *testing.T) {
	p := MakeEmptyPersister()
	if p.ReadRaftState() != nil {
		t.Fatalf("empty persister should have nil raft state, got %v", p.ReadRaftState())
	}
	data := []byte("term=3 votedFor=1 log=...")
	p.SaveRaftState(data)
	got := p.ReadRaftState()
	if !bytes.Equal(got, data) {
		t.Fatalf("raft state mismatch: got %q want %q", got, data)
	}
	// SaveRaftState 必须拷贝输入：外部修改不应泄漏到已存状态。
	data[0] = 'X'
	if bytes.Equal(got, data) {
		t.Fatalf("SaveRaftState must copy input; external mutation leaked")
	}
	if !bytes.Equal(p.ReadRaftState(), []byte("term=3 votedFor=1 log=...")) {
		t.Fatalf("persisted state corrupted by external mutation")
	}
}

func TestPersisterSnapshotRoundTrip(t *testing.T) {
	p := MakeEmptyPersister()
	if p.ReadSnapshot() != nil {
		t.Fatalf("empty persister should have nil snapshot")
	}
	snap := []byte{1, 2, 3, 4}
	p.SaveSnapshot(snap)
	if !bytes.Equal(p.ReadSnapshot(), snap) {
		t.Fatalf("snapshot round-trip mismatch")
	}
	snap[0] = 9 // 外部修改不应泄漏
	if p.ReadSnapshot()[0] == 9 {
		t.Fatalf("SaveSnapshot must copy input")
	}
}

func TestPersisterCopyIsolation(t *testing.T) {
	p := MakeEmptyPersister()
	p.SaveRaftState([]byte("state-v1"))
	p.SaveSnapshot([]byte("snap-v1"))
	cp := p.Copy()
	// 修改原 p，副本必须不受影响（崩溃恢复场景：旧副本用于回放，不能被新状态污染）。
	p.SaveRaftState([]byte("state-v2"))
	p.SaveSnapshot([]byte("snap-v2"))
	if !bytes.Equal(cp.ReadRaftState(), []byte("state-v1")) {
		t.Fatalf("Copy must be isolated from later mutations (raft state): got %q", cp.ReadRaftState())
	}
	if !bytes.Equal(cp.ReadSnapshot(), []byte("snap-v1")) {
		t.Fatalf("Copy must be isolated from later mutations (snapshot): got %q", cp.ReadSnapshot())
	}
}
