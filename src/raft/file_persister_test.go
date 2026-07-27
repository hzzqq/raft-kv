package raft

import (
	"os"
	"path/filepath"
	"testing"
)

// TestMemoryPersisterRoundTrip 验证内存 Persister 的读写闭环与 Copy 隔离。
func TestMemoryPersisterRoundTrip(t *testing.T) {
	p := MakeEmptyPersister()
	if p.ReadRaftState() != nil || p.ReadSnapshot() != nil {
		t.Fatalf("空 persister 初始应返回 nil")
	}
	p.SaveRaftState([]byte("state-abc"))
	p.SaveSnapshot([]byte("snap-xyz"))
	if string(p.ReadRaftState()) != "state-abc" {
		t.Fatalf("ReadRaftState 失配")
	}
	if string(p.ReadSnapshot()) != "snap-xyz" {
		t.Fatalf("ReadSnapshot 失配")
	}

	// Copy 隔离：副本写入不应影响原对象。
	c := p.Copy()
	c.SaveRaftState([]byte("state-changed"))
	if string(p.ReadRaftState()) != "state-abc" {
		t.Fatalf("Copy 未隔离：原对象被副本修改")
	}
	if string(c.ReadRaftState()) != "state-changed" {
		t.Fatalf("Copy 副本写入失败")
	}
}

// TestFilePersisterRoundTrip 验证落盘 Persister 的读写闭环。
func TestFilePersisterRoundTrip(t *testing.T) {
	dir, err := os.MkdirTemp("", "persister_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	f := NewFilePersister(dir)
	f.SaveRaftState([]byte("file-state"))
	f.SaveSnapshot([]byte("file-snap"))

	if string(f.ReadRaftState()) != "file-state" {
		t.Fatalf("FilePersister ReadRaftState 失配")
	}
	if string(f.ReadSnapshot()) != "file-snap" {
		t.Fatalf("FilePersister ReadSnapshot 失配")
	}

	// 文件确实落盘。
	if _, err := os.Stat(filepath.Join(dir, "raft_state.bin")); err != nil {
		t.Fatalf("raft_state.bin 未落盘: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "snapshot.bin")); err != nil {
		t.Fatalf("snapshot.bin 未落盘: %v", err)
	}
}

// TestFilePersisterCrashRecovery 模拟进程崩溃重启：复用同一目录恢复状态。
func TestFilePersisterCrashRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "persister_crash_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// 第一次“进程”：写入状态后“崩溃”。
	first := NewFilePersister(dir)
	first.SaveRaftState([]byte("committed-term-3"))
	first.SaveSnapshot([]byte("snapshot-before-crash"))

	// 第二次“进程”：全新实例复用同一目录，应读到上一次持久化的状态。
	second := NewFilePersister(dir)
	if string(second.ReadRaftState()) != "committed-term-3" {
		t.Fatalf("崩溃恢复失败：RaftState 未恢复")
	}
	if string(second.ReadSnapshot()) != "snapshot-before-crash" {
		t.Fatalf("崩溃恢复失败：Snapshot 未恢复")
	}
}

// TestFilePersisterOverwriteAtomicity 验证反复覆盖不残留 .tmp 文件（原子 rename）。
func TestFilePersisterOverwriteAtomicity(t *testing.T) {
	dir, err := os.MkdirTemp("", "persister_atomic_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	f := NewFilePersister(dir)
	for i := 0; i < 20; i++ {
		f.SaveRaftState([]byte("round"))
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("存在残留临时文件: %s", e.Name())
		}
	}
}
