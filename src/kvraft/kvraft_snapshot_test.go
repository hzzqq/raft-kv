// kvraft_snapshot_test.go —— installSnapshot 正确性与竞态防护的 cluster-free 单测（#198）。
// installSnapshot 只触碰 mu/data/sessions/appliedIndex，可直接构造 KVServer 字面量验证，
// 无需启动 raft 集群（遵循沙箱内规避进程内选举的纪律）。
package kvraft

import (
	"bytes"
	"encoding/gob"
	"testing"
)

func encState(t *testing.T, st KVPersistState) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(st); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return buf.Bytes()
}

func bareKV() *KVServer {
	return &KVServer{
		data:     map[string]string{"old": "1"},
		sessions: map[int64]*clientSession{7: {LastSeq: 3}},
	}
}

// TestInstallSnapshotFresh 验证：新快照安装成功，状态覆盖且 appliedIndex 推进。
func TestInstallSnapshotFresh(t *testing.T) {
	kv := bareKV()
	snap := encState(t, KVPersistState{
		Data:     map[string]string{"k": "v"},
		Sessions: map[int64]*clientSession{1: {LastSeq: 9, LastResult: "r"}},
	})
	if !kv.installSnapshot(snap, 5) {
		t.Fatalf("fresh snapshot should install")
	}
	if kv.appliedIndex != 5 {
		t.Fatalf("appliedIndex = %d, want 5", kv.appliedIndex)
	}
	if kv.data["k"] != "v" || kv.data["old"] != "" {
		t.Fatalf("state not replaced: %v", kv.data)
	}
	if kv.sessions[1] == nil || kv.sessions[1].LastSeq != 9 {
		t.Fatalf("sessions not replaced: %v", kv.sessions)
	}
}

// TestInstallSnapshotStaleRejected 验证：陈旧快照（index<=appliedIndex）拒绝安装，
// 状态机不被回滚——这是线性一致性的关键防线。
func TestInstallSnapshotStaleRejected(t *testing.T) {
	kv := bareKV()
	kv.appliedIndex = 10
	snap := encState(t, KVPersistState{Data: map[string]string{"stale": "x"}})
	if kv.installSnapshot(snap, 10) {
		t.Fatalf("equal-index snapshot must be rejected")
	}
	if kv.installSnapshot(snap, 3) {
		t.Fatalf("older snapshot must be rejected")
	}
	if kv.data["old"] != "1" || kv.appliedIndex != 10 {
		t.Fatalf("state must be untouched after rejection: %v idx=%d", kv.data, kv.appliedIndex)
	}
}

// TestInstallSnapshotGarbage 验证：坏字节/空字节不安装、不 panic、状态不动。
func TestInstallSnapshotGarbage(t *testing.T) {
	kv := bareKV()
	if kv.installSnapshot(nil, 5) {
		t.Fatalf("empty snapshot should be no-op")
	}
	if kv.installSnapshot([]byte{0xde, 0xad}, 5) {
		t.Fatalf("garbage snapshot should be rejected")
	}
	if kv.data["old"] != "1" {
		t.Fatalf("state must be untouched: %v", kv.data)
	}
}

// TestInstallSnapshotNilMapsNormalized 验证：快照携带 nil map 时归一为空 map，
// 后续写入不 panic（gob 解码空 map 的经典陷阱）。
func TestInstallSnapshotNilMapsNormalized(t *testing.T) {
	kv := bareKV()
	snap := encState(t, KVPersistState{Data: nil, Sessions: nil})
	if !kv.installSnapshot(snap, 2) {
		t.Fatalf("nil-map snapshot should still install")
	}
	// 安装后直接写入不得 panic
	kv.mu.Lock()
	kv.data["new"] = "ok"
	kv.sessions[1] = &clientSession{LastSeq: 1}
	kv.mu.Unlock()
	if kv.data["new"] != "ok" {
		t.Fatalf("write after nil-map install failed")
	}
}

// TestInstallSnapshotZeroIndex 验证：snapIndex=0（无索引信息）安装但不推进 appliedIndex。
func TestInstallSnapshotZeroIndex(t *testing.T) {
	kv := bareKV()
	snap := encState(t, KVPersistState{Data: map[string]string{"z": "1"}})
	if !kv.installSnapshot(snap, 0) {
		t.Fatalf("zero-index snapshot should install")
	}
	if kv.appliedIndex != 0 {
		t.Fatalf("appliedIndex must stay 0, got %d", kv.appliedIndex)
	}
	if kv.data["z"] != "1" {
		t.Fatalf("state not installed: %v", kv.data)
	}
}
