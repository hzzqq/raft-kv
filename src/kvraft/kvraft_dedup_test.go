// kvraft_dedup_test.go —— 验证 kvraft applier 的幂等去重（#77），簇无关：
// 直接构造 KVServer（不经由 MakeKVServer，避免拉起真实 Raft），手动喂 applyCh，
// 断言相同 client+seq 的重复命令复用上次结果（LastResult），新 seq 才重新执行。
//
// 这正是线性一致性的关键保证——leader 切换导致客户端重试同一条命令时，状态机不会
// 重复应用（Put 不会写两次、Append 不会追加两次）。
package kvraft

import (
	"testing"

	"raftkv/src/raft"
)

func TestKVRaftDedup(t *testing.T) {
	kv := &KVServer{
		data:     make(map[string]string),
		sessions: make(map[int64]*clientSession),
		notify:   make(map[int]chan applyResult),
		applyCh:  make(chan raft.ApplyMsg, 8),
	}
	kv.data["k"] = "x" // 预置初值
	go kv.applier()

	// waitAppliedIndex 风格的等待：为某 apply 索引注册通知 channel 并等待结果。
	wait := func(idx int) applyResult {
		ch := make(chan applyResult, 1)
		kv.mu.Lock()
		kv.notify[idx] = ch
		kv.mu.Unlock()
		return <-ch
	}

	// 1) 首次 Get seq=1 -> 读当前 data 得到 "x"，并记入 LastResult。
	kv.applyCh <- raft.ApplyMsg{
		CommandValid: true,
		Command:      Op{Key: "k", OpType: "Get", ClientId: 1, Seq: 1},
		CommandIndex: 1,
	}
	ar1 := wait(1)
	if ar1.result.Value != "x" {
		t.Fatalf("first Get = %q, want x", ar1.result.Value)
	}

	// 2) 篡改底层 data（模拟后续状态变化），随后发送"重复"的 Get seq=1。
	kv.mu.Lock()
	kv.data["k"] = "y"
	kv.mu.Unlock()
	kv.applyCh <- raft.ApplyMsg{
		CommandValid: true,
		Command:      Op{Key: "k", OpType: "Get", ClientId: 1, Seq: 1},
		CommandIndex: 2,
	}
	ar2 := wait(2)
	if ar2.result.Value != "x" {
		t.Fatalf("duplicate Get returned %q, want x (dedup must reuse LastResult, not re-read data)", ar2.result.Value)
	}

	// 3) 新 seq=2 -> 应重新执行，读到篡改后的 "y"。
	kv.applyCh <- raft.ApplyMsg{
		CommandValid: true,
		Command:      Op{Key: "k", OpType: "Get", ClientId: 1, Seq: 2},
		CommandIndex: 3,
	}
	ar3 := wait(3)
	if ar3.result.Value != "y" {
		t.Fatalf("new seq Get = %q, want y", ar3.result.Value)
	}

	// 4) Put 去重：重复 Put 同一 seq 不应二次写入（值不变）。先 Put seq=10 = "A"。
	kv.applyCh <- raft.ApplyMsg{
		CommandValid: true,
		Command:      Op{Key: "p", OpType: "Put", Value: "A", ClientId: 2, Seq: 10},
		CommandIndex: 4,
	}
	wait(4)
	kv.mu.Lock()
	if kv.data["p"] != "A" {
		kv.mu.Unlock()
		t.Fatalf("after Put, data[p]=%q want A", kv.data["p"])
	}
	kv.mu.Unlock()
	// 重复 Put seq=10 = "B"（若去重失效会覆盖为 B）。
	kv.applyCh <- raft.ApplyMsg{
		CommandValid: true,
		Command:      Op{Key: "p", OpType: "Put", Value: "B", ClientId: 2, Seq: 10},
		CommandIndex: 5,
	}
	wait(5)
	kv.mu.Lock()
	if kv.data["p"] != "A" {
		kv.mu.Unlock()
		t.Fatalf("duplicate Put changed data[p]=%q, want A (dedup broken)", kv.data["p"])
	}
	kv.mu.Unlock()

	close(kv.applyCh) // 让 applier 优雅退出
}
