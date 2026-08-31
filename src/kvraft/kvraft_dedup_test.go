// kvraft_dedup_test.go —— 验证 kvraft applier 的幂等去重（#77），簇无关：
// 直接构造 KVServer（不经由 MakeKVServer，避免拉起真实 Raft），手动喂 applyCh，
// 断言相同 client+seq 的重复命令复用上次结果（LastResult），新 seq 才重新执行。
//
// 这正是线性一致性的关键保证——leader 切换导致客户端重试同一条命令时，状态机不会
// 重复应用（Put 不会写两次、Append 不会追加两次）。
//
// 注意时序（I152 修复）：notify channel 必须在把 ApplyMsg 投进 applyCh **之前**注册。
// 反过来写（先投、后注册）存在竞态——applier 可能在注册完成前就应用完该条目，
// 此时 notify[idx] 还不存在，通知无人接收，测试将永久阻塞到整包 timeout。
// 该竞态原先低概率触发；机器负载升高后被放大，曾导致 ./src/... 全量回归卡死
// 900s（单个测试拖垮整次 CI）。因此这里既修时序，也给每次等待加超时兜底：
// 万一将来再出问题，表现是「秒级失败并指明索引」而不是「整包挂死」。
package kvraft

import (
	"testing"
	"time"

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

	// register 先占好 idx 的通知位（必须早于投递 ApplyMsg，见文件头注释）。
	register := func(idx int) chan applyResult {
		ch := make(chan applyResult, 1)
		kv.mu.Lock()
		kv.notify[idx] = ch
		kv.mu.Unlock()
		return ch
	}
	// await 等待 applier 的通知，带超时兜底：失败要快且要指明是哪个索引没被应用。
	await := func(ch chan applyResult, idx int) applyResult {
		t.Helper()
		select {
		case ar := <-ch:
			return ar
		case <-time.After(5 * time.Second):
			t.Fatalf("等待 apply 索引 %d 的通知超时：applier 未回调（notify 注册与投递是否又反序了？）", idx)
			return applyResult{}
		}
	}
	// apply 是「注册 → 投递 → 等待」的原子化封装，避免调用点再写错顺序。
	apply := func(idx int, op Op) applyResult {
		t.Helper()
		ch := register(idx)
		kv.applyCh <- raft.ApplyMsg{CommandValid: true, Command: op, CommandIndex: idx}
		return await(ch, idx)
	}

	// 1) 首次 Get seq=1 -> 读当前 data 得到 "x"，并记入 LastResult。
	ar1 := apply(1, Op{Key: "k", OpType: "Get", ClientId: 1, Seq: 1})
	if ar1.result.Value != "x" {
		t.Fatalf("first Get = %q, want x", ar1.result.Value)
	}

	// 2) 篡改底层 data（模拟后续状态变化），随后发送"重复"的 Get seq=1。
	kv.mu.Lock()
	kv.data["k"] = "y"
	kv.mu.Unlock()
	ar2 := apply(2, Op{Key: "k", OpType: "Get", ClientId: 1, Seq: 1})
	if ar2.result.Value != "x" {
		t.Fatalf("duplicate Get returned %q, want x (dedup must reuse LastResult, not re-read data)", ar2.result.Value)
	}

	// 3) 新 seq=2 -> 应重新执行，读到篡改后的 "y"。
	ar3 := apply(3, Op{Key: "k", OpType: "Get", ClientId: 1, Seq: 2})
	if ar3.result.Value != "y" {
		t.Fatalf("new seq Get = %q, want y", ar3.result.Value)
	}

	// 4) Put 去重：重复 Put 同一 seq 不应二次写入（值不变）。先 Put seq=10 = "A"。
	apply(4, Op{Key: "p", OpType: "Put", Value: "A", ClientId: 2, Seq: 10})
	kv.mu.Lock()
	if kv.data["p"] != "A" {
		kv.mu.Unlock()
		t.Fatalf("after Put, data[p]=%q want A", kv.data["p"])
	}
	kv.mu.Unlock()
	// 重复 Put seq=10 = "B"（若去重失效会覆盖为 B）。
	apply(5, Op{Key: "p", OpType: "Put", Value: "B", ClientId: 2, Seq: 10})
	kv.mu.Lock()
	if kv.data["p"] != "A" {
		kv.mu.Unlock()
		t.Fatalf("duplicate Put changed data[p]=%q, want A (dedup broken)", kv.data["p"])
	}
	kv.mu.Unlock()

	close(kv.applyCh) // 让 applier 优雅退出
}
