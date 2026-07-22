package shardkv

import "testing"

// simState 是分片状态机的极简模拟：一个 KV map + 一个 DedupStore。
// apply 实现「先判重、再执行、后标记」的契约，等价于真实状态机在 apply 一条写 op 时的逻辑。
// 注意：真实环境由 raft 日志保证 op 按 seq 顺序到达；本测试同样按 seq 顺序应用，
// 以符合「去重簿信任单调 seq」的契约（见 TestDedupGapSemantics）。
type simState struct {
	data map[string]string
	ded  *DedupStore
}

func (s *simState) apply(clientID, seq int64, opType, key, value string) (executed bool) {
	if s.ded.Seen(clientID, seq) {
		return false // 重复：跳过，不改动状态
	}
	switch opType {
	case "Put":
		s.data[key] = value
	case "Append":
		s.data[key] += value
	}
	s.ded.Mark(clientID, seq)
	return true
}

// TestDedupAcrossRebalance 是端到端去重深测：
//  1. 副本 A 顺序应用客户端 1 的写序列（含重复重发），验证幂等；
//  2. rebalance：把去重簿快照 + 数据快照迁移到新副本 B；
//  3. 副本 B 继续服务，验证「迁移前已执行的 op 仍被判重跳过」「新 seq 正常执行」
//     「全新客户端可独立工作」「最终数据线性一致」。
func TestDedupAcrossRebalance(t *testing.T) {
	A := &simState{data: map[string]string{}, ded: NewDedupStore()}

	// 客户端 1：seq 1 Put a=1, seq 2 Append a+=2, 重复重发 seq1, seq3 Put b=9, 重复重发 seq2
	if !A.apply(1, 1, "Put", "a", "1") {
		t.Fatal("op1 should execute")
	}
	if !A.apply(1, 2, "Append", "a", "2") {
		t.Fatal("op2 should execute")
	}
	if A.apply(1, 1, "Put", "a", "1") {
		t.Fatal("duplicate op1 must be skipped")
	}
	if !A.apply(1, 3, "Put", "b", "9") {
		t.Fatal("op3 should execute")
	}
	if A.apply(1, 2, "Append", "a", "2") {
		t.Fatal("duplicate op2 must be skipped")
	}
	if A.data["a"] != "12" || A.data["b"] != "9" {
		t.Fatalf("pre-rebalance data wrong: %v", A.data)
	}

	// rebalance：快照去重簿 + 数据，迁移到副本 B
	snapDed := A.ded.Snapshot()
	snapData := make(map[string]string, len(A.data))
	for k, v := range A.data {
		snapData[k] = v
	}
	B := &simState{data: snapData, ded: NewDedupStore()}
	B.ded.Restore(snapDed)

	// 迁移后：旧 op 的重复重发必须被跳过
	if B.apply(1, 3, "Put", "b", "9") {
		t.Fatal("post-rebalance duplicate op3 must be skipped")
	}
	if B.apply(1, 1, "Put", "a", "1") {
		t.Fatal("post-rebalance duplicate op1 must be skipped")
	}
	// 客户端 1 继续推进新 seq
	if !B.apply(1, 4, "Append", "a", "3") {
		t.Fatal("op4 should execute on B")
	}
	if B.data["a"] != "123" {
		t.Fatalf("B a = %q, want 123", B.data["a"])
	}
	// 全新客户端 2 独立工作
	if !B.apply(2, 1, "Put", "c", "7") {
		t.Fatal("client2 op1 should execute")
	}
	if B.data["c"] != "7" {
		t.Fatalf("client2 data wrong: %v", B.data)
	}
	if B.apply(2, 1, "Put", "c", "7") {
		t.Fatal("client2 duplicate must be skipped")
	}
	// 最终数据线性一致
	if B.data["a"] != "123" || B.data["b"] != "9" || B.data["c"] != "7" {
		t.Fatalf("final B data wrong: %v", B.data)
	}
}

// TestDedupGapSemantics 明确去重簿的契约：它信任单调 seq（由 raft 日志顺序保证）。
// 若某客户端最大 seq 被推进到 5（例如中间 op 在别处已提交），则 seq<=5 的到达一律判重——
// 即便其中某个 seq 在本副本从未真正执行过，也由「顺序应用」契约保证其早已生效。
// 此测试用于固化该语义，防止误把去重改成「逐 op 记录」而增加迁移快照体积。
func TestDedupGapSemantics(t *testing.T) {
	d := NewDedupStore()
	d.Mark(1, 5) // 直接推进到 5（模拟前序 op 已在他处提交）
	if !d.Seen(1, 5) {
		t.Fatal("(1,5) should be seen")
	}
	if !d.Seen(1, 4) {
		t.Fatal("(1,4) must be treated as seen (<= max), even if never applied here")
	}
	if !d.Seen(1, 1) {
		t.Fatal("(1,1) must be treated as seen")
	}
	if d.Seen(1, 6) {
		t.Fatal("(1,6) should still be new")
	}
}
