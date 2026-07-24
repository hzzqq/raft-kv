// kvraft_status_test.go —— KV 状态机只读健康快照与 GC 可观测性(#224)：
// 簇无关构造 KVServer（不拉起真实 Raft，rf=nil），断言 Status()/规模 gauge/GC 计数。
//
// 此前 KV 状态机对运维完全不透明——数据规模、去重会话表增长、apply 进度、GC 回收
// 均不可见，与同仓 raft/shardkv 已暴露的共识健康不对齐；本测试锁死这些一手信号。
package kvraft

import (
	"testing"
	"time"

	"raftkv/src/raft"
)

func TestKVStatusAndGCMetrics(t *testing.T) {
	kv := &KVServer{
		data:     make(map[string]string),
		sessions: make(map[int64]*clientSession),
		notify:   make(map[int]chan applyResult),
		applyCh:  make(chan raft.ApplyMsg, 8),
		gcTTL:    time.Nanosecond, // 极小 TTL，gcSweep 用未来时间即可判定超时
	}
	go kv.applier()

	// wait 注册某 apply 索引的通知 channel 并同步等待 applier 处理完成。
	wait := func(idx int) applyResult {
		ch := make(chan applyResult, 1)
		kv.mu.Lock()
		kv.notify[idx] = ch
		kv.mu.Unlock()
		return <-ch
	}

	// 1) 两个不同 key 的 Put（同 client），推进 apply 索引至 2。
	kv.applyCh <- raft.ApplyMsg{
		CommandValid: true,
		Command:      Op{Key: "a", Value: "1", OpType: "Put", ClientId: 1, Seq: 1},
		CommandIndex: 1,
	}
	wait(1)
	kv.applyCh <- raft.ApplyMsg{
		CommandValid: true,
		Command:      Op{Key: "b", Value: "2", OpType: "Put", ClientId: 1, Seq: 2},
		CommandIndex: 2,
	}
	wait(2)

	// 2) Status() 反映数据面一手信号（全部读取 kv 字段，确定性强）。
	st := kv.Status()
	if st.DataKeys != 2 {
		t.Fatalf("Status().DataKeys = %d, want 2", st.DataKeys)
	}
	if st.Sessions != 1 {
		t.Fatalf("Status().Sessions = %d, want 1", st.Sessions)
	}
	if st.AppliedIndex != 2 {
		t.Fatalf("Status().AppliedIndex = %d, want 2", st.AppliedIndex)
	}
	// rf=nil 时共识字段应为零值，且不 panic。
	if st.Role != "" || st.LeaderID != 0 {
		t.Fatalf("Status() with nil rf: Role=%q LeaderID=%d, want empty/0", st.Role, st.LeaderID)
	}

	// 3) 规模 gauge 与 Status 同源（同进程顺序执行测试，无并发写入者）。
	if g := Metrics.Gauge("kv_data_keys").Value(); g != float64(st.DataKeys) {
		t.Fatalf("kv_data_keys gauge = %g, want %d", g, st.DataKeys)
	}
	if g := Metrics.Gauge("kv_sessions").Value(); g != float64(st.Sessions) {
		t.Fatalf("kv_sessions gauge = %g, want %d", g, st.Sessions)
	}

	// 4) GC 可观测：扫描计数 +1，且超时会话被回收（evicted +1）。
	beforeSweep := Metrics.Counter("gc_sweeps_total").Value()
	beforeEvict := Metrics.Counter("gc_sessions_evicted_total").Value()
	kv.gcSweep(time.Now().Add(time.Hour)) // 远未来时间，全部会话判为超时
	afterSweep := Metrics.Counter("gc_sweeps_total").Value()
	afterEvict := Metrics.Counter("gc_sessions_evicted_total").Value()
	if afterSweep != beforeSweep+1 {
		t.Fatalf("gc_sweeps_total = %d, want %d", afterSweep, beforeSweep+1)
	}
	if afterEvict != beforeEvict+1 {
		t.Fatalf("gc_sessions_evicted_total = %d, want %d", afterEvict, beforeEvict+1)
	}
	// 回收后 Status 立即反映会话归零（读取 kv，确定性）。
	if st2 := kv.Status(); st2.Sessions != 0 {
		t.Fatalf("after GC Status().Sessions = %d, want 0", st2.Sessions)
	}
	if g := Metrics.Gauge("kv_sessions").Value(); g != 0 {
		t.Fatalf("after GC kv_sessions gauge = %g, want 0", g)
	}
}

// TestKVStatusConfigAndRegistry 锁死两个此前未覆盖的一手信号(#224 收口)：
//  1. GCTTLSec / GCIntervalSec 由 gcTTL / gcInterval 正确派生（运维据此判断 GC 频率是否合理）；
//  2. init() 里 GaugeWithHelp/CounterWithHelp 注册的 4 个序列确实进入全局注册表
//     （防止「Help 文本注册静默 no-op，/metrics 里查无此指标」这类隐性退化）。
func TestKVStatusConfigAndRegistry(t *testing.T) {
	kv := &KVServer{
		data:        make(map[string]string),
		sessions:    make(map[int64]*clientSession),
		notify:      make(map[int]chan applyResult),
		applyCh:     make(chan raft.ApplyMsg, 8),
		gcTTL:       30 * time.Second,
		gcInterval:  10 * time.Second,
	}
	go kv.applier()

	st := kv.Status()
	if st.GCTTLSec != 30 {
		t.Fatalf("Status().GCTTLSec = %d, want 30", st.GCTTLSec)
	}
	if st.GCIntervalSec != 10 {
		t.Fatalf("Status().GCIntervalSec = %d, want 10", st.GCIntervalSec)
	}

	snap := Metrics.Snapshot()
	gauges, _ := snap["gauges"].(map[string]float64)
	counters, _ := snap["counters"].(map[string]int64)
	wantGauges := []string{"kv_data_keys", "kv_sessions"}
	wantCounters := []string{"gc_sweeps_total", "gc_sessions_evicted_total"}
	for _, name := range wantGauges {
		if _, ok := gauges[name]; !ok {
			t.Fatalf("registry gauges missing %q (init GaugeWithHelp 静默失败?)", name)
		}
	}
	for _, name := range wantCounters {
		if _, ok := counters[name]; !ok {
			t.Fatalf("registry counters missing %q (init CounterWithHelp 静默失败?)", name)
		}
	}
}
