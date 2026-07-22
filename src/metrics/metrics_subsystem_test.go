package metrics

import (
	"bytes"
	"strings"
	"testing"
)

// TestSubsystemPrefix 验证：子系统注册的指标名自动加前缀，根表可见全名，子系统快照仅见自身。
func TestSubsystemPrefix(t *testing.T) {
	root := NewRegistry()
	sub := root.Subsystem("shardkv")
	sub.Counter("ops_total").Inc()
	root.Counter("other").Inc()

	rs := root.Snapshot()
	counters := rs["counters"].(map[string]int64)
	if counters["shardkv_ops_total"] != 1 {
		t.Fatalf("根表应见 shardkv_ops_total=1，实际 %v", counters)
	}
	if counters["other"] != 1 {
		t.Fatalf("根表应见 other=1，实际 %v", counters)
	}
	if _, ok := counters["ops_total"]; ok {
		t.Fatalf("根表不应有未加前缀的 ops_total（应为 shardkv_ops_total）")
	}

	ss := sub.Snapshot()
	subCounters := ss["counters"].(map[string]int64)
	if _, ok := subCounters["other"]; ok {
		t.Fatalf("子系统快照不应含父表 other")
	}
	if subCounters["shardkv_ops_total"] != 1 {
		t.Fatalf("子系统快照应含 shardkv_ops_total=1，实际 %v", subCounters)
	}
}

// TestSubsystemWritePrometheus 验证：子系统导出时序列名带前缀且含 HELP。
func TestSubsystemWritePrometheus(t *testing.T) {
	root := NewRegistry()
	sub := root.Subsystem("gateway")
	sub.CounterWithHelp("req_total", "total gateway requests")
	sub.Counter("req_total").Inc()

	var buf bytes.Buffer
	if err := sub.WritePrometheus(&buf); err != nil {
		t.Fatalf("WritePrometheus 失败：%v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "gateway_req_total 1") {
		t.Fatalf("子系统导出应含 gateway_req_total 1，实际 %q", out)
	}
	if !strings.Contains(out, "# HELP gateway_req_total total gateway requests") {
		t.Fatalf("子系统导出应含带前缀的 HELP，实际 %q", out)
	}
	if strings.Contains(out, "other") {
		t.Fatalf("子系统导出不应含其他指标，实际 %q", out)
	}
}

// TestSubsystemNested 验证：子系统可嵌套，前缀累加。
func TestSubsystemNested(t *testing.T) {
	root := NewRegistry()
	skv := root.Subsystem("shardkv")
	raft := skv.Subsystem("raft")
	raft.Counter("commits").Inc()

	rs := root.Snapshot()
	counters := rs["counters"].(map[string]int64)
	if counters["shardkv_raft_commits"] != 1 {
		t.Fatalf("嵌套前缀应为 shardkv_raft_commits，实际 %v", counters)
	}
}

// TestSubsystemReset 验证：子系统 Reset 仅清自身前缀，不动父表其余指标。
func TestSubsystemReset(t *testing.T) {
	root := NewRegistry()
	sub := root.Subsystem("kv")
	sub.Counter("a").Inc()
	sub.Counter("b").Inc()
	root.Counter("other").Inc()

	sub.Reset()
	rs := root.Snapshot()
	counters := rs["counters"].(map[string]int64)
	if _, ok := counters["kv_a"]; ok {
		t.Fatalf("子系统 Reset 后应清除 kv_a，实际 %v", counters)
	}
	if _, ok := counters["kv_b"]; ok {
		t.Fatalf("子系统 Reset 后应清除 kv_b，实际 %v", counters)
	}
	if counters["other"] != 1 {
		t.Fatalf("子系统 Reset 不应影响父表 other，实际 %v", counters)
	}
}
