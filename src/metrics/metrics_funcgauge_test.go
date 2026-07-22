package metrics

import (
	"strings"
	"testing"
)

// TestFuncGaugeValue 验证：注册的取值函数每次 Value 实时调用，反映外部状态变化。
func TestFuncGaugeValue(t *testing.T) {
	var x float64 = 10
	g := NewFuncGauge(func() float64 { return x })
	if g.Value() != 10 {
		t.Fatalf("期望 10，实际 %v", g.Value())
	}
	x = 42
	if g.Value() != 42 {
		t.Fatalf("期望实时 42，实际 %v", g.Value())
	}
	// nil 函数安全返回 0。
	nilG := NewFuncGauge(nil)
	if nilG.Value() != 0 {
		t.Fatalf("nil fn 期望 0，实际 %v", nilG.Value())
	}
}

// TestRegistryFuncGauge 验证：注册到 Registry 后 Snapshot 与 Prometheus 导出包含其值，且 HELP 对齐。
func TestRegistryFuncGauge(t *testing.T) {
	r := NewRegistry()
	var n int = 3
	r.FuncGaugeWithHelp("goroutines", "当前 goroutine 数", func() float64 { return float64(n) })

	snap := r.Snapshot()
	gauges, _ := snap["gauges"].(map[string]float64)
	if gauges["goroutines"] != 3 {
		t.Fatalf("期望 Snapshot 含 goroutines=3，实际 %+v", gauges)
	}

	var sb strings.Builder
	if err := r.WritePrometheus(&sb); err != nil {
		t.Fatalf("WritePrometheus 失败: %v", err)
	}
	out := sb.String()
	if !strings.Contains(out, "# HELP goroutines 当前 goroutine 数") {
		t.Fatalf("期望含 HELP 描述，实际:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE goroutines gauge") || !strings.Contains(out, "\ngoroutines 3") {
		t.Fatalf("期望含 gauge 序列 goroutines 3，实际:\n%s", out)
	}
}

// TestRegistryFuncGaugeSubsystem 验证：子系统 FuncGauge 加前缀、Snapshot/导出隔离。
func TestRegistryFuncGaugeSubsystem(t *testing.T) {
	r := NewRegistry()
	sub := r.Subsystem("raft")
	sub.FuncGauge("term", func() float64 { return 5 })

	snap := r.Snapshot()
	gauges, _ := snap["gauges"].(map[string]float64)
	if v, ok := gauges["raft_term"]; !ok || v != 5 {
		t.Fatalf("期望根表含 raft_term=5，实际 %+v", gauges)
	}
	// 根表本身（前缀为空）不应直接含无前缀的 term。
	if _, ok := gauges["term"]; ok {
		t.Fatalf("根表不应含无前缀 term")
	}

	var sb strings.Builder
	if err := r.WritePrometheus(&sb); err != nil {
		t.Fatalf("WritePrometheus 失败: %v", err)
	}
	if !strings.Contains(sb.String(), "raft_term 5") {
		t.Fatalf("期望导出含 raft_term 5，实际:\n%s", sb.String())
	}
}
