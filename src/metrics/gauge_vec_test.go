package metrics

import (
	"strings"
	"testing"
)

// TestGaugeVecBasic 验证：不同 label 值对应独立子 gauge，可分别 Set/读。
func TestGaugeVecBasic(t *testing.T) {
	v := NewGaugeVec("method")
	if len(v.LabelNames()) != 1 || v.LabelNames()[0] != "method" {
		t.Fatalf("期望标签名 [method]，实际 %v", v.LabelNames())
	}
	v.WithLabelValues("GET").Set(10)
	v.WithLabelValues("PUT").Set(20)

	snap := v.Snapshot()
	if snap["GET"] != 10 || snap["PUT"] != 20 {
		t.Fatalf("期望 GET=10 PUT=20，实际 %v", snap)
	}

	// 同 label 多次返回同一 gauge（值累加验证）。
	v.WithLabelValues("GET").Set(15)
	if v.WithLabelValues("GET").Value() != 15 {
		t.Fatalf("同 label 应返回同一 gauge，期望 15，实际 %g", v.WithLabelValues("GET").Value())
	}
}

// TestGaugeVecKeys 验证：Keys 返回所有已注册组合。
func TestGaugeVecKeys(t *testing.T) {
	v := NewGaugeVec("shard")
	v.WithLabelValues("0")
	v.WithLabelValues("1")
	v.WithLabelValues("2")
	keys := v.Keys()
	if len(keys) != 3 {
		t.Fatalf("期望 3 个 key，实际 %d", len(keys))
	}
}

// TestGaugeVecWritePrometheus 验证：导出带标签序列，含 HELP/TYPE 与正确值。
func TestGaugeVecWritePrometheus(t *testing.T) {
	v := NewGaugeVec("method")
	v.WithLabelValues("GET").Set(7)
	v.WithLabelValues("PUT").Set(3)

	var sb strings.Builder
	if err := v.WritePrometheus(&sb, "http_active", "当前活跃请求数"); err != nil {
		t.Fatalf("WritePrometheus 失败: %v", err)
	}
	out := sb.String()
	if !strings.Contains(out, "# HELP http_active 当前活跃请求数") {
		t.Fatalf("期望 HELP 行，实际:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE http_active gauge") {
		t.Fatalf("期望 TYPE 行，实际:\n%s", out)
	}
	if !strings.Contains(out, "http_active{method=\"GET\"} 7") {
		t.Fatalf("期望 GET 序列，实际:\n%s", out)
	}
	if !strings.Contains(out, "http_active{method=\"PUT\"} 3") {
		t.Fatalf("期望 PUT 序列，实际:\n%s", out)
	}
}
