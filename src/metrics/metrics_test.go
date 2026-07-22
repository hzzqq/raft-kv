package metrics

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestCounter(t *testing.T) {
	c := NewRegistry().Counter("x")
	if c.Value() != 0 {
		t.Fatalf("want 0 got %d", c.Value())
	}
	c.Inc()
	c.Add(4)
	if c.Value() != 5 {
		t.Fatalf("want 5 got %d", c.Value())
	}
}

func TestHistogramPercentiles(t *testing.T) {
	h := NewHistogram(100)
	for i := 1; i <= 1000; i++ {
		h.Record(float64(i))
	}
	s := h.Snapshot()
	if s.Count != 1000 {
		t.Fatalf("count want 1000 got %d", s.Count)
	}
	if s.P50 <= 0 || s.P95 <= s.P50 || s.P99 <= s.P95 {
		t.Fatalf("bad percentiles p50=%v p95=%v p99=%v", s.P50, s.P95, s.P99)
	}
	if s.Mean <= 0 {
		t.Fatalf("mean should be > 0 got %v", s.Mean)
	}
}

func TestHistogramRingOverwrite(t *testing.T) {
	h := NewHistogram(8)
	for i := 1; i <= 100; i++ {
		h.Record(float64(i))
	}
	s := h.Snapshot()
	// 容量 8：只保留最近 8 个样本（93..100），p50 应 >= 93。
	if s.P50 < 93 {
		t.Fatalf("ring overwrite wrong: p50=%v (want >=93)", s.P50)
	}
}

func TestRegistrySnapshot(t *testing.T) {
	r := NewRegistry()
	r.Counter("ops").Inc()
	r.Histogram("lat").Record(1.5)
	snap := r.Snapshot()
	counters, ok := snap["counters"].(map[string]int64)
	if !ok {
		t.Fatalf("counters missing/wrong type")
	}
	if counters["ops"] != 1 {
		t.Fatalf("want ops=1 got %v", counters["ops"])
	}
	hists, ok := snap["histograms"].(map[string]HistSnapshot)
	if !ok {
		t.Fatalf("histograms missing/wrong type")
	}
	if hists["lat"].Count != 1 {
		t.Fatalf("want lat count=1 got %v", hists["lat"].Count)
	}
}

func TestDumpJSON(t *testing.T) {
	r := NewRegistry()
	r.Counter("ops").Inc()
	r.Histogram("lat").Record(2.0)
	b, err := r.DumpJSON()
	if err != nil {
		t.Fatalf("DumpJSON error: %v", err)
	}
	var snap map[string]interface{}
	if err := json.Unmarshal(b, &snap); err != nil {
		t.Fatalf("DumpJSON not valid JSON: %v (body=%s)", err, string(b))
	}
	if _, ok := snap["counters"]; !ok {
		t.Fatalf("DumpJSON missing counters")
	}
}

func TestPeriodicReporter(t *testing.T) {
	r := NewRegistry()
	r.Counter("ops").Inc()
	var buf bytes.Buffer
	stop := make(chan struct{})
	StartPeriodicReporter(r, 20*time.Millisecond, &buf, stop)
	time.Sleep(120 * time.Millisecond) // 应触发 >=1 次 dump
	close(stop)
	time.Sleep(40 * time.Millisecond) // 等 goroutine 退出

	if buf.Len() == 0 {
		t.Fatalf("periodic reporter wrote nothing")
	}
	// 关闭后应停止写入：再等一段时间，长度不应继续增长。
	lenAfterStop := buf.Len()
	time.Sleep(80 * time.Millisecond)
	if buf.Len() != lenAfterStop {
		t.Fatalf("reporter kept writing after stop: %d -> %d", lenAfterStop, buf.Len())
	}
}

func TestSanitizeMetricName(t *testing.T) {
	cases := map[string]string{
		"":              "_",
		"shardkv.op_ms": "shardkv_op_ms",
		"raft-apply":    "raft_apply",
		"9bad":          "_bad",
		"with space":    "with_space",
		"ok_name:vec":   "ok_name:vec",
	}
	for in, want := range cases {
		if got := sanitizeMetricName(in); got != want {
			t.Fatalf("sanitizeMetricName(%q)=%q want %q", in, got, want)
		}
	}
}

func TestWritePrometheus(t *testing.T) {
	r := NewRegistry()
	r.Counter("shardkv.ops_total").Inc()
	r.Gauge("raft.applied_index").Set(42)
	r.Histogram("shardkv.op_latency_ms").Record(10)
	r.Histogram("shardkv.op_latency_ms").Record(20)

	var buf bytes.Buffer
	if err := r.WritePrometheus(&buf); err != nil {
		t.Fatalf("WritePrometheus error: %v", err)
	}
	out := buf.String()

	// 1) 带点的名字必须被清洗，输出中不应残留裸点序列名
	if bytes.Contains([]byte(out), []byte("shardkv.ops_total ")) ||
		bytes.Contains([]byte(out), []byte("raft.applied_index ")) ||
		bytes.Contains([]byte(out), []byte("shardkv.op_latency_ms ")) {
		t.Fatalf("metric name not sanitized, dot leaked:\n%s", out)
	}
	// 2) 简单序列以清洗后的名字正确输出
	if !bytes.Contains([]byte(out), []byte("shardkv_ops_total 1\n")) {
		t.Fatalf("counter not emitted:\n%s", out)
	}
	if !bytes.Contains([]byte(out), []byte("raft_applied_index 42\n")) {
		t.Fatalf("gauge not emitted:\n%s", out)
	}
	// 3) 禁止对聚合名声明错误的 histogram TYPE（会让 scrape 客户端解析失败）
	if bytes.Contains([]byte(out), []byte("# TYPE shardkv_op_latency_ms histogram")) {
		t.Fatalf("must NOT emit histogram TYPE:\n%s", out)
	}
	// 4) 直方图派生序列各声明正确 TYPE
	if !bytes.Contains([]byte(out), []byte("# TYPE shardkv_op_latency_ms_count counter")) {
		t.Fatalf("missing _count counter TYPE:\n%s", out)
	}
	if !bytes.Contains([]byte(out), []byte("# TYPE shardkv_op_latency_ms_sum gauge")) {
		t.Fatalf("missing _sum gauge TYPE:\n%s", out)
	}
	if !bytes.Contains([]byte(out), []byte("# TYPE shardkv_op_latency_ms_p99 gauge")) {
		t.Fatalf("missing _p99 gauge TYPE:\n%s", out)
	}
}

func TestHistogramMinMax(t *testing.T) {
	h := NewHistogram(100)
	for i := 1; i <= 10; i++ {
		h.Record(float64(i))
	}
	s := h.Snapshot()
	if s.Min != 1 {
		t.Fatalf("min want 1 got %v", s.Min)
	}
	if s.Max != 10 {
		t.Fatalf("max want 10 got %v", s.Max)
	}
	// 空直方图 min/max 应为 0（而非 ±Inf），避免 JSON 出现非有限数被 Prometheus 拒绝。
	empty := NewHistogram(10).Snapshot()
	if empty.Min != 0 || empty.Max != 0 {
		t.Fatalf("empty hist min/max want 0 got %v/%v", empty.Min, empty.Max)
	}
}

func TestTimer(t *testing.T) {
	h := NewHistogram(10)
	tr := h.Timer()
	time.Sleep(5 * time.Millisecond)
	tr.Stop()
	s := h.Snapshot()
	if s.Count != 1 {
		t.Fatalf("timer count want 1 got %d", s.Count)
	}
	if s.Min < 5.0 || s.Max < 5.0 {
		t.Fatalf("timer recorded too-small latency: min=%v max=%v", s.Min, s.Max)
	}
}
