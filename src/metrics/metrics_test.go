package metrics

import "testing"

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
