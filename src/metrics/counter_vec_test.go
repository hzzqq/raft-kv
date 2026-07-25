// counter_vec_test.go —— CounterVec 单测（cycle #117）。
package metrics

import (
	"bytes"
	"strings"
	"testing"
)

func TestCounterVecBasic(t *testing.T) {
	v := NewCounterVec("code", "method")
	a := v.WithLabelValues("200", "GET")
	b := v.WithLabelValues("500", "GET")
	a.Inc()
	a.Add(4)
	b.Inc()
	if got := a.Value(); got != 5 {
		t.Fatalf("200/GET = %d, want 5", got)
	}
	if got := b.Value(); got != 1 {
		t.Fatalf("500/GET = %d, want 1", got)
	}
	// 同一 label 组合复用同一子 counter（指针稳定，供热路径缓存）。
	if v.WithLabelValues("200", "GET") != a {
		t.Fatal("WithLabelValues returned a different instance for identical labels")
	}
}

func TestCounterVecSnapshotAndKeys(t *testing.T) {
	v := NewCounterVec("method")
	v.WithLabelValues("GET").Inc()
	v.WithLabelValues("PUT").Add(3)
	snap := v.Snapshot()
	if snap["GET"] != 1 || snap["PUT"] != 3 {
		t.Fatalf("snapshot = %v, want GET=1 PUT=3", snap)
	}
	keys := v.Keys()
	if len(keys) != 2 {
		t.Fatalf("keys = %v, want 2", keys)
	}
}

func TestCounterVecWritePrometheus(t *testing.T) {
	v := NewCounterVec("code")
	v.WithLabelValues("200").Inc()
	v.WithLabelValues("500").Add(2)
	var buf bytes.Buffer
	if err := v.WritePrometheus(&buf, "http_responses_total", "responses by code"); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "# TYPE http_responses_total counter") {
		t.Fatalf("missing TYPE line:\n%s", out)
	}
	if !strings.Contains(out, `http_responses_total{code="200"} 1`) {
		t.Fatalf("missing 200 series:\n%s", out)
	}
	if !strings.Contains(out, `http_responses_total{code="500"} 2`) {
		t.Fatalf("missing 500 series:\n%s", out)
	}
	if !strings.Contains(out, "# HELP http_responses_total responses by code") {
		t.Fatalf("missing HELP line:\n%s", out)
	}
}

func TestRegistryCounterVec(t *testing.T) {
	r := NewRegistry()
	cv := r.CounterVecWithHelp("http_responses_total", "responses by code and method", "code", "method")
	cv.WithLabelValues("200", "GET").Inc()
	cv.WithLabelValues("500", "GET").Inc()

	var buf bytes.Buffer
	if err := r.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, `http_responses_total{code="200",method="GET"} 1`) {
		t.Fatalf("registry prometheus missing labeled 200/GET:\n%s", out)
	}
	if !strings.Contains(out, `http_responses_total{code="500",method="GET"} 1`) {
		t.Fatalf("registry prometheus missing labeled 500/GET:\n%s", out)
	}
	if !strings.Contains(out, "# HELP http_responses_total responses by code and method") {
		t.Fatalf("registry prometheus missing HELP:\n%s", out)
	}

	// Snapshot 应包含 counterVecs 维度。
	snap := r.Snapshot()
	cvecs, ok := snap["counterVecs"].(map[string]map[string]int64)
	if !ok {
		t.Fatalf("snapshot missing counterVecs: %v", snap)
	}
	if cvecs["http_responses_total"]["200\x1fGET"] != 1 {
		t.Fatalf("snapshot counterVecs = %v", cvecs)
	}
}
