package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// 这些测试覆盖 kvcli 当前最薄弱的分支：client.go 中网关返回非 200 的错误路径、
// Bench 的错误计数、以及 percentile 边界，无需完整集群。

// TestClientNon200Errors：网关返回非 200 时，Get/Put/Append 必须返回错误
// （而非静默空串/成功），覆盖 client.go 中 StatusCode!=200 的错误分支。
func TestClientNon200Errors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	cl := NewClient(ts.URL)
	if _, err := cl.Get("foo"); err == nil {
		t.Fatal("Get 应在非 200 时返回错误")
	}
	if err := cl.Put("foo", "bar"); err == nil {
		t.Fatal("Put 应在非 200 时返回错误")
	}
	if err := cl.Append("foo", "baz"); err == nil {
		t.Fatal("Append 应在非 200 时返回错误")
	}
}

// TestBenchRecordsErrors：压测目标持续返回 500 时，Bench 应记录 Errors>0
// 且仍能算出延迟分位数（覆盖 Bench 内 errCount 累加与 percentile 计算路径）。
func TestBenchRecordsErrors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	cl := NewClient(ts.URL)
	res := cl.Bench(30, 2, "put", 16)
	if res.Errors == 0 {
		t.Fatal("Bench 应在目标报错时记录 Errors>0")
	}
	if res.Ops != 30 {
		t.Fatalf("Bench Ops=%d want 30", res.Ops)
	}
	if res.OpsPerSec <= 0 {
		t.Fatalf("Bench OpsPerSec=%.1f want >0", res.OpsPerSec)
	}
}

// TestPercentile：percentile 边界——空切片返回 0，单元素返回该值，分位数取位正确。
func TestPercentile(t *testing.T) {
	if got := percentile([]float64{}, 0.5); got != 0 {
		t.Fatalf("percentile(empty)=%v want 0", got)
	}
	if got := percentile([]float64{42}, 0.5); got != 42 {
		t.Fatalf("percentile([42])=%v want 42", got)
	}
	s := []float64{1, 2, 3, 4, 5}
	if got := percentile(s, 0.5); got != 3 {
		t.Fatalf("percentile(p50)=%v want 3", got)
	}
	if got := percentile(s, 0.99); got != 5 {
		t.Fatalf("percentile(p99)=%v want 5", got)
	}
}
