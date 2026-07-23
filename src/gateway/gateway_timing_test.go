// gateway_timing_test.go —— X-Process-Time 计时头的 cluster-free 单测（#200）。
package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func timingTestServer() *Server {
	return &Server{
		sem:            make(chan struct{}, maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
		startedAt:      time.Now(),
	}
}

// TestGatewayProcessTimeHeader 验证：正常路径下发 X-Process-Time，格式为 "<毫秒三位小数>ms"。
func TestGatewayProcessTimeHeader(t *testing.T) {
	s := timingTestServer()
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	v := resp.Header.Get("X-Process-Time")
	if v == "" {
		t.Fatalf("missing X-Process-Time header")
	}
	if !strings.HasSuffix(v, "ms") {
		t.Fatalf("X-Process-Time = %q, want <float>ms", v)
	}
	num := strings.TrimSuffix(v, "ms")
	if dot := strings.Index(num, "."); dot < 0 || len(num)-dot-1 != 3 {
		t.Fatalf("X-Process-Time = %q, want 3 decimal places", v)
	}
}

// TestGatewayProcessTimeOnErrorPath 验证：4xx 早退路径（未知路由 404）同样带计时头。
func TestGatewayProcessTimeOnErrorPath(t *testing.T) {
	s := timingTestServer()
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/no/such/route")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("X-Process-Time") == "" {
		t.Fatalf("error path should also carry X-Process-Time")
	}
}

// TestGatewayProcessTimeReflectsDelay 验证：处理耗时（testDelay 注入 30ms）被真实反映（>=25ms）。
func TestGatewayProcessTimeReflectsDelay(t *testing.T) {
	s := timingTestServer()
	s.testDelay = 30 * time.Millisecond
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	v := strings.TrimSuffix(resp.Header.Get("X-Process-Time"), "ms")
	var ms float64
	if _, err := fmt.Sscanf(v, "%f", &ms); err != nil {
		t.Fatalf("parse %q: %v", v, err)
	}
	if ms < 25 {
		t.Fatalf("X-Process-Time = %.3fms, want >= 25ms (testDelay=30ms)", ms)
	}
}
