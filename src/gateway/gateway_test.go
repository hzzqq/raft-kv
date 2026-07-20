// gateway_test.go —— 用 httptest 覆盖网关的三种操作
package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"raftkv/src/cluster"
)

func TestGatewayHTTP(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)

	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// PUT /kv/foo = bar
	putReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/foo", strings.NewReader("bar"))
	resp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	// GET /kv/foo -> bar
	resp2, err := http.Get(ts.URL + "/kv/foo")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if string(b) != "bar" {
		t.Fatalf("GET /kv/foo = %q, want \"bar\"", string(b))
	}

	// POST /kv/foo/append = -baz
	resp3, err := http.Post(ts.URL+"/kv/foo/append", "text/plain", strings.NewReader("-baz"))
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()

	resp4, _ := http.Get(ts.URL + "/kv/foo")
	b4, _ := io.ReadAll(resp4.Body)
	resp4.Body.Close()
	if string(b4) != "bar-baz" {
		t.Fatalf("GET /kv/foo after append = %q, want \"bar-baz\"", string(b4))
	}

	// GET /healthz -> 200
	h, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	if h.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", h.StatusCode)
	}
	h.Body.Close()

	// GET /metrics -> 200 + valid JSON containing "counters"
	m, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	if m.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", m.StatusCode)
	}
	mb, _ := io.ReadAll(m.Body)
	m.Body.Close()
	var parsed map[string]interface{}
	if err := json.Unmarshal(mb, &parsed); err != nil {
		t.Fatalf("GET /metrics body is not valid JSON: %v (body=%s)", err, string(mb))
	}
	if _, ok := parsed["counters"]; !ok {
		t.Fatalf("GET /metrics JSON missing \"counters\" key: %s", string(mb))
	}
	if _, ok := parsed["histograms"]; !ok {
		t.Fatalf("GET /metrics JSON missing \"histograms\" key: %s", string(mb))
	}
}
