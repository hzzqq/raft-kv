package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// TestETagConditionalGet 验证 ETag 回写与 If-None-Match 命中时 304 不回源。
func TestETagConditionalGet(t *testing.T) {
	s := newCacheServer()
	s.SetETag(true)
	var calls atomic.Int32
	stub := func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		io.WriteString(w, `{"hello":"world"}`)
	}
	ts := httptest.NewServer(s.Wrap(stub))
	defer ts.Close()

	// 第一次 GET：200 + ETag
	req1, _ := http.NewRequest("GET", ts.URL+"/e", nil)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	if resp1.StatusCode != 200 {
		t.Fatalf("first GET status=%d", resp1.StatusCode)
	}
	etag := resp1.Header.Get("ETag")
	if etag == "" {
		t.Fatalf("expected ETag header on first GET")
	}

	// 带 If-None-Match 的第二次 GET：应 304 且不回源
	req2, _ := http.NewRequest("GET", ts.URL+"/e", nil)
	req2.Header.Set("If-None-Match", etag)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotModified {
		t.Fatalf("expected 304, got %d", resp2.StatusCode)
	}
	if c := calls.Load(); c != 1 {
		t.Fatalf("expected 1 backend call (304 short-circuits), got %d", c)
	}

	// 带错误 If-None-Match：应 200 且再次回源
	req3, _ := http.NewRequest("GET", ts.URL+"/e", nil)
	req3.Header.Set("If-None-Match", `"wrong"`)
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != 200 {
		t.Fatalf("expected 200 for mismatched etag, got %d", resp3.StatusCode)
	}
	if c := calls.Load(); c != 2 {
		t.Fatalf("expected 2 backend calls after mismatched etag, got %d", c)
	}
	_ = b1
}
