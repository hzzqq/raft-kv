package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// TestClientMSet 验证：并发批量写全部成功、各回源一次、结果归集正确。
func TestClientMSet(t *testing.T) {
	var backend atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	pairs := map[string]string{"a": "1", "b": "2", "c": "3"}
	res := c.MSet(pairs)
	if res.Total != 3 || res.Failed != 0 {
		t.Fatalf("期望全部成功 total=3 failed=0，实际 %+v", res)
	}
	if len(res.Results) != 3 || len(res.Errors) != 0 {
		t.Fatalf("期望 3 成功 0 失败，实际 %+v", res)
	}
	if backend.Load() != 3 {
		t.Fatalf("期望后端 3 次，实际 %d", backend.Load())
	}
}

// TestClientMSetPartialFailure 验证：部分 key 写失败被归集到 Errors，不阻断其它成功项。
func TestClientMSetPartialFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kv/bad" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	pairs := map[string]string{"ok1": "1", "bad": "2", "ok2": "3"}
	res := c.MSet(pairs)
	if res.Total != 3 || res.Failed != 1 {
		t.Fatalf("期望 total=3 failed=1，实际 %+v", res)
	}
	if len(res.Errors) != 1 || res.Errors["bad"] == nil {
		t.Fatalf("期望仅 bad 失败，实际 %+v", res)
	}
	if res.Results["bad"] == nil {
		t.Fatalf("期望 bad 的 Results 记录错误，实际 %+v", res)
	}
	if res.Results["ok1"] != nil || res.Results["ok2"] != nil {
		t.Fatalf("期望 ok1/ok2 成功(nil)，实际 %+v", res)
	}
}

// TestClientMSetEmpty 验证：空输入安全返回零结果、零回源。
func TestClientMSetEmpty(t *testing.T) {
	var backend atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	res := c.MSet(map[string]string{})
	if res.Total != 0 || res.Failed != 0 {
		t.Fatalf("期望空输入零结果，实际 %+v", res)
	}
	if backend.Load() != 0 {
		t.Fatalf("期望零回源，实际 %d", backend.Load())
	}
}
