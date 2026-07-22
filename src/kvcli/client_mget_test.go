package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// newKVTestServer 起一个 httptest 网关桩：GET /kv/{key} 返回 "v:"+key；
// 特定 "missing" key 返回 404；其余方法拒绝。并发计数 GET 次数（验证回源次数）。
func newKVTestServer(t *testing.T, counts *int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && len(r.URL.Path) > len("/kv/") {
			key := r.URL.Path[len("/kv/"):]
			atomic.AddInt64(counts, 1)
			if key == "missing" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("v:" + key))
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
}

// TestClientMGetDistinct 验证：N 个不同 key 各回源一次，结果正确。
func TestClientMGetDistinct(t *testing.T) {
	var counts int64
	srv := newKVTestServer(t, &counts)
	defer srv.Close()
	c := NewClient(srv.URL)

	keys := []string{"a", "b", "c", "d"}
	res := c.MGet(keys)
	if len(res.Errors) != 0 {
		t.Fatalf("期望全成功，实际错误 %v", res.Errors)
	}
	if len(res.Results) != 4 {
		t.Fatalf("期望 4 条结果，实际 %d", len(res.Results))
	}
	for _, k := range keys {
		if res.Results[k] != "v:"+k {
			t.Fatalf("key %s 值错误：%q", k, res.Results[k])
		}
	}
	if atomic.LoadInt64(&counts) != 4 {
		t.Fatalf("期望后端 4 次 GET，实际 %d", counts)
	}
}

// TestClientMGetPartialError 验证：部分 key 失败不影响其余成功，错误按 key 归集。
func TestClientMGetPartialError(t *testing.T) {
	var counts int64
	srv := newKVTestServer(t, &counts)
	defer srv.Close()
	c := NewClient(srv.URL)

	res := c.MGet([]string{"a", "missing", "c"})
	if len(res.Results) != 2 || res.Results["a"] != "v:a" || res.Results["c"] != "v:c" {
		t.Fatalf("部分成功结果不符：%v", res.Results)
	}
	if len(res.Errors) != 1 || res.Errors["missing"] == nil {
		t.Fatalf("期望 missing 入错，实际 %v", res.Errors)
	}
}

// TestClientMGetEmpty 验证：空输入安全返回空结果，不触达后端。
func TestClientMGetEmpty(t *testing.T) {
	var counts int64
	srv := newKVTestServer(t, &counts)
	defer srv.Close()
	c := NewClient(srv.URL)

	res := c.MGet(nil)
	if len(res.Results) != 0 || len(res.Errors) != 0 {
		t.Fatalf("空输入应返回空结果，实际 Results=%v Errors=%v", res.Results, res.Errors)
	}
	if atomic.LoadInt64(&counts) != 0 {
		t.Fatalf("空输入不应触达后端，实际 %d", counts)
	}
}

// TestClientMGetDupKeysSingleFlight 验证：重复 key + 单飞 → 合并为一次回源。
func TestClientMGetDupKeysSingleFlight(t *testing.T) {
	var counts int64
	srv := newKVTestServer(t, &counts)
	defer srv.Close()
	c := NewClient(srv.URL)
	c.EnableSingleFlight()

	res := c.MGet([]string{"dup", "dup", "dup"})
	if len(res.Errors) != 0 || res.Results["dup"] != "v:dup" {
		t.Fatalf("结果不符 Results=%v Errors=%v", res.Results, res.Errors)
	}
	if atomic.LoadInt64(&counts) != 1 {
		t.Fatalf("期望单飞合并为 1 次回源，实际 %d", counts)
	}
}

// TestClientMGetReusesConnection 验证：开启缓存后第二次 MGet 命中缓存、零回源。
func TestClientMGetReusesConnection(t *testing.T) {
	var counts int64
	srv := newKVTestServer(t, &counts)
	defer srv.Close()
	c := NewClient(srv.URL)
	c.EnableCache(60*time.Second, 100) // 60s TTL

	keys := []string{"k1", "k2"}
	_ = c.MGet(keys)
	first := atomic.LoadInt64(&counts)
	_ = c.MGet(keys) // 命中缓存
	second := atomic.LoadInt64(&counts)
	if first != 2 {
		t.Fatalf("首次期望回源 2 次，实际 %d", first)
	}
	if second != 2 {
		t.Fatalf("二次期望零回源（全命中），实际累计 %d", second)
	}
}
