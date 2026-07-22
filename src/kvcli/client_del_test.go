package main

import (
	"testing"

	"net/http"
)

func TestClientDel(t *testing.T) {
	srv, store := newStatefulKVServer(t)
	defer srv.Close()
	c := NewClient(srv.URL)

	store.put("k1", "v1")
	if err := c.Del("k1"); err != nil {
		t.Fatalf("Del 应成功，实际 %v", err)
	}
	if _, ok := store.get("k1"); ok {
		t.Fatal("Del 后 key 应已被删除")
	}
	// 删除不存在的 key 也应成功（幂等）
	if err := c.Del("nope"); err != nil {
		t.Fatalf("删除不存在的 key 应成功，实际 %v", err)
	}
	// 网关非 200（如 405 错误方法）应返回带响应体的错误
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer bad.Close()
	c2 := NewClient(bad.URL)
	if err := c2.Del("x"); err == nil {
		t.Fatal("网关 405 应返回错误")
	}
}
