package main

import "testing"

func TestClientSetNX(t *testing.T) {
	srv, store := newStatefulKVServer(t)
	defer srv.Close()
	c := NewClient(srv.URL)

	// 不存在 → 设置成功
	ok, err := c.SetNX("lock", "me")
	if err != nil || !ok {
		t.Fatalf("首次 SetNX 应成功，实际 ok=%v err=%v", ok, err)
	}
	if v, _ := store.get("lock"); v != "me" {
		t.Fatalf("lock 应被设为 me，实际 %q", v)
	}
	// 已存在 → 不覆盖
	ok2, err2 := c.SetNX("lock", "other")
	if err2 != nil || ok2 {
		t.Fatalf("已存在时 SetNX 应失败，实际 ok=%v err=%v", ok2, err2)
	}
	if v, _ := store.get("lock"); v != "me" {
		t.Fatalf("已存在时不应覆盖，实际 %q", v)
	}
}
