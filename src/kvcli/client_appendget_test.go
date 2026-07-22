package main

import "testing"

func TestClientAppendGet(t *testing.T) {
	srv, store := newStatefulKVServer(t)
	defer srv.Close()
	c := NewClient(srv.URL)

	// 不存在 → append 即设置，返回新值
	v, err := c.AppendGet("log", "a")
	if err != nil || v != "a" {
		t.Fatalf("首次 AppendGet 应返回 \"a\"，实际 %q err=%v", v, err)
	}
	// 再追加
	v2, _ := c.AppendGet("log", "b")
	if v2 != "ab" {
		t.Fatalf("第二次 AppendGet 应返回 \"ab\"，实际 %q", v2)
	}
	if s, _ := store.get("log"); s != "ab" {
		t.Fatalf("存储值应为 \"ab\"，实际 %q", s)
	}
}
