package main

import "testing"

func TestClientIncr(t *testing.T) {
	srv, store := newStatefulKVServer(t)
	defer srv.Close()
	c := NewClient(srv.URL)

	// 不存在 → 从 1 开始
	v, err := c.Incr("cnt")
	if err != nil || v != 1 {
		t.Fatalf("首次 Incr 应返回 1，实际 %d err=%v", v, err)
	}
	// 再次 → 2
	v2, _ := c.Incr("cnt")
	if v2 != 2 {
		t.Fatalf("第二次 Incr 应返回 2，实际 %d", v2)
	}
	if s, _ := store.get("cnt"); s != "2" {
		t.Fatalf("存储值应为 \"2\"，实际 %q", s)
	}
	// 非整数当前值按 0 处理 → 1
	store.put("bad", "notanumber")
	v3, _ := c.Incr("bad")
	if v3 != 1 {
		t.Fatalf("非整数当前值应视作 0，Incr 返回 1，实际 %d", v3)
	}
}
