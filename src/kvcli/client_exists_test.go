package main

import "testing"

func TestClientExists(t *testing.T) {
	srv, store := newStatefulKVServer(t)
	defer srv.Close()
	c := NewClient(srv.URL)

	store.put("present", "x")
	store.put("also", "y")
	res := c.Exists([]string{"present", "also", "absent"})
	if !res["present"] || !res["also"] {
		t.Fatalf("已存在 key 应判为 true：%v", res)
	}
	if res["absent"] {
		t.Fatalf("不存在 key 应判为 false：%v", res)
	}
	// 空输入安全
	if m := c.Exists(nil); len(m) != 0 {
		t.Fatalf("空输入应返回空映射，实际 %v", m)
	}
}
