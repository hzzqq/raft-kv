package main

import "testing"

func TestClientCas(t *testing.T) {
	srv, store := newStatefulKVServer(t)
	defer srv.Close()
	c := NewClient(srv.URL)

	store.put("k", "old")
	// 期望匹配 → 交换成功
	ok, err := c.Cas("k", "old", "new")
	if err != nil || !ok {
		t.Fatalf("期望交换成功，实际 ok=%v err=%v", ok, err)
	}
	if v, _ := store.get("k"); v != "new" {
		t.Fatalf("交换后值应为 new，实际 %q", v)
	}
	// 期望不匹配 → 不交换
	ok2, err2 := c.Cas("k", "old", "wrong")
	if err2 != nil || ok2 {
		t.Fatalf("期望交换失败，实际 ok=%v err=%v", ok2, err2)
	}
	if v, _ := store.get("k"); v != "new" {
		t.Fatalf("不匹配时值应保持 new，实际 %q", v)
	}
	// 不存在的 key，expect="" → 视为设置成功
	ok3, _ := c.Cas("fresh", "", "init")
	if !ok3 {
		t.Fatal("expect=\"\" 对不存在 key 应交换成功")
	}
	if v, _ := store.get("fresh"); v != "init" {
		t.Fatalf("fresh 应被设为 init，实际 %q", v)
	}
}
