package main

import "testing"

func TestClientMDel(t *testing.T) {
	srv, store := newStatefulKVServer(t)
	defer srv.Close()
	c := NewClient(srv.URL)

	// 预置 5 个 key，其中 one 将被删除成功，其余也成功。
	store.put("a", "1")
	store.put("b", "2")
	store.put("c", "3")
	res := c.MDel([]string{"a", "b", "c", "d"})
	if res.Total != 4 {
		t.Fatalf("Total 应为 4，实际 %d", res.Total)
	}
	if res.Deleted != 4 {
		t.Fatalf("期望删除 4 个（含不存在的 d），实际 Deleted=%d Errors=%v", res.Deleted, res.Errors)
	}
	if _, ok := store.get("a"); ok {
		t.Fatal("a 应已删除")
	}
	if len(res.Errors) != 0 {
		t.Fatalf("期望无错误，实际 %v", res.Errors)
	}

	// 空输入安全
	empty := c.MDel(nil)
	if empty.Total != 0 || empty.Deleted != 0 {
		t.Fatal("空输入应返回空结果")
	}
}
