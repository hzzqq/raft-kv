package main

import (
	"testing"
)

func TestClientPipeline(t *testing.T) {
	srv, store := newStatefulKVServer(t)
	defer srv.Close()
	c := NewClient(srv.URL)

	// 预置 a，批内所有操作互不依赖（无同 key 读写竞态、无读后写顺序假设）。
	store.put("a", "1")
	ops := []BatchOp{
		{Kind: "get", Key: "a"},
		{Kind: "put", Key: "b", Value: "2"},
		{Kind: "put", Key: "c", Value: "3"},
		{Kind: "put", Key: "d", Value: "4"},
		{Kind: "get", Key: "missing"},
		{Kind: "bad", Key: "z"},
	}
	res := c.Pipeline(ops)
	if len(res) != len(ops) {
		t.Fatalf("结果长度应等于操作数，实际 %d", len(res))
	}
	if res[0].Value != "1" || res[0].Err != nil {
		t.Fatalf("get a 应返回 \"1\"，实际 %+v", res[0])
	}
	if res[1].Err != nil || res[2].Err != nil || res[3].Err != nil {
		t.Fatalf("put 操作应全部成功，实际 %+v %+v %+v", res[1], res[2], res[3])
	}
	if res[4].Err == nil {
		t.Fatal("get missing 应返回错误（404）")
	}
	if res[5].Err == nil {
		t.Fatal("未知 op kind 应返回错误")
	}
	// 存储校验：b/c/d 均被正确写入
	if v, _ := store.get("b"); v != "2" {
		t.Fatalf("b 应为 \"2\"，实际 %q", v)
	}
	if v, _ := store.get("c"); v != "3" {
		t.Fatalf("c 应为 \"3\"，实际 %q", v)
	}
	if v, _ := store.get("d"); v != "4" {
		t.Fatalf("d 应为 \"4\"，实际 %q", v)
	}
	// 空输入安全
	if r := c.Pipeline(nil); len(r) != 0 {
		t.Fatalf("空输入应返回空切片，实际 %v", r)
	}
}
