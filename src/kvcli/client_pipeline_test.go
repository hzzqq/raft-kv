package main

import (
	"testing"
)

func TestClientPipeline(t *testing.T) {
	srv, store := newStatefulKVServer(t)
	defer srv.Close()
	c := NewClient(srv.URL)

	store.put("a", "1")
	ops := []BatchOp{
		{Kind: "get", Key: "a"},
		{Kind: "put", Key: "b", Value: "2"},
		{Kind: "append", Key: "a", Value: "x"},
		{Kind: "get", Key: "b"},
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
	if res[1].Err != nil {
		t.Fatalf("put b 应成功，实际 %v", res[1].Err)
	}
	if res[2].Err != nil {
		t.Fatalf("append a 应成功，实际 %v", res[2].Err)
	}
	if res[3].Value != "2" {
		t.Fatalf("get b 应返回 \"2\"，实际 %q", res[3].Value)
	}
	if res[4].Err == nil {
		t.Fatal("get missing 应返回错误（404）")
	}
	if res[5].Err == nil {
		t.Fatal("未知 op kind 应返回错误")
	}
	// 最终存储校验：a 应为 "1x"
	if v, _ := store.get("a"); v != "1x" {
		t.Fatalf("a 最终应为 \"1x\"，实际 %q", v)
	}
	// 空输入安全
	if r := c.Pipeline(nil); len(r) != 0 {
		t.Fatalf("空输入应返回空切片，实际 %v", r)
	}
}
