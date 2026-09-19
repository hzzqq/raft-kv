package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// readBrokenServer 起一个"读路径故障、写路径正常"的网关桩：GET 一律 500
//（模拟读路径瞬态故障/重试耗尽），PUT 正常入库。这是 Incr 读故障静默重置
// 计数器的精确杀伤窗口——Get 失败而 Put 成功时，缺陷以 (1, nil) 假成功落地。
func readBrokenServer(t *testing.T) (*httptest.Server, *kvStore) {
	t.Helper()
	store := &kvStore{m: map[string]string{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusInternalServerError)
		case http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			store.put(r.URL.Path[len("/kv/"):], string(b))
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	return srv, store
}

// TestClientIncrReadErrorFailClosed：Incr 读取阶段遇非 404 读故障（网络故障/
// 服务端 5xx/重试耗尽）必须 fail-closed 向上返回错误，而不是把读故障误判为
// "key 不存在从 0 起"——否则读故障窗口内已有计数（如 42）会被静默重置为 1，
// 并以 (1, nil) 假成功掩盖。仅网关明确答复 404（key 确实不存在）才允许从 0
// 开始（既有语义，见 TestClientIncr 与 TestClientIncrNotFoundStartsAtZero）。
func TestClientIncrReadErrorFailClosed(t *testing.T) {
	srv, store := readBrokenServer(t)
	defer srv.Close()
	store.put("cnt", "42")
	c := NewClient(srv.URL)

	v, err := c.Incr("cnt")
	if err == nil {
		t.Fatalf("读故障下 Incr 应返回错误，实际静默返回 %d（读故障被误判为 key 不存在，计数器被重置为 1）", v)
	}
	if v != 0 {
		t.Fatalf("读故障下 Incr 失败时新值应为 0，实际 %d", v)
	}
	if s, _ := store.get("cnt"); s != "42" {
		t.Fatalf("读故障下计数不得被改写，应保持 \"42\"，实际 %q", s)
	}
}

// TestClientIncrNotFoundStartsAtZero：404（key 确实不存在）保持既有语义——
// 从 0 开始自增并成功写入 1，不得被 fail-closed 修复误伤。
func TestClientIncrNotFoundStartsAtZero(t *testing.T) {
	srv, store := newStatefulKVServer(t)
	defer srv.Close()
	c := NewClient(srv.URL)

	v, err := c.Incr("brand-new")
	if err != nil || v != 1 {
		t.Fatalf("404（key 不存在）应从 0 起自增返回 1，实际 %d err=%v", v, err)
	}
	if s, _ := store.get("brand-new"); s != "1" {
		t.Fatalf("存储值应为 \"1\"，实际 %q", s)
	}
}
