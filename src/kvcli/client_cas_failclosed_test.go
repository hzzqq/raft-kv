package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// deadServerClient 返回指向"已关闭服务"的客户端：所有请求必然连接失败，
// 稳定复现网络故障（NewClient 默认 maxRetries=0，不会重试拖慢测试）。
func deadServerClient(t *testing.T) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()
	return NewClient(srv.URL)
}

// TestClientCasReadErrorFailClosed：Cas 读取阶段遇网络故障必须 fail-closed 向上
// 返回错误，而不是把读故障误判为"key 不存在"：
//   - expect="" 时，读故障被当成"不存在"会错误执行写入，覆盖真实存在的值（假 CAS 成功）；
//   - expect!="" 时，读故障被伪装成"期望不匹配"，调用方无从感知后端不可用。
//
// 仅网关明确返回 404（key 确实不存在）才允许以 "" 参与比较（见 TestClientCas）。
func TestClientCasReadErrorFailClosed(t *testing.T) {
	c := deadServerClient(t)

	ok, err := c.Cas("k", "", "v")
	if err == nil {
		t.Fatalf("网络故障下 Cas(expect=\"\") 应返回错误，实际静默成功 ok=%v（读故障被误判为 key 不存在）", ok)
	}
	if ok {
		t.Fatalf("网络故障下 Cas 不得报告交换成功")
	}

	ok2, err2 := c.Cas("k", "x", "v")
	if err2 == nil {
		t.Fatalf("网络故障下 Cas(expect!=\"\") 应返回错误，实际静默返回不匹配 ok=%v（读故障被伪装成期望不匹配）", ok2)
	}
}

// TestClientSetNXReadErrorFailClosed：SetNX 读取阶段遇网络故障必须 fail-closed
// 向上返回错误。SetNX 用于分布式锁初始化/幂等首写：把读故障当成"key 不存在"
// 会让多个调用方在网络抖动时同时"抢占成功"，破坏互斥语义。
func TestClientSetNXReadErrorFailClosed(t *testing.T) {
	c := deadServerClient(t)

	ok, err := c.SetNX("lock", "me")
	if err == nil {
		t.Fatalf("网络故障下 SetNX 应返回错误，实际静默成功 ok=%v（读故障被误判为 key 不存在，锁初始化可被重复抢占）", ok)
	}
	if ok {
		t.Fatalf("网络故障下 SetNX 不得报告设置成功")
	}
}

// TestIsNotFound：404 类型化判别——包装的 404 为真；普通错误（网络故障、
// 非 404 状态）与 nil 均为假。
func TestIsNotFound(t *testing.T) {
	if IsNotFound(nil) {
		t.Fatal("nil 不应判为 NotFound")
	}
	if IsNotFound(fmt.Errorf("GET /kv/k: status 500: boom")) {
		t.Fatal("普通网络/服务端错误不应判为 NotFound")
	}
	if !IsNotFound(&statusError{code: http.StatusNotFound, err: fmt.Errorf("GET /kv/k: status 404: (no body)")}) {
		t.Fatal("包装的 404 应判为 NotFound")
	}
}

// TestGetNotFoundTyped：Get 对网关 404 返回的错误必须可经 IsNotFound 判别，
// 且错误消息文本保持既有格式（含状态码与响应体，排障信息不丢失）。
func TestGetNotFoundTyped(t *testing.T) {
	srv, store := newStatefulKVServer(t)
	defer srv.Close()
	c := NewClient(srv.URL)

	_, err := c.Get("missing")
	if err == nil {
		t.Fatal("缺失 key 的 Get 应返回错误")
	}
	if !IsNotFound(err) {
		t.Fatalf("404 错误应可经 IsNotFound 判别，实际 err=%v (%T)", err, err)
	}
	if want := "GET /kv/missing: status 404: (no body)"; err.Error() != want {
		t.Fatalf("404 错误文本应保持兼容，实际 %q，期望 %q", err.Error(), want)
	}
	_ = store
}
