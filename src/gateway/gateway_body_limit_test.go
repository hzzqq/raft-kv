// gateway_body_limit_test.go —— I215：写路径请求体限额语义回归
//
// 缺陷 A：未知长度（chunked）超发 body 被 handlePut/handleAppend 内的
// io.LimitReader(r.Body, maxBodySize) 静默截断为限额字节后仍写入存储并返回 200
// ——与 wrap「MaxBytesReader 兜底 413 拒绝（覆盖 chunked/流式超发）」的设计意图
// 矛盾：LimitReader 提前 EOF，MaxBytesError 永远不触发。
// 缺陷 B：SetMaxBodySize(0)（文档语义「<=0 表示不限制」）时 LimitReader(0) 读出
// 空 body——PUT 全部写入空串、Append 追加空串。
package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"raftkv/src/cluster"
)

func startBodyLimitCluster(t *testing.T) (*httptest.Server, *Server) {
	t.Helper()
	c := cluster.StartCluster(2, 3, 3, 0)
	s := NewServer(c)
	s.Init(2)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(func() {
		ts.Close()
		c.Cleanup()
	})
	return ts, s
}

// oversizedChunkedPut 构造一个不声明长度（强制 chunked 传输）的超限 PUT 请求。
// 用非常规 reader 类型绕过 http.NewRequest 的 ContentLength 推断。
func oversizedChunkedPut(t *testing.T, url string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, struct{ io.Reader }{bytes.NewReader(body)})
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = -1
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// getKvBody GET 指定 key 并返回 (状态码, 响应体)。
func getKvBody(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, string(b)
}

// TestOversizedUnknownLengthBodyRejected 缺陷 A：chunked 超发 body 必须被 413
// 拒绝，且不得把截断数据静默写入存储（PUT 截断落库 / Append 追加截断段）。
func TestOversizedUnknownLengthBodyRejected(t *testing.T) {
	ts, s := startBodyLimitCluster(t)
	s.SetMaxBodySize(64)

	t.Run("put truncates and lies 200", func(t *testing.T) {
		body := bytes.Repeat([]byte("x"), 128)
		resp := oversizedChunkedPut(t, ts.URL+"/kv/big", body)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Errorf("PUT oversized chunked body status = %d, want 413", resp.StatusCode)
		}
		// 反向验证：截断的 64 字节不得落库（修复前写入 body[:64]）。
		_, got := getKvBody(t, ts.URL+"/kv/big")
		if got == string(body[:64]) {
			t.Errorf("truncated %d-byte body was silently stored", 64)
		}
	})

	t.Run("append truncates and lies 200", func(t *testing.T) {
		// 先正常写入种子值（限额内）。
		resp, err := http.Post(ts.URL+"/kv/acc/append", "text/plain", strings.NewReader("seed"))
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		body := bytes.Repeat([]byte("y"), 128)
		apr, err := http.NewRequest(http.MethodPost, ts.URL+"/kv/acc/append", struct{ io.Reader }{bytes.NewReader(body)})
		if err != nil {
			t.Fatal(err)
		}
		apr.ContentLength = -1
		aresp, err := http.DefaultClient.Do(apr)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, aresp.Body)
		aresp.Body.Close()
		if aresp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Errorf("APPEND oversized chunked body status = %d, want 413", aresp.StatusCode)
		}
		// 反向验证：截断追加不得落库（修复前追加 64 个 "y"）。
		_, got := getKvBody(t, ts.URL+"/kv/acc")
		if got != "seed" {
			t.Errorf("GET /kv/acc after oversized append = %q, want %q (truncated segment must not be appended)", got, "seed")
		}
	})
}

// TestPutWithDisabledBodyLimitStoresFullValue 缺陷 B：SetMaxBodySize(0)
// （文档语义「<=0 表示不限制」）时 PUT/Append 必须写入完整 body，而非空串。
func TestPutWithDisabledBodyLimitStoresFullValue(t *testing.T) {
	ts, s := startBodyLimitCluster(t)
	s.SetMaxBodySize(0)

	putReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/disabled", strings.NewReader("hello"))
	resp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}

	apr, err := http.Post(ts.URL+"/kv/disabled/append", "text/plain", strings.NewReader("-world"))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, apr.Body)
	apr.Body.Close()

	_, got := getKvBody(t, ts.URL+"/kv/disabled")
	if got != "hello-world" {
		t.Fatalf("GET /kv/disabled with disabled body limit = %q, want \"hello-world\" (LimitReader(0) must not empty the body)", got)
	}
}
