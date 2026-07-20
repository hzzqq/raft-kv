// client.go —— kvcli 的 HTTP 客户端核心
//
// Client 封装对网关（gateway）的 REST 调用：GET /kv/{key}、PUT /kv/{key}、
// POST /kv/{key}/append。核心方法可单测（对 httptest 起的网关发请求）。
package main

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client 是网关的 HTTP 客户端。
type Client struct {
	base string
	http *http.Client
}

// NewClient 构造客户端，base 为网关根地址（如 http://localhost:8080）。
func NewClient(base string) *Client {
	return &Client{
		base: strings.TrimRight(base, "/"),
		http: &http.Client{Timeout: 5 * time.Second},
	}
}

// Get 读取 key 的当前值。
func (c *Client) Get(key string) (string, error) {
	resp, err := c.http.Get(c.base + "/kv/" + url.PathEscape(key))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b), nil
}

// Put 写入 key = value。
func (c *Client) Put(key, value string) error {
	req, err := http.NewRequest(http.MethodPut, c.base+"/kv/"+url.PathEscape(key), strings.NewReader(value))
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// Append 把 value 追加到 key 的当前值之后。
func (c *Client) Append(key, value string) error {
	resp, err := c.http.Post(c.base+"/kv/"+url.PathEscape(key)+"/append", "text/plain", strings.NewReader(value))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}
