package main

import (
	"compress/gzip"
	"io"
	"net/http"
	"strings"
)

// EnableGzip 开启 gzip 透明解压：回源请求带 Accept-Encoding: gzip，若响应
// Content-Encoding 为 gzip 则自动解压，对调用方透明（返回明文）。默认关闭，
// 对未启用压缩的调用方零行为影响。
func (c *Client) EnableGzip() {
	c.gzip = true
}

// readRespBody 读取响应体，若 Content-Encoding 含 gzip 则先解压后返回明文。
// 用于 Get 回源，使客户端在开启 gzip 时无需关心传输编码。
func readRespBody(resp *http.Response) ([]byte, error) {
	if strings.Contains(resp.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		return io.ReadAll(gz)
	}
	return io.ReadAll(resp.Body)
}
