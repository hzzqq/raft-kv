package main

import (
	"net/http"
	"strconv"
	"time"
)

// retryAfterSeconds 解析响应的 Retry-After 头（仅支持整数秒形式），
// 返回应等待时长与是否命中。网关限流 429 / 集群 503 会下发该头；客户端据此退避
// 而非盲目使用固定退避，更贴合服务端期望。超过 5s 截断以保护调用方。
func retryAfterSeconds(resp *http.Response) (time.Duration, bool) {
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0, false
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		d := time.Duration(n) * time.Second
		if d > 5*time.Second {
			d = 5 * time.Second
		}
		return d, true
	}
	return 0, false
}
