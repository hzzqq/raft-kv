package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// deleteCtx 是 Del 的纯回源逻辑（含重试），向网关发 DELETE /kv/{key}。
// 重试语义与 putCtx/appendCtx 一致：网络错误与 503/504 瞬态可重试；
// 其它非 200 视为业务错误并携带响应体（便于排障）。
func (c *Client) deleteCtx(ctx context.Context, key string) error {
	var lastErr error
	reqCtx, cancel := c.ctxForRequest(ctx)
	defer cancel()
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(c.backoffFor(attempt))
		}
		req, err := http.NewRequestWithContext(reqCtx, http.MethodDelete, c.base+"/kv/"+url.PathEscape(key), nil)
		if err != nil {
			return err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
			resp.Body.Close()
			lastErr = fmt.Errorf("retryable status %d for DELETE /kv/%s", resp.StatusCode, key)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return respErr("DELETE", key, resp)
		}
		return nil
	}
	return lastErr
}

// Del 删除单个 key。网关返回非 200 时返回错误（含响应体）。删除是幂等的，
// 对不存在的 key 网关桩同样返回 200（视为已删除）。
func (c *Client) Del(key string) error {
	return c.deleteCtx(context.Background(), key)
}
