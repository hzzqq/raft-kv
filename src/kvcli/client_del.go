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
		// 修复：成功（200）与业务错误（非 503/504 的非 200）两条出径都必须 Close 响应体，
		// 与 putCtx/appendCtx/fetchGet 同口径。此前仅 503/504 重试路径 Close，其余出径
		// 未关闭——未关闭响应体的连接无法归还连接池复用，每调用一次 Del/MDel 泄漏一个
		// 连接（fd 累积至 GC finalizer 兜底，长跑进程可耗尽 fd）。
		if resp.StatusCode != http.StatusOK {
			err := respErr("DELETE", key, resp)
			resp.Body.Close()
			return err
		}
		resp.Body.Close()
		return nil
	}
	return lastErr
}

// Del 删除单个 key。网关返回非 200 时返回错误（含响应体）。删除是幂等的，
// 对不存在的 key 网关桩同样返回 200（视为已删除）。
func (c *Client) Del(key string) error {
	return c.deleteCtx(context.Background(), key)
}
