package main

import (
	"context"
	"fmt"
	"net/http"
)

// Ping 存活探测：请求 /healthz，非 200 返回 error（含状态码）。用于客户端主动探活。
func (c *Client) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/healthz", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("kvcli: Ping /healthz status %d", resp.StatusCode)
	}
	return nil
}

// Healthy 存活探针：/healthz 返回 200 即 true。仅表示进程活着，集群未必就绪。
func (c *Client) Healthy(ctx context.Context) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/healthz", nil)
	if err != nil {
		return false
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// Ready 就绪探针：/readyz 返回 200 即 true（集群所有 group 有 leader 且无迁移卡滞）。
// 与 Healthy 区分——Ready=false 表示暂不能服务读写，调用方应降级或等待。注入的新需求。
func (c *Client) Ready(ctx context.Context) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/readyz", nil)
	if err != nil {
		return false
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
