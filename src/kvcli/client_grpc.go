package main

import (
	"context"
	"sync"

	"raftkv/src/transport"
)

// grpcKVReq / grpcKVResp 是 KV gRPC 风格接口的请求/响应体，JSON 结构与网关侧
// KvRequest/KvResponse 一致（二者分属不同 main 包，无法互导，按契约各定义一份）。
type grpcKVReq struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	OpType string `json:"op_type,omitempty"`
}
type grpcKVResp struct {
	Value string `json:"value,omitempty"`
	Err   string `json:"err,omitempty"`
}

// GRPCClient 是走 transport（gRPC 风格真实 TCP）的 KV 客户端，与 Client 同语义。
// 每次 RPC 由 transport 建链/拆链，并发安全；适用于网关以 gRPC 风格端口暴露的场景。
type GRPCClient struct {
	cc  *transport.ClientConn
	ctx context.Context
}

// DialGRPC 连接到网关的 gRPC 风格地址（host:port）。
func DialGRPC(addr string) *GRPCClient {
	return &GRPCClient{cc: transport.Dial(addr), ctx: context.Background()}
}

// Get 读取 key 的当前值（失败返回空串，与 Client.Get 语义一致）。
func (c *GRPCClient) Get(key string) string {
	var r grpcKVResp
	_ = c.cc.InvokeMsg(c.ctx, "/Kv/Get", &grpcKVReq{Key: key}, &r)
	return r.Value
}

// Put 写入 key=value。
func (c *GRPCClient) Put(key, value string) {
	_ = c.cc.InvokeMsg(c.ctx, "/Kv/Put", &grpcKVReq{Key: key, Value: value}, &grpcKVResp{})
}

// Append 追加 value 到 key。
func (c *GRPCClient) Append(key, value string) {
	_ = c.cc.InvokeMsg(c.ctx, "/Kv/Append", &grpcKVReq{Key: key, Value: value}, &grpcKVResp{})
}

// MSet 并发批量写入，返回失败 key→error 的映射（空映射表示全成功）。
func (c *GRPCClient) MSet(pairs map[string]string) map[string]error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	errs := make(map[string]error)
	for k, v := range pairs {
		wg.Add(1)
		go func(k, v string) {
			defer wg.Done()
			err := c.cc.InvokeMsg(c.ctx, "/Kv/Put", &grpcKVReq{Key: k, Value: v}, &grpcKVResp{})
			if err != nil {
				mu.Lock()
				errs[k] = err
				mu.Unlock()
			}
		}(k, v)
	}
	wg.Wait()
	return errs
}

// MGet 并发批量读取，返回 key→value 映射。
func (c *GRPCClient) MGet(keys []string) map[string]string {
	var wg sync.WaitGroup
	var mu sync.Mutex
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			var r grpcKVResp
			_ = c.cc.InvokeMsg(c.ctx, "/Kv/Get", &grpcKVReq{Key: k}, &r)
			mu.Lock()
			out[k] = r.Value
			mu.Unlock()
		}(k)
	}
	wg.Wait()
	return out
}
