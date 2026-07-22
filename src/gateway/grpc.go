package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"

	"raftkv/src/shardkv"
	"raftkv/src/transport"
)

// KvRequest 是 gRPC 风格 KV 服务的请求体（与 REST 路由的 body 同构）。
type KvRequest struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	OpType string `json:"op_type,omitempty"` // Put / Append，仅写类请求使用
}

// KvResponse 是 Kv 服务的响应体；Err 非空表示失败。
type KvResponse struct {
	Value string `json:"value,omitempty"`
	Err   string `json:"err,omitempty"`
}

// KvService 是 KV 语义的服务契约。网关把 shardkv.Clerk 适配成它，
// 测试可注入内存 stub，从而让 gRPC 传输层完全 cluster-free 验证。
type KvService interface {
	Get(ctx context.Context, req *KvRequest) (*KvResponse, error)
	Put(ctx context.Context, req *KvRequest) (*KvResponse, error)
	Append(ctx context.Context, req *KvRequest) (*KvResponse, error)
}

// kvServiceDesc 把 KvService 适配成 transport.ServiceDesc：
// 每个方法 JSON 解请求、调服务、JSON 编响应；服务返回 error 时走错误帧。
func kvServiceDesc(svc KvService) transport.ServiceDesc {
	wrap := func(fn func(context.Context, *KvRequest) (*KvResponse, error)) transport.MethodHandler {
		return func(ctx context.Context, reqData []byte) ([]byte, error) {
			var req KvRequest
			if err := json.Unmarshal(reqData, &req); err != nil {
				return nil, err
			}
			resp, err := fn(ctx, &req)
			if err != nil {
				return nil, err
			}
			return json.Marshal(resp)
		}
	}
	return transport.ServiceDesc{
		Name: "Kv",
		Methods: map[string]transport.MethodHandler{
			"Get":    wrap(svc.Get),
			"Put":    wrap(svc.Put),
			"Append": wrap(svc.Append),
		},
	}
}

// clerkKvService 把 shardkv.Clerk 适配成 KvService（生产路径）。
type clerkKvService struct{ ck *shardkv.Clerk }

func newKVServiceFromClerk(ck *shardkv.Clerk) KvService { return &clerkKvService{ck: ck} }

func (s *clerkKvService) Get(ctx context.Context, req *KvRequest) (*KvResponse, error) {
	v, err := s.ck.GetE(req.Key)
	if err != "" {
		return &KvResponse{}, fmt.Errorf("%s", err)
	}
	return &KvResponse{Value: v}, nil
}

func (s *clerkKvService) Put(ctx context.Context, req *KvRequest) (*KvResponse, error) {
	if err := s.ck.PutE(req.Key, req.Value); err != "" {
		return &KvResponse{}, fmt.Errorf("%s", err)
	}
	return &KvResponse{}, nil
}

func (s *clerkKvService) Append(ctx context.Context, req *KvRequest) (*KvResponse, error) {
	if err := s.ck.AppendE(req.Key, req.Value); err != "" {
		return &KvResponse{}, fmt.Errorf("%s", err)
	}
	return &KvResponse{}, nil
}

// ServeGRPCWith 在给定监听器上用 transport 暴露 KvService（真实 TCP，非内存桩）。
// 阻塞直至监听器关闭或出错。
func (s *Server) ServeGRPCWith(lis net.Listener, svc KvService) error {
	s.mu.Lock()
	s.grpcLis = lis
	s.mu.Unlock()
	tsvr := transport.NewServer()
	tsvr.Register(kvServiceDesc(svc))
	return tsvr.Serve(lis)
}

// ServeGRPC 用网关自身的 shardkv.Clerk 作为后端暴露 gRPC 风格服务。
func (s *Server) ServeGRPC(lis net.Listener) error {
	return s.ServeGRPCWith(lis, newKVServiceFromClerk(s.clerk))
}
