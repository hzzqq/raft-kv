package main

import (
	"context"
	"net"
	"sync"
	"testing"

	"raftkv/src/transport"
)

// stubKvService 是内存版 KvService，供 gRPC 传输层 cluster-free 验证。
type stubKvService struct {
	mu   sync.Mutex
	data map[string]string
}

func newStubKvService() *stubKvService {
	return &stubKvService{data: make(map[string]string)}
}

func (s *stubKvService) Get(ctx context.Context, req *KvRequest) (*KvResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &KvResponse{Value: s.data[req.Key]}, nil
}

func (s *stubKvService) Put(ctx context.Context, req *KvRequest) (*KvResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[req.Key] = req.Value
	return &KvResponse{}, nil
}

func (s *stubKvService) Append(ctx context.Context, req *KvRequest) (*KvResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[req.Key] += req.Value
	return &KvResponse{}, nil
}

// startGRPC 起一个网关 gRPC 服务（基于 stub 后端），返回地址与停止函数。
func startGRPC(t *testing.T, svc KvService) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := NewServer(nil)
	go func() { _ = srv.ServeGRPCWith(lis, svc) }()
	return lis.Addr().String(), func() { lis.Close() }
}

func TestGatewayGRPCKV(t *testing.T) {
	svc := newStubKvService()
	addr, stop := startGRPC(t, svc)
	defer stop()

	cc := transport.Dial(addr)
	ctx := context.Background()

	// Put
	var pr KvResponse
	if err := cc.InvokeMsg(ctx, "/Kv/Put", &KvRequest{Key: "a", Value: "1"}, &pr); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Append
	var ar KvResponse
	if err := cc.InvokeMsg(ctx, "/Kv/Append", &KvRequest{Key: "a", Value: "2"}, &ar); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Get -> 应得到 "12"
	var gr KvResponse
	if err := cc.InvokeMsg(ctx, "/Kv/Get", &KvRequest{Key: "a"}, &gr); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if gr.Value != "12" {
		t.Fatalf("Get a = %q, want 12", gr.Value)
	}
}
