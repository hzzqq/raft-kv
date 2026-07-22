package main

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"

	"raftkv/src/transport"
)

// stubKVServer 起一个内存 KV 的 transport 服务，供 GRPCClient cluster-free 验证。
func stubKVServer(t *testing.T) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	store := map[string]string{}
	var mu sync.Mutex
	handle := func(fn func(*grpcKVReq) grpcKVResp) transport.MethodHandler {
		return func(ctx context.Context, d []byte) ([]byte, error) {
			var q grpcKVReq
			if err := json.Unmarshal(d, &q); err != nil {
				return nil, err
			}
			mu.Lock()
			r := fn(&q)
			mu.Unlock()
			return json.Marshal(r)
		}
	}
	tsvr := transport.NewServer()
	tsvr.Register(transport.ServiceDesc{
		Name: "Kv",
		Methods: map[string]transport.MethodHandler{
			"Get": handle(func(q *grpcKVReq) grpcKVResp { return grpcKVResp{Value: store[q.Key]} }),
			"Put": handle(func(q *grpcKVReq) grpcKVResp { store[q.Key] = q.Value; return grpcKVResp{} }),
			"Append": handle(func(q *grpcKVReq) grpcKVResp {
				store[q.Key] += q.Value
				return grpcKVResp{}
			}),
		},
	})
	go func() { _ = tsvr.Serve(lis) }()
	return lis.Addr().String(), func() { tsvr.Stop() }
}

func TestGRPCClientBasic(t *testing.T) {
	addr, stop := stubKVServer(t)
	defer stop()

	cli := DialGRPC(addr)
	cli.Put("a", "1")
	cli.Append("a", "2")
	if got := cli.Get("a"); got != "12" {
		t.Fatalf("Get a = %q, want 12", got)
	}
}

func TestGRPCClientBatch(t *testing.T) {
	addr, stop := stubKVServer(t)
	defer stop()

	cli := DialGRPC(addr)
	errs := cli.MSet(map[string]string{"x": "X", "y": "Y", "z": "Z"})
	if len(errs) != 0 {
		t.Fatalf("MSet unexpected errors: %v", errs)
	}
	got := cli.MGet([]string{"x", "y", "z", "missing"})
	if got["x"] != "X" || got["y"] != "Y" || got["z"] != "Z" {
		t.Fatalf("MGet mismatch: %v", got)
	}
	if got["missing"] != "" {
		t.Fatalf("missing key should be empty, got %q", got["missing"])
	}
}
