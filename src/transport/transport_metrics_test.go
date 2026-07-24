package transport

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

// TestServerMetricsPerMethod 验证 RPC 框架层可观测性埋点：serveConn 在每次 RPC 后
// 按方法累计调用次数、错误次数，并记录延迟直方图。直接起真实 TCP 服务端/客户端驱动，
// 断言埋点与调用行为一致（R6 可验证收益）。
func TestServerMetricsPerMethod(t *testing.T) {
	s := NewServer()
	d := NewService("T").
		Method("Ping", func(_ context.Context, _ []byte) ([]byte, error) {
			return []byte("pong"), nil
		}).
		Method("Fail", func(_ context.Context, _ []byte) ([]byte, error) {
			return nil, fmt.Errorf("boom")
		}).
		Build()
	s.Register(d)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go s.Serve(lis)
	defer s.Stop()

	cc := Dial(lis.Addr().String())
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	bPing := Metrics.Counter("rpc_/T/Ping").Value()
	bFail := Metrics.Counter("rpc_/T/Fail").Value()
	bErr := Metrics.Counter("transport_rpc_errors").Value()
	bLat := Metrics.Histogram("transport_rpc_latency_ms").Snapshot().Count

	// Ping 两次（成功）。
	for i := 0; i < 2; i++ {
		if _, err := cc.Invoke(ctx, "/T/Ping", nil); err != nil {
			t.Fatalf("Ping #%d: %v", i, err)
		}
	}
	// Fail 一次（handler 返回错误）。
	if _, err := cc.Invoke(ctx, "/T/Fail", nil); err == nil {
		t.Fatalf("Fail 期望返回错误, 实际 nil")
	}

	if got := Metrics.Counter("rpc_/T/Ping").Value(); got < bPing+2 {
		t.Fatalf("rpc_/T/Ping 增量不足: before=%d after=%d", bPing, got)
	}
	if got := Metrics.Counter("rpc_/T/Fail").Value(); got < bFail+1 {
		t.Fatalf("rpc_/T/Fail 增量不足: before=%d after=%d", bFail, got)
	}
	if got := Metrics.Counter("transport_rpc_errors").Value(); got < bErr+1 {
		t.Fatalf("transport_rpc_errors 增量不足: before=%d after=%d", bErr, got)
	}
	if got := Metrics.Histogram("transport_rpc_latency_ms").Snapshot().Count; got < bLat+3 {
		t.Fatalf("transport_rpc_latency_ms 样本数增量不足: before=%d after=%d", bLat, got)
	}
}
