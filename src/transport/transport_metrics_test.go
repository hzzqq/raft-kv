package transport

import (
	"context"
	"net"
	"testing"
)

// TestClientServerMetrics 验证客户端与服务端的调用/字节/错误计数正确累积。
func TestClientServerMetrics(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer()
	srv.Register(ServiceDesc{
		Name: "Echo",
		Methods: map[string]MethodHandler{
			"Ping": func(ctx context.Context, req []byte) ([]byte, error) {
				return req, nil
			},
			"Fail": func(ctx context.Context, req []byte) ([]byte, error) {
				return nil, context.Canceled
			},
		},
	})
	go srv.Serve(lis)
	defer srv.Stop()

	cc := Dial(lis.Addr().String())
	defer cc.Close()
	cc.SetPool(2, 0) // 复用以便观察 dials 与 reused

	const n = 20
	body := []byte("hello-world")
	for i := 0; i < n; i++ {
		if _, err := cc.Invoke(context.Background(), "/Echo/Ping", body); err != nil {
			t.Fatalf("ping %d: %v", i, err)
		}
	}
	// 一次失败调用，应计入 Errs
	if _, err := cc.Invoke(context.Background(), "/Echo/Fail", nil); err == nil {
		t.Fatalf("expected error from Fail method")
	}

	cs := cc.Stats()
	if cs.RPCs != n+1 {
		t.Fatalf("client RPCs=%d, want %d", cs.RPCs, n+1)
	}
	if cs.Errs != 1 {
		t.Fatalf("client Errs=%d, want 1", cs.Errs)
	}
	if cs.BytesSent <= 0 || cs.BytesRecv <= 0 {
		t.Fatalf("client bytes not counted: sent=%d recv=%d", cs.BytesSent, cs.BytesRecv)
	}
	if cs.Dials != 1 {
		t.Fatalf("expected 1 dial (pool reuse), got %d", cs.Dials)
	}
	if cs.Reused < n-1 {
		t.Fatalf("expected heavy reuse, got reused=%d", cs.Reused)
	}

	sm := srv.Metrics()
	if sm.RPCs != n+1 {
		t.Fatalf("server RPCs=%d, want %d", sm.RPCs, n+1)
	}
	if sm.Errs != 1 {
		t.Fatalf("server Errs=%d, want 1", sm.Errs)
	}
	if sm.ConnsActive != 0 {
		t.Fatalf("server ConnsActive=%d, want 0 (all returned)", sm.ConnsActive)
	}
	if sm.BytesRecv <= 0 || sm.BytesSent <= 0 {
		t.Fatalf("server bytes not counted: recv=%d sent=%d", sm.BytesRecv, sm.BytesSent)
	}
}
