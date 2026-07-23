package transport

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestClientWarmupReused 验证：Warmup 主动建链入池后，首次 Invoke 复用该连接，
// 不再产生额外建链（Dials 保持 1，Reused>=1）。
func TestClientWarmupReused(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer()
	srv.Register(ServiceDesc{
		Name: "Echo",
		Methods: map[string]MethodHandler{
			"Ping": func(ctx context.Context, req []byte) ([]byte, error) { return req, nil },
		},
	})
	go srv.Serve(lis)
	defer srv.Stop()

	cc := Dial(lis.Addr().String())
	defer cc.Close()
	cc.SetPool(2, 0)
	if err := cc.Warmup(); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	cs := cc.Stats()
	if cs.Dials != 1 {
		t.Fatalf("after warmup Dials=%d, want 1", cs.Dials)
	}
	if cs.Idle != 1 {
		t.Fatalf("after warmup Idle=%d, want 1", cs.Idle)
	}

	if _, err := cc.Invoke(context.Background(), "/Echo/Ping", []byte("hi")); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	cs = cc.Stats()
	if cs.Dials != 1 {
		t.Fatalf("first invoke should reuse warm connection, Dials=%d want 1", cs.Dials)
	}
	if cs.Reused < 1 {
		t.Fatalf("first invoke should reuse warm connection, Reused=%d", cs.Reused)
	}
}

// TestClientWarmupNoPoolNoop 验证：池关闭(maxIdle<=0)时 Warmup 为空操作，不建链。
func TestClientWarmupNoPoolNoop(t *testing.T) {
	cc := Dial("127.0.0.1:0")
	defer cc.Close()
	cc.SetPool(0, 0)
	if err := cc.Warmup(); err != nil {
		t.Fatalf("warmup with pool disabled should be noop, got %v", err)
	}
	if cs := cc.Stats(); cs.Dials != 0 {
		t.Fatalf("warmup should not dial when pool disabled, Dials=%d", cs.Dials)
	}
}

// TestSetDialTimeout 验证：SetDialTimeout 改变建链超时且 DialTimeout getter 反映之。
func TestSetDialTimeout(t *testing.T) {
	cc := Dial("127.0.0.1:0")
	defer cc.Close()
	if cc.DialTimeout() != 5*time.Second {
		t.Fatalf("default dial timeout = %v, want 5s", cc.DialTimeout())
	}
	cc.SetDialTimeout(250 * time.Millisecond)
	if cc.DialTimeout() != 250*time.Millisecond {
		t.Fatalf("after SetDialTimeout = %v, want 250ms", cc.DialTimeout())
	}
}
