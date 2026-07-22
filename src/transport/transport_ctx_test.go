package transport

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestCtxDeadlinePropagation 验证客户端 ctx 截止时间能传播到 TCP 连接，
// 慢 handler 下客户端应在截止时间附近快速失败，而非空等满全程。
func TestCtxDeadlinePropagation(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer()
	srv.Register(ServiceDesc{
		Name: "Slow",
		Methods: map[string]MethodHandler{
			"Wait": func(ctx context.Context, req []byte) ([]byte, error) {
				select {
				case <-time.After(300 * time.Millisecond):
					return []byte("done"), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		},
	})
	go srv.Serve(lis)
	defer srv.Stop()

	cc := Dial(lis.Addr().String())
	defer cc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = cc.Invoke(ctx, "/Slow/Wait", []byte("x"))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected deadline error, got nil")
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("client did not honor ctx deadline: took %v (want < 200ms)", elapsed)
	}
}

// TestCtxNoDeadlineNormal 验证带宽松 ctx 的正常调用不受影响（回归保护）。
func TestCtxNoDeadlineNormal(t *testing.T) {
	addr, stop := echoServer(t)
	defer stop()
	cc := Dial(addr)
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := cc.Invoke(ctx, "/Echo/Ping", []byte("hi")); err != nil {
		t.Fatalf("normal ctx call: %v", err)
	}
}

// TestCtxCancelledFast 验证无 deadline 但 ctx 被主动取消时，客户端能尽快从在途读返回。
func TestCtxCancelledFast(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer()
	srv.Register(ServiceDesc{
		Name: "Slow",
		Methods: map[string]MethodHandler{
			"Wait": func(ctx context.Context, req []byte) ([]byte, error) {
				<-time.After(500 * time.Millisecond)
				return []byte("done"), nil
			},
		},
	})
	go srv.Serve(lis)
	defer srv.Stop()

	cc := Dial(lis.Addr().String())
	defer cc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err = cc.Invoke(ctx, "/Slow/Wait", []byte("x"))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected cancellation error, got nil")
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("ctx cancel did not abort in-flight read: took %v", elapsed)
	}
}
