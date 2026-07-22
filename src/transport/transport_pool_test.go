package transport

import (
	"context"
	"net"
	"testing"
	"time"
)

// echoServer 起一个本地 TCP echo 服务，返回地址与停止函数。
func echoServer(t *testing.T) (string, func()) {
	t.Helper()
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
		},
	})
	go srv.Serve(lis)
	return lis.Addr().String(), func() { srv.Stop() }
}

func TestConnPoolReuseSequential(t *testing.T) {
	addr, stop := echoServer(t)
	defer stop()
	cc := Dial(addr)
	defer cc.Close()
	cc.SetPool(4, time.Minute)
	for i := 0; i < 10; i++ {
		if _, err := cc.Invoke(context.Background(), "/Echo/Ping", []byte("hi")); err != nil {
			t.Fatalf("invoke %d: %v", i, err)
		}
	}
	st := cc.Stats()
	if st.Dials != 1 {
		t.Fatalf("expected 1 dial (full reuse), got %d", st.Dials)
	}
	if st.Reused < 9 {
		t.Fatalf("expected >=9 reused, got %d", st.Reused)
	}
}

func TestConnPoolIdleCap(t *testing.T) {
	addr, stop := echoServer(t)
	defer stop()
	cc := Dial(addr)
	defer cc.Close()
	cc.SetPool(2, time.Minute) // 空闲上限 2
	for i := 0; i < 10; i++ {
		if _, err := cc.Invoke(context.Background(), "/Echo/Ping", []byte("x")); err != nil {
			t.Fatalf("invoke %d: %v", i, err)
		}
	}
	if st := cc.Stats(); st.Idle > 2 {
		t.Fatalf("idle exceeds cap: %d (want <=2)", st.Idle)
	}
}

func TestConnPoolIdleExpiry(t *testing.T) {
	addr, stop := echoServer(t)
	defer stop()
	cc := Dial(addr)
	defer cc.Close()
	cc.SetPool(4, 10*time.Millisecond) // 极短空闲回收
	cc.Invoke(context.Background(), "/Echo/Ping", []byte("x"))
	time.Sleep(30 * time.Millisecond) // 超过 idleTimeout
	cc.Invoke(context.Background(), "/Echo/Ping", []byte("x"))
	if st := cc.Stats(); st.Dials != 2 {
		t.Fatalf("expected 2 dials after idle expiry, got %d", st.Dials)
	}
}

func TestConnPoolDisabled(t *testing.T) {
	addr, stop := echoServer(t)
	defer stop()
	cc := Dial(addr)
	defer cc.Close()
	cc.SetPool(0, 0) // 关闭池
	const n = 8
	for i := 0; i < n; i++ {
		if _, err := cc.Invoke(context.Background(), "/Echo/Ping", []byte("x")); err != nil {
			t.Fatalf("invoke %d: %v", i, err)
		}
	}
	if st := cc.Stats(); st.Dials != n {
		t.Fatalf("pool disabled: expected %d dials, got %d", n, st.Dials)
	}
	if st := cc.Stats(); st.Idle != 0 {
		t.Fatalf("pool disabled: expected 0 idle, got %d", st.Idle)
	}
}

func TestConnPoolCloseReleases(t *testing.T) {
	addr, stop := echoServer(t)
	defer stop()
	cc := Dial(addr)
	cc.SetPool(4, time.Minute)
	cc.Invoke(context.Background(), "/Echo/Ping", []byte("x"))
	if cc.Stats().Idle != 1 {
		t.Fatalf("expected 1 idle before close")
	}
	if err := cc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// 关闭后再次调用应失败（连接已释放、池已关闭）
	if _, err := cc.Invoke(context.Background(), "/Echo/Ping", []byte("x")); err == nil {
		t.Fatalf("expected error after Close()")
	}
}
