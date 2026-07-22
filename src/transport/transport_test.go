package transport

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// startTestServer 在本机起一个 Server，注册 Echo 服务，返回监听地址与停止函数。
func startTestServer(t *testing.T) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := NewServer()
	srv.Register(ServiceDesc{
		Name: "Echo",
		Methods: map[string]MethodHandler{
			"Upper": func(ctx context.Context, req []byte) ([]byte, error) {
				return []byte(strings.ToUpper(string(req))), nil
			},
		},
	})
	go func() { _ = srv.Serve(lis) }()
	return lis.Addr().String(), func() { srv.Stop() }
}

func TestInvokeRoundTrip(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	cc := Dial(addr)
	got, err := cc.Invoke(context.Background(), "/Echo/Upper", []byte("hello"))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if string(got) != "HELLO" {
		t.Fatalf("got %q, want HELLO", got)
	}
}

func TestInvokeMsgTyped(t *testing.T) {
	// 用 JSON 编解码验证类型往返。独立起一个 Typ 服务（不复用 Echo 服务）。
	type req struct{ Msg string }
	type rep struct{ Msg string }
	srv := NewServer()
	srv.Register(ServiceDesc{
		Name: "Typ",
		Methods: map[string]MethodHandler{
			"Echo": func(ctx context.Context, data []byte) ([]byte, error) {
				var r req
				if err := json.Unmarshal(data, &r); err != nil {
					return nil, err
				}
				return json.Marshal(rep{Msg: r.Msg + "-echo"})
			},
		},
	})
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	defer lis.Close()
	go func() { _ = srv.Serve(lis) }()

	cc := Dial(lis.Addr().String())
	var out rep
	if err := cc.InvokeMsg(context.Background(), "/Typ/Echo", req{Msg: "hi"}, &out); err != nil {
		t.Fatalf("InvokeMsg: %v", err)
	}
	if out.Msg != "hi-echo" {
		t.Fatalf("got %q, want hi-echo", out.Msg)
	}
}

func TestInvokeUnknownMethod(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	cc := Dial(addr)
	_, err := cc.Invoke(context.Background(), "/Echo/Nope", []byte("x"))
	if err == nil {
		t.Fatal("expected error for unknown method")
	}
	if !strings.Contains(err.Error(), "method not found") {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestInvokeContextCanceled(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cc := Dial(addr)
	if _, err := cc.Invoke(ctx, "/Echo/Upper", []byte("x")); err == nil {
		t.Fatal("expected ctx error")
	}
}

func TestInvokeConcurrent(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cc := Dial(addr)
			got, err := cc.Invoke(context.Background(), "/Echo/Upper", []byte("abc"))
			if err != nil {
				errs <- err
				return
			}
			if string(got) != "ABC" {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent invoke failed: %v", err)
		}
	}
}

func TestServeStop(t *testing.T) {
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	srv := NewServer()
	go func() { _ = srv.Serve(lis) }()
	// 给 Accept 一点时间启动。
	time.Sleep(20 * time.Millisecond)
	srv.Stop()
	// Stop 后新连接应失败。
	cc := Dial(lis.Addr().String())
	_, err := cc.Invoke(context.Background(), "/Echo/Upper", []byte("x"))
	if err == nil {
		t.Fatal("expected connection refused after stop")
	}
}
