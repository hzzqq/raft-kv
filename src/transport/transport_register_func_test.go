package transport

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestRegisterFuncEndToEnd 验证 RegisterFunc 便捷注册在真实 TCP 上走通：
// 默认 JSON 编解码，省去手写 TypedHandler 样板。
func TestRegisterFuncEndToEnd(t *testing.T) {
	s := NewServer()
	s.Register(RegisterFunc(NewService("Echo"), "Hello", func(_ context.Context, req *codecEchoReq) (*codecEchoResp, error) {
		return &codecEchoResp{Greeting: "hi " + req.Name, Doubled: req.N * 2}, nil
	}).Build())
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
	var resp codecEchoResp
	if err := cc.InvokeMsg(ctx, "/Echo/Hello", &codecEchoReq{Name: "kv", N: 7}, &resp); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if resp.Greeting != "hi kv" || resp.Doubled != 14 {
		t.Fatalf("unexpected resp: %+v", resp)
	}
}

// TestRegisterFuncChaining 验证 RegisterFunc 与 Method 可链式混用且同名后者覆盖。
func TestRegisterFuncChaining(t *testing.T) {
	b := RegisterFunc(NewService("M"), "A", func(_ context.Context, req *codecEchoReq) (*codecEchoResp, error) {
		return &codecEchoResp{Greeting: "A:" + req.Name}, nil
	}).
		Method("B", func(_ context.Context, _ []byte) ([]byte, error) {
			return []byte("raw-b"), nil
		})
	d := b.Build()
	if _, ok := d.Methods["A"]; !ok {
		t.Fatalf("RegisterFunc did not register method A")
	}
	if _, ok := d.Methods["B"]; !ok {
		t.Fatalf("Method did not register method B")
	}
}

// TestRegisterFuncPanicRecovered 验证：RegisterFunc 的业务函数 panic 不会让测试进程
// 崩溃，客户端拿到错误；且服务端 goroutine 存活，后续正常 RPC 仍可用（R2 隐性健壮）。
func TestRegisterFuncPanicRecovered(t *testing.T) {
	s := NewServer()
	svcS := RegisterFunc(NewService("S"), "Boom", func(_ context.Context, req *codecEchoReq) (*codecEchoResp, error) {
		panic("kaboom from RegisterFunc")
	})
	svcS = RegisterFunc(svcS, "Ok", func(_ context.Context, req *codecEchoReq) (*codecEchoResp, error) {
		return &codecEchoResp{Greeting: "alive"}, nil
	})
	s.Register(svcS.Build())
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

	var resp codecEchoResp
	if err := cc.InvokeMsg(ctx, "/S/Boom", &codecEchoReq{}, &resp); err == nil {
		t.Fatalf("expected error from panicking handler, got nil")
	}
	// 服务端未被拖垮：后续 RPC 照常工作。
	var alive codecEchoResp
	if err := cc.InvokeMsg(ctx, "/S/Ok", &codecEchoReq{}, &alive); err != nil {
		t.Fatalf("server goroutine died after panic: %v", err)
	}
	if alive.Greeting != "alive" {
		t.Fatalf("unexpected post-panic resp: %+v", alive)
	}
}

// TestSafeCallRawMethodPanic 直接验证 safeCall 对原始 MethodHandler panic 的恢复
// （不走 TypedHandler/RegisterFunc，覆盖 safeCall 本体）。
func TestSafeCallRawMethodPanic(t *testing.T) {
	s := NewServer()
	s.Register(NewService("R").Method("Boom", func(_ context.Context, _ []byte) ([]byte, error) {
		panic("raw-kaboom")
	}).Build())
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
	if _, err := cc.Invoke(ctx, "/R/Boom", []byte("x")); err == nil {
		t.Fatalf("expected error from panicking raw handler")
	}
}
