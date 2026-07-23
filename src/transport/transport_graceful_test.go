package transport

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestGracefulStopWaitsForInFlight 验证 GracefulStop 会阻塞等待在途 RPC 处理完毕，
// 而非立即返回；在途 RPC 最终成功完成。
func TestGracefulStopWaitsForInFlight(t *testing.T) {
	s := NewServer()
	s.Register(RegisterFunc(NewService("Slow"), "Echo", func(_ context.Context, req *codecEchoReq) (*codecEchoResp, error) {
		time.Sleep(50 * time.Millisecond)
		return &codecEchoResp{Greeting: "done"}, nil
	}).Build())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go s.Serve(lis)
	cc := Dial(lis.Addr().String())
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 发起一个慢 RPC（进入在途）
	done := make(chan error, 1)
	go func() {
		var r codecEchoResp
		done <- cc.InvokeMsg(ctx, "/Slow/Echo", &codecEchoReq{}, &r)
	}()
	time.Sleep(10 * time.Millisecond) // 确保 RPC 已在 handler 中

	start := time.Now()
	if err := s.GracefulStop(2 * time.Second); err != nil {
		t.Fatalf("graceful stop: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("graceful stop returned too early (%dms), did not wait for in-flight", elapsed.Milliseconds())
	}
	// 在途 RPC 应成功完成
	if err := <-done; err != nil {
		t.Fatalf("in-flight RPC failed after graceful stop: %v", err)
	}
}

// TestGracefulStopRejectsNewConns 验证 GracefulStop 后新连接无法建立（监听器已关）。
func TestGracefulStopRejectsNewConns(t *testing.T) {
	s := NewServer()
	s.Register(RegisterFunc(NewService("S"), "Ok", func(_ context.Context, req *codecEchoReq) (*codecEchoResp, error) {
		return &codecEchoResp{Greeting: "ok"}, nil
	}).Build())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go s.Serve(lis)
	cc := Dial(lis.Addr().String())
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var r codecEchoResp
	if err := cc.InvokeMsg(ctx, "/S/Ok", &codecEchoReq{}, &r); err != nil {
		t.Fatalf("pre-stop rpc: %v", err)
	}

	if err := s.GracefulStop(time.Second); err != nil {
		t.Fatalf("graceful stop: %v", err)
	}
	// 新客户端应连接失败
	cc2 := Dial(lis.Addr().String())
	defer cc2.Close()
	var r2 codecEchoResp
	if err := cc2.InvokeMsg(ctx, "/S/Ok", &codecEchoReq{}, &r2); err == nil {
		t.Fatalf("expected connection error after graceful stop, got nil")
	}
}

// TestGracefulStopTimeout 验证在途 RPC 超时就返回错误并报告残留数量。
func TestGracefulStopTimeout(t *testing.T) {
	s := NewServer()
	s.Register(RegisterFunc(NewService("Slow"), "Echo", func(_ context.Context, req *codecEchoReq) (*codecEchoResp, error) {
		time.Sleep(300 * time.Millisecond)
		return &codecEchoResp{}, nil
	}).Build())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go s.Serve(lis)
	cc := Dial(lis.Addr().String())
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		var r codecEchoResp
		_ = cc.InvokeMsg(ctx, "/Slow/Echo", &codecEchoReq{}, &r)
	}()
	time.Sleep(10 * time.Millisecond)
	if err := s.GracefulStop(50 * time.Millisecond); err == nil {
		t.Fatalf("expected timeout error from graceful stop")
	}
	// 清理在途 RPC 后再彻底停（避免测试退出时 handler 仍跑）
	s.Stop()
}
