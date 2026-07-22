package transport

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"
)

// TestServerIdleTimeoutReclaimsHalfOpen 验证：开启读空闲超时后，客户端建连并发送
// 方法帧后挂起（半开连接），服务端会在超时后主动关闭连接、回收 goroutine；
// 此类回收不计入 handler 错误（errs 不变，且不应产生任何完成的 RPC）。
func TestServerIdleTimeoutReclaimsHalfOpen(t *testing.T) {
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
	srv.SetIdleTimeout(150 * time.Millisecond)
	go srv.Serve(lis)
	defer srv.Stop()

	raw, err := net.DialTimeout("tcp", lis.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	w := bufio.NewWriter(raw)
	// 只发方法帧，随后挂起，模拟半开/慢速连接
	if err := writeFrame(w, frameData, []byte("/Echo/Ping")); err != nil {
		t.Fatal(err)
	}

	// 轮询直到服务端回收该连接（ConnsActive 回落 0）
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Metrics().ConnsActive == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if srv.Metrics().ConnsActive != 0 {
		t.Fatalf("half-open conn not reclaimed: ConnsActive=%d", srv.Metrics().ConnsActive)
	}
	if srv.Metrics().Errs != 0 {
		t.Fatalf("idle-timeout reclaim should not count as handler error, Errs=%d", srv.Metrics().Errs)
	}
	if srv.Metrics().RPCs != 0 {
		t.Fatalf("no RPC should have completed, RPCs=%d", srv.Metrics().RPCs)
	}
}

// TestServerIdleTimeoutNormalRPCUnaffected 验证：开启短空闲超时后，客户端在超时窗口内
// 正常完成 RPC（连续收发方法帧+请求帧+响应帧）不受影响，RPC 成功且计数正确。
func TestServerIdleTimeoutNormalRPCUnaffected(t *testing.T) {
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
	srv.SetIdleTimeout(150 * time.Millisecond)
	go srv.Serve(lis)
	defer srv.Stop()

	cc := Dial(lis.Addr().String())
	defer cc.Close()
	cc.SetPool(0, 0) // 每 RPC 建链/拆链，便于观察服务端连接随 RPC 结束回收

	for i := 0; i < 10; i++ {
		resp, err := cc.Invoke(context.Background(), "/Echo/Ping", []byte("ping"))
		if err != nil {
			t.Fatalf("rpc %d failed: %v", i, err)
		}
		if string(resp) != "ping" {
			t.Fatalf("rpc %d: resp=%q want ping", i, resp)
		}
	}

	if sm := srv.Metrics(); sm.RPCs != 10 || sm.Errs != 0 {
		t.Fatalf("metrics mismatch: RPCs=%d Errs=%d", sm.RPCs, sm.Errs)
	}
}
