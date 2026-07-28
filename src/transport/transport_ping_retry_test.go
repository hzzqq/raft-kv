package transport

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestPing 验证应用层保活帧：Ping 成功往返，且连接被放回池中复用（顺带预热）。
func TestPing(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer()
	srv.Register(ServiceDesc{Name: "Echo", Methods: map[string]MethodHandler{
		"Hi": func(ctx context.Context, req []byte) ([]byte, error) { return req, nil },
	}})
	go srv.Serve(lis)
	defer srv.Stop()

	cc := Dial(lis.Addr().String())
	defer cc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cc.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if got := cc.Stats().Idle; got != 1 {
		t.Fatalf("after Ping idle=%d want 1 (连接应放回池)", got)
	}
	// ping 后的连接可直接复用跑真 RPC。
	resp, err := cc.Invoke(ctx, "/Echo/Hi", []byte("x"))
	if err != nil || string(resp) != "x" {
		t.Fatalf("Invoke after Ping: resp=%q err=%v", resp, err)
	}
	if reused := cc.Stats().Reused; reused < 1 {
		t.Fatalf("reused=%d want >=1 (应复用 ping 预热的连接)", reused)
	}
}

// TestPingServerDown 验证对端不可达时 Ping 返回错误而非挂起。
func TestPingServerDown(t *testing.T) {
	// 占一个端口再立即释放，保证大概率无人监听。
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()

	cc := Dial(addr)
	cc.SetDialTimeout(300 * time.Millisecond)
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cc.Ping(ctx); err == nil {
		t.Fatal("Ping 到无人监听地址应报错")
	}
}

// TestDialRetry 验证有限拨号重试：服务端延迟启动，首拨失败、重试后成功。
func TestDialRetry(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close() // 先让端口空置，制造首拨 connection refused

	cc := Dial(addr)
	cc.SetDialTimeout(300 * time.Millisecond)
	cc.SetDialRetry(6, 100*time.Millisecond) // 最多 6 次尝试，指数退避
	defer cc.Close()

	// 250ms 后在同一地址拉起服务端（落在第 2~3 次重试窗口内）。
	go func() {
		time.Sleep(250 * time.Millisecond)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return // 端口被抢占则本测试靠 fatal 超时兜底
		}
		srv := NewServer()
		srv.Register(ServiceDesc{Name: "Echo", Methods: map[string]MethodHandler{
			"Hi": func(ctx context.Context, req []byte) ([]byte, error) { return req, nil },
		}})
		go srv.Serve(l)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := cc.Invoke(ctx, "/Echo/Hi", []byte("retry"))
	if err != nil {
		t.Fatalf("重试后 Invoke 仍失败: %v", err)
	}
	if string(resp) != "retry" {
		t.Fatalf("resp=%q want retry", resp)
	}
}
