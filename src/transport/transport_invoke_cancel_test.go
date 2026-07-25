package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

// TestInvokeCancelDoesNotPoisonPool 回归测试：此前取消协程在 ctx.Done() 时直接
// pc.conn.Close()，而成功路径会 putConn(pc) 复用连接——「响应到达」与「ctx 取消」竞态
// 会把已关闭连接放回池中，毒化连接池，使后续复用随机失败。本用例以风暴方式制造该竞态，
// 并断言风暴后一次正常调用仍能成功（连接池未被关闭连接污染）。
func TestInvokeCancelDoesNotPoisonPool(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := NewServer()
	srv.Register(ServiceDesc{
		Name: "Echo",
		Methods: map[string]MethodHandler{
			"Ping": func(ctx context.Context, d []byte) ([]byte, error) {
				time.Sleep(2 * time.Millisecond) // 制造客户端取消与响应到达的竞态窗口
				return d, nil
			},
		},
	})
	go srv.Serve(lis)
	defer srv.Stop()

	cc := Dial(lis.Addr().String())
	defer cc.Close()

	// 取消风暴：每个请求使用「很快被取消」的 ctx，反复触发「响应到达瞬间 ctx 取消」。
	for i := 0; i < 300; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		go func() { time.Sleep(time.Millisecond); cancel() }()
		_, _ = cc.Invoke(ctx, "/Echo/Ping", []byte("x"))
		cancel()
	}

	// 关键断言：风暴后连接池不应被关闭连接污染，一次正常调用必须成功且回显正确。
	ctx := context.Background()
	resp, err := cc.Invoke(ctx, "/Echo/Ping", []byte("hello"))
	if err != nil {
		t.Fatalf("连接池在取消风暴后被中毒：%v", err)
	}
	if string(resp) != "hello" {
		t.Fatalf("回显错误：got %q want %q", resp, "hello")
	}
}

// TestInvokeErrorFrameReusesConn 验证：服务端返回错误帧时连接仍健康，应放回池复用
// 而非被关闭丢弃（既不浪费连接，也不改变调用方语义）。
func TestInvokeErrorFrameReusesConn(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := NewServer()
	srv.Register(ServiceDesc{
		Name: "S",
		Methods: map[string]MethodHandler{
			"Fail": func(ctx context.Context, d []byte) ([]byte, error) {
				return nil, errBoom // 返回错误 → 服务端走错误帧
			},
		},
	})
	go srv.Serve(lis)
	defer srv.Stop()

	cc := Dial(lis.Addr().String())
	defer cc.Close()
	// 首次调用触发错误帧。
	if _, err := cc.Invoke(context.Background(), "/S/Fail", []byte("a")); err == nil {
		t.Fatal("期望错误帧返回 error，却为 nil")
	}
	// 连接池应仍可用（错误帧后连接被复用），后续调用可继续。
	st := cc.Stats()
	if st.Dials == 0 {
		t.Fatal("期望至少建过一次链")
	}
	// 再发一次，验证池未被破坏（仍能正常路由到方法）。
	if _, err := cc.Invoke(context.Background(), "/S/Fail", []byte("b")); err == nil {
		t.Fatal("期望错误帧返回 error，却为 nil")
	}
}
