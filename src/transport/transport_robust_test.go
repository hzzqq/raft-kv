package transport

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// TestHandlerErrorFrame 验证 handler 返回错误时，客户端收到错误帧而非数据帧。
func TestHandlerErrorFrame(t *testing.T) {
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	defer lis.Close()
	srv := NewServer()
	srv.Register(ServiceDesc{
		Name: "Bad",
		Methods: map[string]MethodHandler{
			"Fail": func(ctx context.Context, req []byte) ([]byte, error) {
				return nil, errTestBoom
			},
		},
	})
	go func() { _ = srv.Serve(lis) }()

	cc := Dial(lis.Addr().String())
	_, err := cc.Invoke(context.Background(), "/Bad/Fail", []byte("x"))
	if err == nil {
		t.Fatal("expected error from failing handler")
	}
	if err.Error() != errTestBoom.Error() {
		t.Fatalf("got %v, want %v", err, errTestBoom)
	}
}

// TestEmptyPayload 验证空请求体也能正常往返。
func TestEmptyPayload(t *testing.T) {
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	defer lis.Close()
	srv := NewServer()
	srv.Register(ServiceDesc{
		Name: "Echo",
		Methods: map[string]MethodHandler{
			"Bounce": func(ctx context.Context, req []byte) ([]byte, error) {
				return req, nil
			},
		},
	})
	go func() { _ = srv.Serve(lis) }()

	cc := Dial(lis.Addr().String())
	got, err := cc.Invoke(context.Background(), "/Echo/Bounce", nil)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty, got %q", got)
	}
}

// TestBinaryPayload 验证任意二进制请求体不损坏。
func TestBinaryPayload(t *testing.T) {
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	defer lis.Close()
	srv := NewServer()
	srv.Register(ServiceDesc{
		Name: "Echo",
		Methods: map[string]MethodHandler{
			"Bounce": func(ctx context.Context, req []byte) ([]byte, error) {
				return req, nil
			},
		},
	})
	go func() { _ = srv.Serve(lis) }()

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	cc := Dial(lis.Addr().String())
	got, err := cc.Invoke(context.Background(), "/Echo/Bounce", payload)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("len %d != %d", len(got), len(payload))
	}
	for i := range payload {
		if got[i] != payload[i] {
			t.Fatalf("byte %d mismatch", i)
		}
	}
}

// TestOversizedFrameRejected 验证客户端发送声称超大长度的帧时，服务端会因读满失败
// 而关闭连接（防御内存爆炸），客户端随后得到连接错误而非卡死。
func TestOversizedFrameRejected(t *testing.T) {
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	defer lis.Close()
	srv := NewServer()
	srv.Register(ServiceDesc{
		Name: "Echo",
		Methods: map[string]MethodHandler{
			"Bounce": func(ctx context.Context, req []byte) ([]byte, error) {
				return req, nil
			},
		},
	})
	go func() { _ = srv.Serve(lis) }()
	time.Sleep(20 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", lis.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	// 方法帧：合法短帧。
	var mh [5]byte
	mh[0] = frameData
	binary.BigEndian.PutUint32(mh[1:], 7)
	conn.Write(mh[:])
	conn.Write([]byte("/Echo/"))
	// 请求体帧：声称 1<<30 长度，但不再发送实际字节。
	var bh [5]byte
	bh[0] = frameData
	binary.BigEndian.PutUint32(bh[1:], 1<<30)
	conn.Write(bh[:])

	buf := make([]byte, 5)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	// 服务端应在尝试读满超大帧时失败并关闭连接，导致这里读不到完整响应。
	_, rerr := conn.Read(buf)
	if rerr == nil {
		t.Fatal("expected connection closed/error due to oversized frame, but read succeeded")
	}
}

// errTestBoom 是 handler 测试用的哨兵错误。
var errTestBoom = &boomError{}

type boomError struct{}

func (e *boomError) Error() string { return "boom" }
