package transport

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"
)

type codecEchoReq struct {
	Name string
	N    int
}

type codecEchoResp struct {
	Greeting string
	Doubled  int
}

// TestGobCodecRoundTrip 验证 GobCodec 编解码往返一致，且产物比 JSON 可解析域独立。
func TestGobCodecRoundTrip(t *testing.T) {
	c := GobCodec{}
	in := codecEchoReq{Name: "raft", N: 21}
	b, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out codecEchoReq
	if err := c.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != in {
		t.Fatalf("round trip mismatch: %+v != %+v", out, in)
	}
	// gob 产物不应是合法 JSON（确认确实换了编码而非静默退回）
	var js interface{}
	if json.Unmarshal(b, &js) == nil {
		t.Fatalf("gob output unexpectedly parses as JSON")
	}
}

// TestGobCodecUnmarshalError 验证坏字节返回带上下文的错误而非 panic。
func TestGobCodecUnmarshalError(t *testing.T) {
	var out codecEchoReq
	if err := (GobCodec{}).Unmarshal([]byte{0xff, 0x00, 0x01}, &out); err == nil {
		t.Fatalf("expected error on garbage bytes")
	}
}

// TestTypedHandlerEndToEnd 用 TypedHandler+ServiceBuilder+GobCodec 打通真实 TCP RPC。
func TestTypedHandlerEndToEnd(t *testing.T) {
	s := NewServer()
	s.Register(NewService("Echo").
		Method("Hello", TypedHandler(GobCodec{}, func(_ context.Context, req *codecEchoReq) (*codecEchoResp, error) {
			return &codecEchoResp{Greeting: "hi " + req.Name, Doubled: req.N * 2}, nil
		})).
		Build())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go s.Serve(lis)
	defer s.Stop()

	cc := Dial(lis.Addr().String())
	defer cc.Close()
	cc.SetCodec(GobCodec{})

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

// TestTypedHandlerDecodeError 验证请求解码失败返回错误帧（客户端拿到 error）。
func TestTypedHandlerDecodeError(t *testing.T) {
	h := TypedHandler(GobCodec{}, func(_ context.Context, req *codecEchoReq) (*codecEchoResp, error) {
		return &codecEchoResp{}, nil
	})
	if _, err := h(context.Background(), []byte("not-gob")); err == nil {
		t.Fatalf("expected decode error")
	}
}

// TestTypedHandlerNilCodecDefaultsJSON 验证 codec 传 nil 时默认 JSON。
func TestTypedHandlerNilCodecDefaultsJSON(t *testing.T) {
	h := TypedHandler[codecEchoReq, codecEchoResp](nil, func(_ context.Context, req *codecEchoReq) (*codecEchoResp, error) {
		return &codecEchoResp{Greeting: req.Name}, nil
	})
	out, err := h(context.Background(), []byte(`{"Name":"json","N":1}`))
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	var resp codecEchoResp
	if err := json.Unmarshal(out, &resp); err != nil || resp.Greeting != "json" {
		t.Fatalf("bad response: %s err=%v", out, err)
	}
}

// TestSetCodecConcurrent 验证 SetCodec 与 codecRef 并发无竞态（配合 -race 时更严格，
// 本环境无 cgo 亦可验证互斥正确性不死锁）。
func TestSetCodecConcurrent(t *testing.T) {
	cc := Dial("127.0.0.1:1") // 不实际建链
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); cc.SetCodec(GobCodec{}) }()
		go func() { defer wg.Done(); _ = cc.codecRef() }()
	}
	wg.Wait()
	if cc.codecRef() == nil {
		t.Fatalf("codec should never be nil")
	}
	cc.SetCodec(nil) // nil 空操作
	if cc.codecRef() == nil {
		t.Fatalf("nil SetCodec must be no-op")
	}
}
