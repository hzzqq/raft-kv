package transport

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
)

// GobCodec 使用 encoding/gob 的二进制编解码器：比 JSON 更紧凑、编解码更快，
// 适合 Go 两端互通的内部 RPC（跨语言场景仍建议 JSONCodec）。零外部依赖。
type GobCodec struct{}

// Marshal 用 gob 序列化 v。
func (GobCodec) Marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, fmt.Errorf("transport: gob marshal: %w", err)
	}
	return buf.Bytes(), nil
}

// Unmarshal 用 gob 反序列化进 v（v 必须是指针）。
func (GobCodec) Unmarshal(data []byte, v interface{}) error {
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(v); err != nil {
		return fmt.Errorf("transport: gob unmarshal: %w", err)
	}
	return nil
}

// SetCodec 替换客户端 InvokeMsg 使用的编解码器（默认 JSONCodec）。
// 必须与服务端 handler 的编解码保持一致；nil 视为空操作。并发安全。
func (cc *ClientConn) SetCodec(c Codec) {
	if c == nil {
		return
	}
	cc.mu.Lock()
	cc.codec = c
	cc.mu.Unlock()
}

// codecRef 在锁内取当前编解码器，避免与 SetCodec 并发读写竞态。
func (cc *ClientConn) codecRef() Codec {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.codec
}

// TypedHandler 把「类型化的业务函数」适配成 MethodHandler：
// 用 codec 解码请求→调用 fn→编码响应，消除服务端手写字节编解码的样板代码。
// codec 传 nil 时默认 JSONCodec。
func TypedHandler[Req any, Resp any](codec Codec, fn func(ctx context.Context, req *Req) (*Resp, error)) MethodHandler {
	if codec == nil {
		codec = JSONCodec{}
	}
	return func(ctx context.Context, reqData []byte) ([]byte, error) {
		var req Req
		if err := codec.Unmarshal(reqData, &req); err != nil {
			return nil, fmt.Errorf("transport: decode request: %w", err)
		}
		resp, err := fn(ctx, &req)
		if err != nil {
			return nil, err
		}
		return codec.Marshal(resp)
	}
}

// ServiceBuilder 以链式调用构建 ServiceDesc，便于集中注册方法。
type ServiceBuilder struct {
	desc ServiceDesc
}

// NewService 创建名为 name 的服务构建器。
func NewService(name string) *ServiceBuilder {
	return &ServiceBuilder{desc: ServiceDesc{Name: name, Methods: make(map[string]MethodHandler)}}
}

// Method 注册一个方法处理器（同名后者覆盖），返回自身以支持链式调用。
func (b *ServiceBuilder) Method(name string, h MethodHandler) *ServiceBuilder {
	b.desc.Methods[name] = h
	return b
}

// Build 产出最终 ServiceDesc，可直接交给 Server.Register。
func (b *ServiceBuilder) Build() ServiceDesc { return b.desc }
