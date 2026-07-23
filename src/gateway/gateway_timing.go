// gateway_timing.go —— X-Process-Time 请求处理耗时响应头（I92 / #200）。
//
// 语义：从网关收到请求到「首字节响应」的服务端处理耗时（TTFB 口径），单位毫秒、
// 三位小数。响应头必须在首次 Write/WriteHeader 冻结头之前注入（net/http 契约，
// 见 #169 ETag 修复的同一教训），因此用外层 writer 在写头瞬间注入，任何 handler
// 路径（含 gzip / 缓存回放 / 错误早退之后的正常路径）都自动生效。
package main

import (
	"net/http"
	"strconv"
	"time"
)

// processTimeHeader 是处理耗时响应头名。
const processTimeHeader = "X-Process-Time"

// timingWriter 在首次写头前注入 X-Process-Time。嵌入接口保留底层 writer 的
// 其余能力；Flush 透传以兼容流式响应。
type timingWriter struct {
	http.ResponseWriter
	start time.Time
	wrote bool
}

func (t *timingWriter) inject() {
	if t.wrote {
		return
	}
	t.wrote = true
	ms := float64(time.Since(t.start).Microseconds()) / 1000.0
	t.Header().Set(processTimeHeader, strconv.FormatFloat(ms, 'f', 3, 64)+"ms")
}

func (t *timingWriter) WriteHeader(code int) {
	t.inject()
	t.ResponseWriter.WriteHeader(code)
}

func (t *timingWriter) Write(b []byte) (int, error) {
	t.inject() // 未显式 WriteHeader 时 net/http 会隐式 200，这里先注入
	return t.ResponseWriter.Write(b)
}

// Flush 透传（http.Flusher），保证流式端点不因包装而失去 flush 能力。
func (t *timingWriter) Flush() {
	if f, ok := t.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
