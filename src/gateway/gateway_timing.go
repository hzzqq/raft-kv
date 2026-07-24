// gateway_timing.go —— 外层响应 writer 包装（I92 / #200 / #205）。
//
// 两个可观测能力都依赖「在首次写头冻结响应头之前注入」这一 net/http 契约
// （见 #169 ETag 修复的同一教训）：
//   - X-Process-Time：服务端处理耗时（TTFB 口径，毫秒三位小数）。
//   - X-Response-Size：响应体在「线上」的字节数（gzip 开启时为压缩后字节）。
//     对占绝大多数的「单次 Write 的 JSON 响应」精确等于完整 body；对分块/
//     流式多 Write 响应，记录首块大小（header 一旦冻结不可更改，属已知取舍）。
//
// 嵌入底层 ResponseWriter 保留其全部能力；Flush 透传以兼容流式响应。
package main

import (
	"net/http"
	"strconv"
	"time"
)

// processTimeHeader 是处理耗时响应头名。
const processTimeHeader = "X-Process-Time"

// respSizeHeader 是响应体大小响应头名（线上字节数）。
const respSizeHeader = "X-Response-Size"

// reqSizeHeader 是请求体大小响应头名（声明的内容长度 Content-Length）。
const reqSizeHeader = "X-Request-Size"

// metricsWriter 在首次写头前注入 X-Process-Time 与 X-Response-Size（首写时），
// 并累计响应字节数供请求级直方图上报（X-Response-Size 的 header 形式因头冻结
// 语义仅能反映首块，但进程内计数始终为完整线上字节）。X-Request-Size 反映入站
// 请求的声明体大小，便于识别超大请求（带宽/限流可观测）。
type metricsWriter struct {
	http.ResponseWriter
	start     time.Time
	wrote     bool
	respBytes int64
	reqSize   int64 // 入站请求体声明大小（来自 r.ContentLength），-1 表示未知（分块）
}

func (t *metricsWriter) inject() {
	if t.wrote {
		return
	}
	t.wrote = true
	ms := float64(time.Since(t.start).Microseconds()) / 1000.0
	t.Header().Set(processTimeHeader, strconv.FormatFloat(ms, 'f', 3, 64)+"ms")
	// X-Request-Size：仅当声明长度已知（>=0）时输出；分块上传 ContentLength=-1
	// 跳过，避免输出无意义负值。
	if t.reqSize >= 0 {
		t.Header().Set(reqSizeHeader, strconv.FormatInt(t.reqSize, 10))
	}
}

func (t *metricsWriter) WriteHeader(code int) {
	t.inject()
	// WriteHeader 时刻记录当前累计字节数：对「先写 body 再 WriteHeader」的 handler
	// 即完整 body；对纯状态（无 body）正确为 0。WriteHeader 之后头的冻结语义下，
	// 若 handler 再 Write 已无法更新（已知取舍，详见 Write 注释）。
	t.Header().Set(respSizeHeader, strconv.FormatInt(t.respBytes, 10))
	t.ResponseWriter.WriteHeader(code)
}

func (t *metricsWriter) Write(b []byte) (int, error) {
	t.respBytes += int64(len(b))
	// 首写前（头尚未冻结）把当前累计字节数写入 X-Response-Size；对单次 Write
	// 的 JSON 响应这正是完整 body。若 handler 先 WriteHeader（wrote 已置位），
	// 头已冻结，后续 Write 无法再改 X-Response-Size（net/http 契约，已知取舍）。
	if !t.wrote {
		t.Header().Set(respSizeHeader, strconv.FormatInt(t.respBytes, 10))
	}
	t.inject() // 未显式 WriteHeader 时 net/http 会隐式 200，这里先注入计时头
	return t.ResponseWriter.Write(b)
}

// Flush 透传（http.Flusher），保证流式端点不因包装而失去 flush 能力。
func (t *metricsWriter) Flush() {
	if f, ok := t.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
