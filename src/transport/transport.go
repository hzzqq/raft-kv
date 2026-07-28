// Package transport 提供零依赖、gRPC 风格的真实网络传输层。
//
// 设计要点：
//   - 真实 TCP 监听/连接（localhost 或跨机），非内存桩，是「真实网络传输」里程碑。
//   - 帧格式对齐 gRPC 的长度前缀帧：[1 字节消息类型标志][4 字节大端长度][payload]。
//     消息类型：0=数据帧，1=错误帧（payload 为错误文本）。
//   - 一次 RPC = 客户端顺序发送两帧（方法名帧 + 请求体帧），服务端回一帧（响应/错误）。
//   - 编解码默认 JSON（零依赖、可人工审查）；Handler 也接受裸字节，便于自定义编码。
//   - 客户端默认走连接池（maxIdle 个空闲连接复用），降低高并发建链开销；
//     SetPool(0,0) 可回退为 connection-per-RPC。池内连接无未决读，天然规避多路复用竞态，并发安全。
//   - 支持 ctx 截止时间传播（客户端设连接 deadline）与可选 TLS（crypto/tls，零外部依赖）。
//
// 之所以不引入 google.golang.org/grpc：当前构建环境不可联网安装外部模块，本包用标准库
// 复刻了 gRPC 的核心传输契约（长度前缀帧 + 方法路由 + 错误帧），足以支撑网关/客户端
// 走真实 TCP 通信，且不引入任何第三方依赖、可独立单测。
package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/src/metrics"
)

const (
	frameData  byte = 0
	frameError byte = 1
	// framePing 是应用层保活/健康探测帧：客户端发 ping（空 payload），服务端立即
	// 回一个同类型帧（pong），不经过任何 handler。用于跨机部署里探测对端是否存活、
	// 连接是否可用（比"发一次真 RPC 看是否报错"便宜且无副作用）。
	framePing byte = 2

	defaultMaxFrame = 16 << 20 // 16 MiB 单帧上限，防御超大帧打爆内存
)

// Metrics 是 transport 服务端(RPC 框架层)的可观测性指标（best-effort 进程级聚合）。
// 此前框架只在内置原子计数里记总量(bytesSent/rpcs/errs)，既无按方法拆分、也无延迟分布，
// 且未被统一 metrics 注册表纳管(无法走 Prometheus/JSON 暴露)。本注册表补齐这两点：
// 按方法计数的 RPC、错误数、以及整体 RPC 延迟直方图，便于定位"哪个方法慢/哪个方法在报错"。
var Metrics = metrics.NewRegistry()

// ErrMethodNotFound 表示方法未注册。
var ErrMethodNotFound = errors.New("transport: method not found")

// ErrClosed 表示 Server 已停止。
var ErrClosed = errors.New("transport: server closed")

// Codec 负责请求/响应体的序列化。默认 JSONCodec（零依赖）。
type Codec interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}

// JSONCodec 是默认编解码器，使用 encoding/json。
type JSONCodec struct{}

// Marshal 序列化 v 为 JSON 字节。
func (JSONCodec) Marshal(v interface{}) ([]byte, error) { return json.Marshal(v) }

// Unmarshal 把 JSON 字节反序列化进 v。
func (JSONCodec) Unmarshal(data []byte, v interface{}) error { return json.Unmarshal(data, v) }

// MethodHandler 处理单个 RPC：reqData 为请求体字节，返回响应体字节。
type MethodHandler func(ctx context.Context, reqData []byte) (respData []byte, err error)

// ServiceDesc 描述一个服务：名称 + 方法名→处理器映射。
type ServiceDesc struct {
	Name    string
	Methods map[string]MethodHandler
}

// ---- 帧读写 ----

func writeFrame(w *bufio.Writer, typ byte, payload []byte) error {
	if len(payload) > defaultMaxFrame {
		return fmt.Errorf("transport: frame too large: %d", len(payload))
	}
	var hdr [5]byte
	hdr[0] = typ
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	return w.Flush()
}

func readFrame(r *bufio.Reader) (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if n > defaultMaxFrame {
		return 0, nil, fmt.Errorf("transport: frame too large: %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, err
	}
	return hdr[0], buf, nil
}

func fullMethod(svc, method string) string { return "/" + svc + "/" + method }

// ---- Server ----

// Server 持有已注册方法的处理器，监听 TCP 并处理 RPC。
type Server struct {
	mu       sync.RWMutex
	handlers map[string]MethodHandler // "/Svc/Method" -> handler
	lis      net.Listener
	quit     chan struct{}
	closed   bool

	connsActive atomic.Int64
	inFlight    atomic.Int64
	rpcs        atomic.Int64
	bytesSent   atomic.Int64
	bytesRecv   atomic.Int64
	errs        atomic.Int64

	// idleTimeoutNanos 是读空闲超时（纳秒，atomic 存储以支持 Serve 后动态配置）。
	// >0 时，每个连接在两次帧读取之间的空闲若超过该值，将被主动关闭，从而回收
	// 半开/慢速连接占用的 goroutine；<=0 表示禁用（默认行为，与历史版本一致）。
	idleTimeoutNanos atomic.Int64
}

// NewServer 构造空 Server。默认禁用读空闲超时。
func NewServer() *Server {
	return &Server{handlers: make(map[string]MethodHandler), quit: make(chan struct{})}
}

// SetIdleTimeout 设置服务端读空闲超时：>0 时，连接在两次帧读取之间空闲超过该值将被关闭，
// 用于回收半开（建连后只发部分帧即 hang）或慢速连接占用的 goroutine；<=0 表示禁用（默认）。
func (s *Server) SetIdleTimeout(d time.Duration) {
	s.idleTimeoutNanos.Store(int64(d))
}

// IdleTimeout 返回当前读空闲超时配置（<=0 表示禁用）。
func (s *Server) IdleTimeout() time.Duration {
	return time.Duration(s.idleTimeoutNanos.Load())
}

// Register 注册一个服务的方法处理器（重复注册同名方法后者覆盖）。
func (s *Server) Register(desc ServiceDesc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for m, h := range desc.Methods {
		s.handlers[fullMethod(desc.Name, m)] = h
	}
}

// Serve 在给定监听器上循环接受连接并处理；Stop 或监听出错时返回。
func (s *Server) Serve(lis net.Listener) error {
	s.mu.Lock()
	s.lis = lis
	s.mu.Unlock()
	for {
		conn, err := lis.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return ErrClosed
			default:
				return err
			}
		}
		go s.serveConn(conn)
	}
}

// ServeTLS 在监听器上以 TLS 提供服务（cert 为已加载证书，可用 tls.X509KeyPair 构造）。
// 接受连接后先完成 TLS 握手再进入 RPC 处理，其余语义与 Serve 一致。
func (s *Server) ServeTLS(lis net.Listener, cert tls.Certificate) error {
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	s.mu.Lock()
	s.lis = lis
	s.mu.Unlock()
	for {
		conn, err := lis.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return ErrClosed
			default:
				return err
			}
		}
		go s.serveConn(tls.Server(conn, cfg))
	}
}

// safeCall 调用 handler 并恢复其 panic，保证单个 handler 崩溃不会拖垮整个服务端
// goroutine乃至进程（R2 隐性健壮性修复）。panic 被归一为错误帧，连接继续服务后续
// 请求。错误计数交回 serveConn 统一处理，避免重复累加。
func (s *Server) safeCall(h MethodHandler, ctx context.Context, reqData []byte) (resp []byte, herr error) {
	defer func() {
		if r := recover(); r != nil {
			herr = fmt.Errorf("transport: handler panic recovered: %v", r)
		}
	}()
	return h(ctx, reqData)
}

func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()
	s.connsActive.Add(1)
	defer s.connsActive.Add(-1)
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	// 每连接一个可取消 context：连接关闭（serveConn 退出）时取消，
	// handler 可借 connCtx.Done() 感知对端断开并及早中止，避免空转。
	connCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		// 读空闲超时：回收半开/慢速连接占用的 goroutine。每次读帧前刷新
		// deadline，因此 handler 处理期间的耗时不会误杀下一次读帧（handler
		// 在两次 readFrame 之间执行，下一次读帧前会重新设定 deadline）。
		if d := time.Duration(s.idleTimeoutNanos.Load()); d > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(d)); err != nil {
				return
			}
		}
		// 方法名帧
		typ, method, err := readFrame(r)
		if err != nil {
			return
		}
		if typ == framePing {
			// 保活探测：立即回 pong，不进 handler、不计入 RPC 统计。
			if err := writeFrame(w, framePing, nil); err != nil {
				return
			}
			continue
		}
		if typ != frameData {
			return // 协议错误：方法帧必须是数据帧
		}
		// 请求体帧
		typ, reqData, err := readFrame(r)
		if err != nil {
			return
		}
		if typ != frameData {
			return
		}
		s.mu.RLock()
		h, ok := s.handlers[string(method)]
		s.mu.RUnlock()
		if !ok {
			_ = writeFrame(w, frameError, []byte(ErrMethodNotFound.Error()))
			continue
		}
		s.inFlight.Add(1)
		rpcStart := time.Now()
		resp, herr := s.safeCall(h, connCtx, reqData)
		rpcDurMs := float64(time.Since(rpcStart).Microseconds()) / 1000.0
		s.inFlight.Add(-1)
		s.rpcs.Add(1)
		s.bytesRecv.Add(int64(len(reqData)))
		// 框架层可观测性（best-effort，纯原子操作，零行为影响）：
		// 按方法累计 RPC 次数、错误次数，并记录整体 RPC 延迟分布。
		mName := "rpc_" + string(method)
		Metrics.CounterWithHelp(mName, "累计 RPC 次数(按方法)").Inc()
		Metrics.HistWithHelp("transport_rpc_latency_ms", "RPC 端到端延迟(毫秒)分布").Record(rpcDurMs)
		if herr != nil {
			s.errs.Add(1)
			Metrics.CounterWithHelp("transport_rpc_errors", "累计 handler 返回错误的 RPC 次数").Inc()
			_ = writeFrame(w, frameError, []byte(herr.Error()))
			continue
		}
		s.bytesSent.Add(int64(len(resp)))
		_ = writeFrame(w, frameData, resp)
	}
}

// Stop 停止接受新连接并关闭监听器。幂等。
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.quit)
	if s.lis != nil {
		_ = s.lis.Close()
	}
}

// GracefulStop 优雅关闭：停止接受新连接，但等待所有在途 RPC 处理完毕（连接本身
// 保持，不强制断开）后再返回；超时则返回错误（仍残留 in-flight 数量）。幂等。
// 与 Stop 的区别：Stop 立即关闭监听器、在途 RPC 继续在后台跑完但调用方不等待；
// GracefulStop 会阻塞到在途清空，适合需要"先排空再退出"的滚动发布场景。
func (s *Server) GracefulStop(timeout time.Duration) error {
	s.Stop()
	deadline := time.Now().Add(timeout)
	for {
		if s.inFlight.Load() == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("transport: graceful stop timed out, %d in-flight RPCs remain", s.inFlight.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

// InFlight 返回当前在途 RPC 数（仅已读到完整请求帧、进入 handler 调用阶段的请求）。
func (s *Server) InFlight() int64 { return s.inFlight.Load() }

// ServerMetrics 是 Server 的观测快照。
type ServerMetrics struct {
	ConnsActive int64 // 当前活动连接数
	InFlight    int64 // 当前在途 RPC 数
	RPCs        int64 // 累计 RPC 次数
	BytesSent   int64 // 累计响应字节
	BytesRecv   int64 // 累计请求字节
	Errs        int64 // 累计 handler 错误数
}

// Metrics 返回服务端累计观测指标（仅供观测/测试）。
func (s *Server) Metrics() ServerMetrics {
	return ServerMetrics{
		ConnsActive: s.connsActive.Load(),
		InFlight:    s.inFlight.Load(),
		RPCs:        s.rpcs.Load(),
		BytesSent:   s.bytesSent.Load(),
		BytesRecv:   s.bytesRecv.Load(),
		Errs:        s.errs.Load(),
	}
}

// ---- ClientConn ----

// pooledConn 是池化的底层连接，复用 bufio 读写器以避免重复分配。
type pooledConn struct {
	conn   net.Conn
	r      *bufio.Reader
	w      *bufio.Writer
	usedAt time.Time
}

// ClientConn 是到某 target（host:port）的 gRPC 风格客户端。
// 通过连接池复用空闲 TCP 连接（受 maxIdle 限制），降低高并发下的建链开销；并发安全。
type ClientConn struct {
	target      string
	codec       Codec
	dialTO      time.Duration
	maxIdle     int
	idleTimeout time.Duration
	tlsCfg      *tls.Config
	// dialAttempts / dialBackoff：建链瞬时失败（connection refused / 超时）时的有限重试，
	// 提升跨机部署里"对端尚未起来"场景的鲁棒性。默认 1 次（不重试），保持既有行为。
	dialAttempts int
	dialBackoff  time.Duration

	mu     sync.Mutex
	idle   []*pooledConn
	dials  int
	reused int
	closed bool

	rpcs      atomic.Int64
	bytesSent atomic.Int64
	bytesRecv atomic.Int64
	errs      atomic.Int64
}

// Dial 构造到 target 的客户端（不立即建链）。默认开启连接池（maxIdle=4，空闲 30s 回收）。
func Dial(target string) *ClientConn {
	return &ClientConn{target: target, codec: JSONCodec{}, dialTO: 5 * time.Second, maxIdle: 4, idleTimeout: 30 * time.Second}
}

// DialTLS 构造到 target 的 TLS 客户端（不立即建链）。cfg 用于握手校验（如 InsecureSkipVerify）。
// 连接池与明文客户端一致，TLS 会话在首次握手后随连接复用。
func DialTLS(target string, cfg *tls.Config) *ClientConn {
	cc := Dial(target)
	cc.tlsCfg = cfg
	return cc
}

// SetPool 配置连接池：maxIdle 为最大空闲连接数（<=0 表示关闭池、每次 RPC 建链/拆链），
// idleTimeout 为空闲连接最大存活时间。
func (cc *ClientConn) SetPool(maxIdle int, idleTimeout time.Duration) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.maxIdle = maxIdle
	cc.idleTimeout = idleTimeout
	if cc.maxIdle <= 0 {
		for _, pc := range cc.idle {
			pc.conn.Close()
		}
		cc.idle = nil
	}
}

// ClientStats 是 ClientConn 的观测快照。
type ClientStats struct {
	Dials     int // 自建链次数
	Reused    int // 复用空闲连接次数
	Idle      int // 当前空闲连接数
	RPCs      int64
	BytesSent int64
	BytesRecv int64
	Errs      int64
}

// Stats 返回客户端的连接池与调用统计（仅供观测/测试）。
func (cc *ClientConn) Stats() ClientStats {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return ClientStats{
		Dials:     cc.dials,
		Reused:    cc.reused,
		Idle:      len(cc.idle),
		RPCs:      cc.rpcs.Load(),
		BytesSent: cc.bytesSent.Load(),
		BytesRecv: cc.bytesRecv.Load(),
		Errs:      cc.errs.Load(),
	}
}

// SetDialTimeout 设置建链超时（默认 5s），仅影响后续新建连接。
func (cc *ClientConn) SetDialTimeout(d time.Duration) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.dialTO = d
}

// SetDialRetry 配置建链失败的有限重试：attempts 为总尝试次数（<=1 表示不重试，默认），
// backoff 为首次重试前的等待，之后按 2 倍指数退避。适合跨机部署"对端尚未起来/瞬时抖动"
// 场景；注意重试会拉长单次 Invoke 的最坏耗时，raft 这类自带重试的上层不必开启。
func (cc *ClientConn) SetDialRetry(attempts int, backoff time.Duration) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.dialAttempts = attempts
	cc.dialBackoff = backoff
}

// DialTimeout 返回当前建链超时配置。
func (cc *ClientConn) DialTimeout() time.Duration {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.dialTO
}

// Warmup 主动建立一条空闲连接放入池中（池开启时），降低首次 Invoke 的建链延迟尖刺；
// 池关闭(maxIdle<=0)或已关闭时为空操作，返回错误仅在建链失败。
func (cc *ClientConn) Warmup() error {
	cc.mu.Lock()
	if cc.closed {
		cc.mu.Unlock()
		return ErrClosed
	}
	if cc.maxIdle <= 0 {
		cc.mu.Unlock()
		return nil
	}
	cc.mu.Unlock()
	pc, err := cc.getConn()
	if err != nil {
		return err
	}
	cc.putConn(pc)
	return nil
}

// getConn 取一条可用连接：优先复用空闲池中的健康连接，否则新建。
func (cc *ClientConn) getConn() (*pooledConn, error) {
	cc.mu.Lock()
	if cc.closed {
		cc.mu.Unlock()
		return nil, ErrClosed
	}
	dialTO := cc.dialTO // 锁内拷出，避免 SetDialTimeout 并发写竞态
	attempts := cc.dialAttempts
	backoff := cc.dialBackoff
	for len(cc.idle) > 0 {
		pc := cc.idle[len(cc.idle)-1]
		cc.idle = cc.idle[:len(cc.idle)-1]
		if time.Since(pc.usedAt) <= cc.idleTimeout {
			cc.reused++
			cc.mu.Unlock()
			return pc, nil
		}
		pc.conn.Close()
	}
	cc.dials++
	cc.mu.Unlock()

	if attempts < 1 {
		attempts = 1
	}
	if backoff <= 0 {
		backoff = 50 * time.Millisecond
	}
	var raw net.Conn
	var err error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			time.Sleep(backoff)
			backoff *= 2 // 指数退避，避免风暴式重连
		}
		if cc.tlsCfg != nil {
			raw, err = tls.DialWithDialer(&net.Dialer{Timeout: dialTO}, "tcp", cc.target, cc.tlsCfg)
		} else {
			raw, err = net.DialTimeout("tcp", cc.target, dialTO)
		}
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return &pooledConn{conn: raw, r: bufio.NewReader(raw), w: bufio.NewWriter(raw), usedAt: time.Now()}, nil
}

// putConn 归还连接：若池满或已关闭则直接关闭，否则放回空闲池。
func (cc *ClientConn) putConn(pc *pooledConn) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.closed || len(cc.idle) >= cc.maxIdle {
		pc.conn.Close()
		return
	}
	pc.usedAt = time.Now()
	cc.idle = append(cc.idle, pc)
}

// Close 关闭客户端并释放所有空闲连接。幂等。
func (cc *ClientConn) Close() error {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.closed {
		return nil
	}
	cc.closed = true
	for _, pc := range cc.idle {
		pc.conn.Close()
	}
	cc.idle = nil
	return nil
}

// Invoke 发起一次 RPC：method 为完整方法名（如 "/Kv/Get"），reqData 为请求体字节，
// 返回响应体字节。ctx 取消或连接失败时返回错误。连接经池化复用。
//
// 取消安全（R2 隐性健壮性修复）：此前取消协程在 ctx.Done() 时直接 pc.conn.Close()，
// 而成功路径会 putConn(pc) 复用连接——若「响应帧恰好到达」与「ctx 取消」竞态（尤其
// 无 deadline 的纯取消 ctx），关连接与放回池会同时发生，把已关闭连接放回池中，导致
// 后续复用失败（连接池中毒）。现用 connGuard 互斥保证「关闭丢弃」与「放回复用」二选一。
func (cc *ClientConn) Invoke(ctx context.Context, method string, reqData []byte) (respData []byte, err error) {
	if err := ctx.Err(); err != nil {
		cc.errs.Add(1)
		return nil, err
	}
	cc.rpcs.Add(1)
	pc, gerr := cc.getConn()
	if gerr != nil {
		cc.errs.Add(1)
		return nil, gerr
	}
	// connGuard 协调「取消关闭」与「成功复用」：同一连接只能二选一，由锁串行化，
	// 避免被既关闭又放回池（连接池中毒）。
	var guard struct {
		sync.Mutex
		decided bool
		closed  bool
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			guard.Lock()
			if !guard.decided {
				guard.decided = true
				guard.closed = true
				_ = pc.conn.Close()
			}
			guard.Unlock()
		case <-done:
		}
	}()
	// ctx 截止时间传播到 TCP 连接：超时即中断在途读写，且不污染复用连接。
	if dl, ok := ctx.Deadline(); ok {
		if err = pc.conn.SetDeadline(dl); err != nil {
			return nil, err
		}
	} else {
		pc.conn.SetDeadline(time.Time{}) // 清除既往 deadline
	}
	r, w := pc.r, pc.w
	cc.bytesSent.Add(int64(len(method) + len(reqData)))
	if err = writeFrame(w, frameData, []byte(method)); err != nil {
		cc.errs.Add(1)
		return nil, err
	}
	if err = writeFrame(w, frameData, reqData); err != nil {
		cc.errs.Add(1)
		return nil, err
	}
	var typ byte
	var resp []byte
	typ, resp, err = readFrame(r)
	if err != nil {
		cc.errs.Add(1)
		// 读失败：连接已不可用，直接关闭丢弃，不再放回池中。
		guard.Lock()
		if !guard.decided {
			guard.decided = true
			guard.closed = true
			_ = pc.conn.Close()
		}
		guard.Unlock()
		return nil, err
	}
	// 读帧成功：先独占标记连接归属，阻止取消协程并发关闭后我们又放回池中。
	guard.Lock()
	if guard.decided {
		// ctx 在响应到达瞬间被取消，取消协程已关闭连接；丢弃，不污染池。
		guard.Unlock()
		cc.errs.Add(1)
		return nil, errors.New("transport: connection cancelled concurrently after response")
	}
	guard.decided = true
	guard.Unlock()

	if typ == frameError {
		cc.bytesRecv.Add(int64(len(resp)))
		cc.errs.Add(1)
		// 服务端显式错误帧：请求/响应完整交换，连接仍健康，放回池复用。
		cc.putConn(pc)
		return nil, errors.New(string(resp))
	}
	cc.bytesRecv.Add(int64(len(resp)))
	cc.putConn(pc)
	return resp, nil
}

// Ping 发送一个应用层保活帧并等待 pong，验证「到 target 的 TCP 链路 + 对端服务循环」
// 都是活的。成功时连接放回池中复用（顺带完成了连接预热）。ctx 截止时间生效。
// 与 Warmup 的区别：Warmup 只建链，Ping 还验证对端会读帧并回应。
func (cc *ClientConn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pc, err := cc.getConn()
	if err != nil {
		return err
	}
	if dl, ok := ctx.Deadline(); ok {
		if err := pc.conn.SetDeadline(dl); err != nil {
			pc.conn.Close()
			return err
		}
	} else {
		pc.conn.SetDeadline(time.Time{})
	}
	if err := writeFrame(pc.w, framePing, nil); err != nil {
		pc.conn.Close()
		return err
	}
	typ, _, err := readFrame(pc.r)
	if err != nil {
		pc.conn.Close()
		return err
	}
	if typ != framePing {
		pc.conn.Close()
		return fmt.Errorf("transport: unexpected pong frame type %d", typ)
	}
	cc.putConn(pc)
	return nil
}

// InvokeMsg 是 Invoke 的类型安全封装：用 codec 编解码 req/reply。
// codec 经锁内快照读取，可与 SetCodec 并发安全共用。
func (cc *ClientConn) InvokeMsg(ctx context.Context, method string, req, reply interface{}) error {
	codec := cc.codecRef()
	reqData, err := codec.Marshal(req)
	if err != nil {
		return err
	}
	respData, err := cc.Invoke(ctx, method, reqData)
	if err != nil {
		return err
	}
	return codec.Unmarshal(respData, reply)
}

// Target 返回客户端目标地址。
func (cc *ClientConn) Target() string { return cc.target }
