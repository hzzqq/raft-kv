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
)

const (
	frameData  byte = 0
	frameError byte = 1

	defaultMaxFrame = 16 << 20 // 16 MiB 单帧上限，防御超大帧打爆内存
)

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
		resp, herr := h(connCtx, reqData)
		s.rpcs.Add(1)
		s.bytesRecv.Add(int64(len(reqData)))
		if herr != nil {
			s.errs.Add(1)
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

// ServerMetrics 是 Server 的观测快照。
type ServerMetrics struct {
	ConnsActive int64 // 当前活动连接数
	RPCs        int64 // 累计 RPC 次数
	BytesSent   int64 // 累计响应字节
	BytesRecv   int64 // 累计请求字节
	Errs        int64 // 累计 handler 错误数
}

// Metrics 返回服务端累计观测指标（仅供观测/测试）。
func (s *Server) Metrics() ServerMetrics {
	return ServerMetrics{
		ConnsActive: s.connsActive.Load(),
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

	var raw net.Conn
	var err error
	if cc.tlsCfg != nil {
		raw, err = tls.DialWithDialer(&net.Dialer{Timeout: dialTO}, "tcp", cc.target, cc.tlsCfg)
	} else {
		raw, err = net.DialTimeout("tcp", cc.target, dialTO)
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
	defer func() {
		if err != nil {
			cc.errs.Add(1)
			pc.conn.Close() // 出错的连接不再复用，避免半写状态污染池
		} else {
			cc.putConn(pc)
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
	// ctx 被主动取消（无 deadline）时关闭连接在途读写，尽快让 readFrame 返回。
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = pc.conn.Close()
		case <-done:
		}
	}()
	r, w := pc.r, pc.w
	cc.bytesSent.Add(int64(len(method) + len(reqData)))
	if err = writeFrame(w, frameData, []byte(method)); err != nil {
		return nil, err
	}
	if err = writeFrame(w, frameData, reqData); err != nil {
		return nil, err
	}
	var typ byte
	var resp []byte
	typ, resp, err = readFrame(r)
	if err != nil {
		return nil, err
	}
	if typ == frameError {
		return nil, errors.New(string(resp))
	}
	cc.bytesRecv.Add(int64(len(resp)))
	return resp, nil
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
