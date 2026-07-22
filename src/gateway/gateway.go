// gateway.go —— 基于进程内 ShardKV 集群的 HTTP REST 网关
//
// 把 cluster 包启动的内存集群暴露成 REST 接口（GET/PUT/POST-append /kv/{key}），
// 供上层 kvcli 或任意 HTTP 客户端访问。Handler() 返回 http.Handler，便于用
// httptest 做单测而无需绑定端口。
//
// 说明：网关自带一个进程内集群（labrpc 网络），因此是一个"自包含演示网关"；
// 生产环境应由网关连接一组独立部署、走真实网络传输的 ShardKV 节点（而非本文件
// 里的内存集群）。
package main

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"raftkv/src/cluster"
	"raftkv/src/shardkv"
	"raftkv/src/shardmaster"
)

// maxConcurrent 是网关在途请求的信号量上限（I12）。超过即返回 429，避免资源被压垮。
const maxConcurrent = 64

// Server 持有集群与绑定到它的 ShardKV 客户端。
type Server struct {
	c     *cluster.Cluster
	clerk *shardkv.Clerk

	// I12：有界并发信号量；I13：在途请求计数，供优雅关闭等待。
	sem chan struct{}
	wg  sync.WaitGroup

	// 可选：底层 HTTP Server，供 Shutdown 一并关闭监听。
	mu  sync.Mutex
	srv *http.Server

	// testDelay 仅用于单测：wrap 在取得信号量后休眠该时长，人为拉长在途窗口，
	// 使并发限流（429）路径能被确定性触发（内存集群后端极快时，正常请求难以
	// 让在途数打满 64 槽）。生产环境恒为 0，零行为影响。
	testDelay time.Duration

	// I15：进程内访问日志环形缓冲（最近 N 条 HTTP 请求），供 /debug/accesslog
	// 暴露，便于审计/排障（无需外部日志采集即可回看近期请求方法/路径/状态码/延迟）。
	accessMu  sync.Mutex
	accessLog []accessEntry
	accessCap int

	// I47：分级结构化日志环形缓冲（最近 N 条），供 /debug/log 暴露，统一排障入口
	// （取代散落的 fmt.Println，且可按级别过滤 debug/info/warn/error）。
	logMu  sync.Mutex
	logBuf []logEntry
	logCap int

	// 单请求最大处理时长（I16）。后端 Raft 操作在迁移抖动/leader 切换下可能偶发
	// 长时间不返回；超出该上限即由 http.TimeoutHandler 返回 503，避免 HTTP 连接
	// 无限挂起（显式兜底，与 Clerk 自身的有界重试互补）。默认 30s，零正常影响。
	requestTimeout time.Duration

	// I49：每客户端令牌桶限流（在全局并发 429 之上更细粒度保护）。按客户端标识
	// （X-Client-ID 或 RemoteAddr IP）限流，超限返回 429 + Retry-After。
	limitMu        sync.Mutex
	clientLimiters map[string]*tokenBucket
	clientRate     float64 // 每客户端每秒补充令牌数（<=0 表示关闭限流）
	clientBurst    float64 // 每客户端桶容量（突发上限）

	// I62：按路由限流。对特定路由（如热点写路径）设独立令牌桶，比全局并发/每客户端
	// 更细粒度保护单一路由不被打爆。key 为 "METHOD /path"（如 "POST /kv/{key}/append"）。
	routeLimitMu  sync.Mutex
	routeLimiters map[string]*tokenBucket

	// I50：CORS 配置（可空，空表示允许所有源 "*"）。供浏览器前端直连网关。
	corsOrigins []string

	// I52：当前生效配置快照（由 Apply 写入），供 /debug/config 展示排障。
	curCfg GatewayConfig

	// I54：单请求最大请求体字节数（防御超大 body 打爆内存/后端）。>0 时 wrap 对
	// 已知 Content-Length 直接 413 拒绝，并对 body 套 MaxBytesReader 兜底（含 chunked）。
	// 默认 1MB（与历史 handler 内 1<<20 限额一致），可由配置 max_body_size 覆盖。
	maxBodySize int64

	// I55：响应 gzip 压缩开关。开启且客户端 Accept-Encoding 含 gzip 时，对响应体压缩
	// （省带宽）；同时写 Vary: Accept-Encoding 避免共享缓存按错误编码缓存。默认开启。
	compress bool

	// I56：安全响应头开关。开启时 wrap 注入 X-Content-Type-Options / X-Frame-Options /
	// Referrer-Policy 等基线安全头，缓解 MIME 嗅探 / 点击劫持 / 引用泄露。默认开启。
	secHeaders bool

	// I57：IP 白名单（CIDR 列表）。非空时仅允许列表内客户端 IP 访问，其余返回 403。
	// 为空表示不限制。用于网关仅对内网/特定来源开放的场景。
	ipAllowMu sync.Mutex
	ipAllow   []*net.IPNet

	// I58：已注册路由清单（在 Handler() 中填充），供 /debug/routes 暴露，便于确认路由面。
	routeMu sync.Mutex
	routes  []string

	// I59：启动时间与版本号，供 /debug/version 暴露，便于运行时透明与 uptime 监控。
	startedAt time.Time
	version   string

	// I61：后端健康熔断状态。连续 5xx 达阈值进入打开态，后续请求快速失败（503）保护
	// 后端；冷却窗口后半开探测，成功即恢复。4xx 视为客户端问题不计入后端故障。
	breakerMu        sync.Mutex
	breakerErrs      int
	breakerOpen      bool
	breakerOpenAt    time.Time
	breakerCooldown  time.Duration
	breakerThreshold int
}

// tokenBucket 是令牌桶：按时间补充令牌，桶满截断；取用时需至少 1 枚令牌。
// 由单客户端限流（I49）与按路由限流（I62）共用，内部持锁保证并发安全。
type tokenBucket struct {
	mu     sync.Mutex
	tokens float64
	last   time.Time
	rate   float64
	burst  float64
}

// allow 按当前时刻补充令牌（elapsed*rate，截断到 burst），若 >=1 则消耗 1 枚并放行。
// 持锁保证并发安全（多 goroutine 同时取用同一桶不会 data race）。
func (b *tokenBucket) allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	elapsed := now.Sub(b.last).Seconds()
	b.last = now
	b.tokens += elapsed * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	if b.tokens >= 1 {
		b.tokens -= 1
		return true
	}
	return false
}

// SetRequestTimeout 仅供单测覆盖默认单请求超时（生产不可用）。
func (s *Server) SetRequestTimeout(d time.Duration) { s.requestTimeout = d }

// SetMaxBodySize 配置单请求最大请求体字节数（生产可用）。<=0 表示不限制。
// 经 wrap 的 413 拒绝 + MaxBytesReader 兜底生效。
func (s *Server) SetMaxBodySize(n int64) { s.maxBodySize = n }

// SetCompress 配置是否对响应启用 gzip 压缩（生产可用）。仅当客户端 Accept-Encoding
// 含 gzip 时压缩，并自动加 Vary: Accept-Encoding。
func (s *Server) SetCompress(on bool) { s.compress = on }

// SetSecurityHeaders 配置是否注入基线安全响应头（生产可用）。默认开启。
func (s *Server) SetSecurityHeaders(on bool) { s.secHeaders = on }

// SetVersion 配置网关版本号（生产可用），供 /debug/version 暴露。
func (s *Server) SetVersion(v string) { s.version = v }

// I61：后端健康熔断。连续 5xx 错误达到阈值后进入「打开」态，后续请求快速失败（503），
// 避免持续打满已不健康的后端；冷却窗口后半开探测，成功即恢复。4xx 视为客户端问题，
// 不计入后端故障，会重置计数（含熔断恢复）。
func (s *Server) breakerOpenNow() bool {
	s.breakerMu.Lock()
	defer s.breakerMu.Unlock()
	if !s.breakerOpen {
		return false
	}
	if time.Since(s.breakerOpenAt) >= s.breakerCooldown {
		return false // 冷却已过，半开探测放行
	}
	return true
}

func (s *Server) observeBackend(status int) {
	s.breakerMu.Lock()
	defer s.breakerMu.Unlock()
	if status >= 500 {
		s.breakerErrs++
		if !s.breakerOpen && s.breakerErrs >= s.breakerThreshold {
			s.breakerOpen = true
			s.breakerOpenAt = time.Now()
			s.logf(levelWarn, "circuit breaker opened", map[string]string{"consecutive_errors": strconv.Itoa(s.breakerErrs)})
		}
		return
	}
	// 成功或客户端错误：重置连续错误计数；若处于打开态则关闭（半开探测成功）。
	if s.breakerErrs > 0 {
		s.breakerErrs = 0
	}
	if s.breakerOpen {
		s.breakerOpen = false
		s.logf(levelInfo, "circuit breaker closed (recovered)", nil)
	}
}

// SetBreaker 配置熔断阈值与冷却窗口（生产可用）。threshold 为连续 5xx 触发数，
// cooldown 为打开态持续时间（半开探测前的等待）。
func (s *Server) SetBreaker(threshold int, cooldown time.Duration) {
	s.breakerMu.Lock()
	defer s.breakerMu.Unlock()
	if threshold > 0 {
		s.breakerThreshold = threshold
	}
	if cooldown > 0 {
		s.breakerCooldown = cooldown
	}
}

// SetIPAllow 配置 IP 白名单（CIDR 字符串列表）。解析失败的条目被跳过并记录警告；
// 空列表表示关闭白名单（允许所有来源）。并发安全。
func (s *Server) SetIPAllow(cidrs []string) {
	var nets []*net.IPNet
	for _, c := range cidrs {
		_, ipnet, err := net.ParseCIDR(c)
		if err != nil {
			s.logf(levelWarn, "invalid allow_cidr, skipped", map[string]string{"cidr": c})
			continue
		}
		nets = append(nets, ipnet)
	}
	s.ipAllowMu.Lock()
	s.ipAllow = nets
	s.ipAllowMu.Unlock()
}

// clientAllowed 判断请求来源 IP 是否在白名单内。白名单为空时恒返回 true（不限制）。
// 来源 IP 优先取 X-Forwarded-For 首段（经反代时），否则取 RemoteAddr 的 IP 部分。
func (s *Server) clientAllowed(r *http.Request) bool {
	s.ipAllowMu.Lock()
	nets := s.ipAllow
	s.ipAllowMu.Unlock()
	if len(nets) == 0 {
		return true
	}
	ipStr := r.RemoteAddr
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		if idx := strings.IndexAny(fwd, ", "); idx >= 0 {
			ipStr = fwd[:idx]
		} else {
			ipStr = fwd
		}
	}
	ip := net.ParseIP(ipStr)
	if idx := strings.LastIndex(ipStr, ":"); idx >= 0 {
		ip = net.ParseIP(ipStr[:idx])
	}
	if ip == nil {
		return false
	}
	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// SetClientRateLimit 配置每客户端令牌桶限流（生产可用）：rps 为每客户端每秒补充
// 令牌数，burst 为桶容量（允许的最大突发请求数）。rps<=0 表示关闭限流。
func (s *Server) SetClientRateLimit(rps float64, burst int) {
	s.limitMu.Lock()
	s.clientRate = rps
	s.clientBurst = float64(burst)
	if s.clientLimiters == nil {
		s.clientLimiters = make(map[string]*tokenBucket)
	}
	s.limitMu.Unlock()
}

// SetRouteLimit 配置某路由（key="METHOD /path"，如 "POST /kv/{key}/append"）的令牌桶
// 限流（rps 每秒补充、burst 桶容量）。用于比 per-client 更细粒度的单路由保护。
func (s *Server) SetRouteLimit(route string, rps float64, burst int) {
	s.routeLimitMu.Lock()
	defer s.routeLimitMu.Unlock()
	if s.routeLimiters == nil {
		s.routeLimiters = make(map[string]*tokenBucket)
	}
	s.routeLimiters[route] = &tokenBucket{tokens: float64(burst), last: time.Now(), rate: rps, burst: float64(burst)}
}

// routeLimiterFor 返回该请求匹配路由的令牌桶（若存在）。key 为 "METHOD /path"。
func (s *Server) routeLimiterFor(r *http.Request) *tokenBucket {
	key := r.Method + " " + r.URL.Path
	s.routeLimitMu.Lock()
	defer s.routeLimitMu.Unlock()
	return s.routeLimiters[key]
}

// SetCORS 配置允许跨域的源列表（生产可用）。空切片表示允许任意源（"*"）。
func (s *Server) SetCORS(origins []string) { s.corsOrigins = origins }

// corsHandler 是 CORS 中间件：注入 Access-Control-* 响应头，并处理 OPTIONS 预检
// （直接返回 204，不进入路由）。corsOrigins 为空时允许所有源；非空时仅回显匹配的
// 源（不匹配则不注入头，等效拒绝跨域）。使网关可被浏览器前端直连。
func (s *Server) corsHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		allow := "*"
		if len(s.corsOrigins) > 0 {
			allow = "" // 默认拒绝；仅当匹配才回显
			for _, o := range s.corsOrigins {
				if o == "*" {
					allow = "*"
					break
				}
				if o == origin {
					allow = origin
					break
				}
			}
		}
		if allow != "" {
			w.Header().Set("Access-Control-Allow-Origin", allow)
			w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Request-ID, X-Client-ID")
			w.Header().Set("Access-Control-Max-Age", "86400")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// clientKey 从请求推导客户端标识：优先 X-Client-ID 头，否则取 RemoteAddr 的 IP 部分
// （去掉 :port，因每次连接的端口会变）。同一客户端的请求共享一个令牌桶。
func (s *Server) clientKey(r *http.Request) string {
	if id := r.Header.Get("X-Client-ID"); id != "" {
		return id
	}
	if idx := strings.LastIndex(r.RemoteAddr, ":"); idx >= 0 {
		return r.RemoteAddr[:idx]
	}
	return r.RemoteAddr
}

// allowClient 返回该客户端本次请求是否被限流放行。clientRate<=0 时直接放行（限流关闭）。
// 令牌桶按需惰性创建；map 过大时清空重置以防内存无限增长（demo 级保护）。
func (s *Server) allowClient(r *http.Request) bool {
	if s.clientRate <= 0 {
		return true
	}
	key := s.clientKey(r)
	s.limitMu.Lock()
	b, ok := s.clientLimiters[key]
	if !ok {
		if len(s.clientLimiters) > 4096 {
			s.clientLimiters = make(map[string]*tokenBucket)
		}
		b = &tokenBucket{tokens: s.clientBurst, last: time.Now(), rate: s.clientRate, burst: s.clientBurst}
		s.clientLimiters[key] = b
	}
	allowed := b.allow(time.Now())
	s.limitMu.Unlock()
	return allowed
}

// accessEntry 是 /debug/accesslog 中单条请求记录。
type accessEntry struct {
	TS        time.Time `json:"ts"`
	Method    string    `json:"method"`
	Path      string    `json:"path"`
	Status    int       `json:"status"`
	LatencyMs float64   `json:"latency_ms"`
	RequestID string    `json:"request_id,omitempty"`
}

// genRequestID 生成一个随机请求 ID（16 位 hex）。用于 X-Request-ID 透传，便于跨服务
// 链路追踪；crypto/rand 保证不可预测且全局唯一（网关非超高频场景，开销可忽略）。
func genRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// statusRecorder 包装 http.ResponseWriter，捕获最终状态码（WriteHeader 可能未被
// 显式调用——此时按 200 计），供访问日志记录。其它方法经嵌入的 ResponseWriter 透传。
type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wrote {
		r.status = code
		r.wrote = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wrote {
		r.status = http.StatusOK
		r.wrote = true
	}
	return r.ResponseWriter.Write(b)
}

// gzipResponseWriter 把 *gzip.Writer 适配成 http.ResponseWriter：Header/WriteHeader
// 透传底层 ResponseWriter，Write 转发到 gzip 压缩流。仅用于 wrap 的 gzip 分支。
type gzipResponseWriter struct {
	http.ResponseWriter
	gz *gzip.Writer
}

func (g *gzipResponseWriter) Write(p []byte) (int, error) { return g.gz.Write(p) }

// SetTestDelay 仅供单测注入人为延迟，使 429 路径可被稳定复现。生产不可用。
func (s *Server) SetTestDelay(d time.Duration) { s.testDelay = d }

// NewServer 用给定集群构造网关（不立即加入 group，需先 Init）。
func NewServer(c *cluster.Cluster) *Server {
	return &Server{c: c, clerk: c.Clerk(), sem: make(chan struct{}, maxConcurrent), accessCap: 256, logCap: 256, requestTimeout: 30 * time.Second, clientLimiters: make(map[string]*tokenBucket), clientRate: 200, clientBurst: 40, maxBodySize: 1 << 20, compress: true, secHeaders: true, startedAt: time.Now(), version: "dev", breakerCooldown: 10 * time.Second, breakerThreshold: 5}
}

// logLevel 是结构化日志级别，数值越大越严重。
type logLevel int

const (
	levelDebug logLevel = iota
	levelInfo
	levelWarn
	levelError
)

func (l logLevel) String() string {
	switch l {
	case levelDebug:
		return "debug"
	case levelInfo:
		return "info"
	case levelWarn:
		return "warn"
	case levelError:
		return "error"
	default:
		return "unknown"
	}
}

// levelOf 把级别字符串映射回数值（未知级别按 info 处理）。
func levelOf(s string) logLevel {
	switch s {
	case "debug":
		return levelDebug
	case "info":
		return levelInfo
	case "warn", "warning":
		return levelWarn
	case "error":
		return levelError
	default:
		return levelInfo
	}
}

// logEntry 是 /debug/log 中单条结构化日志。
type logEntry struct {
	TS     time.Time         `json:"ts"`
	Level  string            `json:"level"`
	Msg    string            `json:"msg"`
	Fields map[string]string `json:"fields,omitempty"`
}

// logf 追加一条分级结构化日志到环形缓冲（超出容量丢弃最旧）。
func (s *Server) logf(level logLevel, msg string, fields map[string]string) {
	s.logMu.Lock()
	e := logEntry{TS: time.Now(), Level: level.String(), Msg: msg, Fields: fields}
	s.logBuf = append(s.logBuf, e)
	if len(s.logBuf) > s.logCap {
		s.logBuf = s.logBuf[len(s.logBuf)-s.logCap:]
	}
	s.logMu.Unlock()
}

// Init 依次加入 nGroups 个 replica group，使分片可写。
func (s *Server) Init(nGroups int) {
	for g := 0; g < nGroups; g++ {
		s.c.Join(g)
		s.c.WaitConfig(g, 0, g+1)
	}
}

// Handler 返回 HTTP 路由（Go 1.22 的 method+path 模式）。register 同时登记路由到
// mux 与 routes 清单（避免两者漂移），供 /debug/routes 暴露完整路由面。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	var routes []string
	register := func(pattern string, h func(http.ResponseWriter, *http.Request)) {
		mux.HandleFunc(pattern, s.wrap(h))
		routes = append(routes, pattern)
	}
	register("GET /kv/{key}", s.handleGet)
	register("PUT /kv/{key}", s.handlePut)
	register("POST /kv/{key}/append", s.handleAppend)
	register("GET /healthz", s.handleHealthz)
	// 就绪探针（I18）：集群可正常服务读写时 200，否则 503，供 k8s readinessProbe 直用。
	register("GET /readyz", s.handleReadyz)
	// 可观测性：把进程内 ShardKV 的 Metrics 注册表 JSON 序列化暴露出来。
	// 指标由 cycle 11 的 metrics 包在热路径上以纯原子操作累计，零行为影响。
	register("GET /metrics", s.handleMetrics)
	// 诊断：暴露每个 group/副本的「分片归属 + 待接收/待迁出」状态，便于定位
	// 3-group 再平衡卡死等迁移问题（pendingIn/pendingOut 残留会冻结配置推进）。
	register("GET /debug/shards", s.handleDebugShards)
	// 集群健康总览（程序化消费）：每 group 是否有 leader、当前 config 号、拥有分片数、
	// 待接收/待迁出/孤儿 incoming 分片，以及是否存在卡滞（StallSeconds>0 即冻结风险）。
	register("GET /status", s.handleStatus)
	// 迁移进度（人类可读，供 CLI `start.sh migrate` 直接展示）：每个 group leader 副本的
	// 实时迁移状态 + 集群最新 config 号，一眼看清再平衡是否卡住。
	register("GET /debug/migrate", s.handleDebugMigrate)
	// 配置历史（人类/程序可读）：展示 shardmaster 从初始到最新的每段配置，便于复盘
	// rebalance 轨迹、确认分片在哪些 group 间迁移（排查 3-group 冻结时尤其有用）。
	register("GET /debug/configs", s.handleDebugConfigs)
	// 当前 group 成员与分片归属（I14）：每个 gid 的 server 列表及其拥有的分片编号。
	register("GET /debug/groups", s.handleDebugGroups)
	// 进程内访问日志（I15）：最近 N 条 HTTP 请求的方法/路径/状态码/延迟，便于审计排障。
	register("GET /debug/accesslog", s.handleDebugAccessLog)
	// 分级结构化日志（I47）：最近 N 条带级别（debug/info/warn/error）的日志，
	// 可按 ?level= 过滤最低级别、?limit= 控制条数，统一排障入口。
	register("GET /debug/log", s.handleDebugLog)
	// 当前生效配置（I52）：返回网关实际生效的配置快照（脱敏），便于确认配置加载结果。
	register("GET /debug/config", s.handleDebugConfig)
	// I58：已注册路由清单，便于确认网关路由面（含本端点自身）。
	register("GET /debug/routes", s.handleDebugRoutes)
	// I59：版本与 uptime，便于运行时透明与监控（不含任何敏感配置）。
	register("GET /debug/version", s.handleDebugVersion)
	s.routeMu.Lock()
	s.routes = routes
	s.routeMu.Unlock()
	// I16：以 http.TimeoutHandler 兜底单请求最大时长，避免后端 Raft 操作在迁移抖动
	// 下长时间不返回时 HTTP 连接无限挂起。超时返回 503；内层 handler 仍在后台跑完
	// （其写入被丢弃），不会破坏状态机，与 Clerk 有界重试互补。requestTimeout 默认 30s。
	// I50：外层套 CORS 中间件，使浏览器前端可直连网关（含 OPTIONS 预检处理）。
	return s.corsHandler(http.TimeoutHandler(mux, s.requestTimeout, "request timed out"))
}

// wrap 给每个 handler 套上并发限制（I12）与在途请求计数（I13 优雅关闭用）。
// 并发超过上限时立即返回 429，不无限排队。
func (s *Server) wrap(h func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// X-Request-ID 透传：入站已带则沿用，否则生成。回写响应头，便于跨服务链路追踪。
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = genRequestID()
		}
		w.Header().Set("X-Request-ID", reqID)
		// I56：基线安全响应头（默认开启）。缓解 MIME 嗅探 / 点击劫持 / 引用泄露。
		// 在 wrap 统一注入，所有路由自动受益，无需逐个 handler 重复。
		if s.secHeaders {
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Header().Set("X-Frame-Options", "DENY")
			w.Header().Set("Referrer-Policy", "no-referrer")
		}
		// I57：IP 白名单。白名单非空且来源 IP 不在列表内 -> 403 拒绝（在并发/限流之前）。
		if !s.clientAllowed(r) {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusForbidden)
			io.WriteString(w, `{"error":"source IP not allowed","code":403}`)
			s.logf(levelWarn, "ip not allowed", map[string]string{"method": r.Method, "path": r.URL.Path, "request_id": reqID})
			return
		}
		// I61：后端健康熔断（打开态）快速失败，保护已不健康的后端（早于请求体/限流）。
		if s.breakerOpenNow() {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, `{"error":"circuit breaker open","code":503}`)
			s.logf(levelWarn, "circuit breaker reject (open)", map[string]string{"method": r.Method, "path": r.URL.Path, "request_id": reqID})
			return
		}
		// I54：请求体大小上限。已知 Content-Length 超阈值直接 413 拒绝；并对 body 套
		// MaxBytesReader 兜底（覆盖 chunked / 流式超发），避免超大 body 打爆内存或后端。
		if s.maxBodySize > 0 {
			if r.ContentLength > s.maxBodySize {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				io.WriteString(w, `{"error":"request body too large","code":413}`)
				s.logf(levelWarn, "request body too large", map[string]string{"method": r.Method, "path": r.URL.Path, "request_id": reqID})
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, s.maxBodySize)
		}
		// 每客户端令牌桶限流（I49）：超限直接 429 + Retry-After，不进入并发信号量。
		if !s.allowClient(r) {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			io.WriteString(w, `{"error":"client rate limit exceeded","code":429}`)
			s.logf(levelWarn, "client rate limit exceeded", map[string]string{"method": r.Method, "path": r.URL.Path, "request_id": reqID})
			return
		}
		// I62：按路由限流。命中注册的路由令牌桶且超限则 429（早于并发信号量）。
		if b := s.routeLimiterFor(r); b != nil && !b.allow(time.Now()) {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			io.WriteString(w, `{"error":"route rate limit exceeded","code":429}`)
			s.logf(levelWarn, "route rate limit exceeded", map[string]string{"method": r.Method, "path": r.URL.Path, "request_id": reqID})
			return
		}
		record := func(status int, d time.Duration) {
			s.recordAccess(r.Method, r.URL.Path, status, d, reqID)
		}
		select {
		case s.sem <- struct{}{}:
			defer func() { <-s.sem }()
		default:
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusTooManyRequests)
			io.WriteString(w, `{"error":"too many concurrent requests","code":429}`)
			s.logf(levelWarn, "concurrency limit exceeded", map[string]string{"method": r.Method, "path": r.URL.Path, "request_id": reqID})
			record(http.StatusTooManyRequests, time.Since(start))
			return
		}
		s.wg.Add(1)
		defer s.wg.Done()
		if s.testDelay > 0 {
			time.Sleep(s.testDelay)
		}
		// I55：响应 gzip 压缩。仅当开启且客户端 Accept-Encoding 含 gzip 时压缩，
		// 并写 Vary: Accept-Encoding 避免共享缓存错乱。错误响应走的早期 return 已先
		// 于此处返回，不会被压缩。
		if s.compress && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Add("Vary", "Accept-Encoding")
			gz := gzip.NewWriter(w)
			defer gz.Close()
			w = &gzipResponseWriter{ResponseWriter: w, gz: gz}
		}
		rec := &statusRecorder{ResponseWriter: w}
		h(rec, r)
		st := rec.status
		if st == 0 {
			st = http.StatusOK
		}
		// 结构化记录每请求一条日志：成功=info，4xx=warn，5xx=error，便于 /debug/log 排障。
		fields := map[string]string{"method": r.Method, "path": r.URL.Path, "status": strconv.Itoa(st), "request_id": reqID}
		switch {
		case st >= 500:
			s.logf(levelError, "request error", fields)
		case st >= 400:
			s.logf(levelWarn, "request client error", fields)
		default:
			s.logf(levelInfo, "request", fields)
		}
		record(st, time.Since(start))
		// I61：后端健康熔断观测。按最终状态码更新熔断状态（5xx 累计，非 5xx 重置/恢复）。
		s.observeBackend(st)
	}
}

// recordAccess 把一条请求记录追加到访问日志环形缓冲（超出容量则丢弃最旧）。
func (s *Server) recordAccess(method, path string, status int, d time.Duration, reqID string) {
	s.accessMu.Lock()
	e := accessEntry{
		TS:        time.Now(),
		Method:    method,
		Path:      path,
		Status:    status,
		LatencyMs: float64(d.Microseconds()) / 1000.0,
		RequestID: reqID,
	}
	s.accessLog = append(s.accessLog, e)
	if len(s.accessLog) > s.accessCap {
		s.accessLog = s.accessLog[len(s.accessLog)-s.accessCap:]
	}
	s.accessMu.Unlock()
}

// handleDebugAccessLog 返回进程内访问日志的最近 N 条（默认 50，可用 ?limit= 覆盖），
// 按时间升序（最旧在前、最新在后），便于回看近期请求轨迹。
func (s *Server) handleDebugAccessLog(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}
	s.accessMu.Lock()
	n := len(s.accessLog)
	if limit > n {
		limit = n
	}
	out := make([]accessEntry, limit)
	copy(out, s.accessLog[n-limit:])
	s.accessMu.Unlock()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleHealthz 是存活探针（200 即健康）。
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// handleDebugLog 返回进程内分级结构化日志的最近 N 条（默认 50，?limit= 覆盖），
// 可按 ?level= 过滤最低级别（debug/info/warn/error，默认 info）。按时间升序（最旧
// 在前、最新在后），便于回看近期事件轨迹与级别分布。
func (s *Server) handleDebugLog(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}
	minLevel := levelInfo
	if lv := r.URL.Query().Get("level"); lv != "" {
		if parsed, ok := parseLogLevel(lv); ok {
			minLevel = parsed
		}
	}
	s.logMu.Lock()
	var filtered []logEntry
	for _, e := range s.logBuf {
		if levelOf(e.Level) >= minLevel {
			filtered = append(filtered, e)
		}
	}
	n := len(filtered)
	if limit > n {
		limit = n
	}
	page := make([]logEntry, limit)
	copy(page, filtered[n-limit:])
	s.logMu.Unlock()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(page); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// parseLogLevel 把 ?level= 查询参数解析为 logLevel，未知值返回 ok=false（保持默认 info）。
func parseLogLevel(s string) (logLevel, bool) {
	switch s {
	case "debug":
		return levelDebug, true
	case "info":
		return levelInfo, true
	case "warn", "warning":
		return levelWarn, true
	case "error":
		return levelError, true
	default:
		return levelInfo, false
	}
}

// ConfigSnapshot 是 /debug/config 的响应体：当前生效的网关配置（脱敏，无敏感字段）。
type ConfigSnapshot struct {
	ListenAddr      string   `json:"listen_addr"`
	RequestTimeout  int      `json:"request_timeout_sec"`
	MaxConcurrent   int      `json:"max_concurrent"`
	ClientRate      float64  `json:"client_rate"`
	ClientBurst     int      `json:"client_burst"`
	CORSOrigins     []string `json:"cors_origins"`
	MaxBodySize     int64    `json:"max_body_size"`
	Compress        bool     `json:"compress"`
	SecurityHeaders bool     `json:"security_headers"`
	AllowCIDRs      []string `json:"allow_cidrs"`
}

// handleDebugConfig 返回网关当前生效配置快照（I52），便于排障确认配置加载结果。
// 数据来自 Apply 写入的 curCfg，与运行时实际生效值一致。
func (s *Server) handleDebugConfig(w http.ResponseWriter, r *http.Request) {
	c := s.curCfg
	snap := ConfigSnapshot{
		ListenAddr:      c.ListenAddr,
		RequestTimeout:  c.RequestTimeout,
		MaxConcurrent:   c.MaxConcurrent,
		ClientRate:      c.ClientRate,
		ClientBurst:     c.ClientBurst,
		CORSOrigins:     c.CORSOrigins,
		MaxBodySize:     int64(c.MaxBodySize),
		Compress:        c.Compress,
		SecurityHeaders: c.SecurityHeaders,
		AllowCIDRs:      c.AllowCIDRs,
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(snap); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleDebugVersion 返回网关版本与 uptime（I59），便于运行时透明与监控。
// 不含任何敏感配置字段。
func (s *Server) handleDebugVersion(w http.ResponseWriter, r *http.Request) {
	uptime := time.Since(s.startedAt).Seconds()
	out := map[string]interface{}{
		"version":    s.version,
		"go_version": runtime.Version(),
		"started_at": s.startedAt.UTC().Format(time.RFC3339),
		"uptime_sec": uptime,
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleDebugRoutes 返回网关已注册的路由清单（I58），便于确认路由面、核对是否缺漏
// 或重复。数据来自 Handler() 填充的 s.routes（与 mux 实际注册一致，无漂移）。
func (s *Server) handleDebugRoutes(w http.ResponseWriter, r *http.Request) {
	s.routeMu.Lock()
	routes := s.routes
	s.routeMu.Unlock()
	out := map[string]interface{}{"count": len(routes), "routes": routes}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
func (s *Server) SetHTTPServer(srv *http.Server) {
	s.mu.Lock()
	s.srv = srv
	s.mu.Unlock()
}

// Shutdown 先等待在途请求（WaitGroup）结束，再关闭 HTTP 监听，实现优雅关闭（I13）。
func (s *Server) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	srv := s.srv
	s.mu.Unlock()
	if srv != nil {
		return srv.Shutdown(ctx)
	}
	return nil
}

// GroupStatus 是 /status 中单个 replica group 的健康快照（取 leader 副本视角）。
type GroupStatus struct {
	Group         int     `json:"group"`
	HasLeader     bool    `json:"has_leader"`
	LeaderReplica int     `json:"leader_replica"`
	ConfigNum     int     `json:"config_num"`
	OwnedCount    int     `json:"owned_count"`
	PendingIn     []int   `json:"pending_in"`
	PendingOut    []int   `json:"pending_out"`
	Incoming      []int   `json:"incoming"`
	StallSeconds  float64 `json:"stall_seconds"`
}

// ClusterStatus 是 /status 的聚合响应。
type ClusterStatus struct {
	Groups       []GroupStatus `json:"groups"`
	Healthy      bool          `json:"healthy"`        // 所有 group 均无卡滞 = true
	MaxConfigNum int           `json:"max_config_num"` // 集群最新已应用 config 号
}

// stallUnhealthySec 是判定「再平衡冻结」的卡滞阈值（秒）。正常多跳迁移的分片在
// 数百毫秒内完成，pendingIn/pendingOut 的 StallSeconds 仅瞬时正值；只有真正冻结
// （配置推进卡死、分片永久不可达）才会持续 > 阈值。取 2s（低于 pollConfig 看门狗的
// 3s），使 /status 的 healthy 标志能先于看门狗触发即暴露冻结，又不误报正常瞬时迁移。
const stallUnhealthySec = 2.0

// clusterHealthy 计算集群是否整体健康：每个 group 都有「持租约的 leader」（仅
// isLeader 不够——分区失联的旧 leader 仍自认 leader 却无法提交，必须 HasLeaderLease
// 为真才表示能正常服务读写），且无任何分片的迁移卡滞（StallSeconds 超阈值视为冻结
// 风险）。是 /status 与 /readyz 共用的健康判据（I18 就绪探针）。
func (s *Server) clusterHealthy() bool {
	for g := range s.c.KVs {
		ready := false
		for r := range s.c.KVs[g] {
			d := s.c.KVs[g][r].ShardDebug()
			if d.Leader && d.Lease {
				ready = true
				if d.StallSeconds > stallUnhealthySec {
					return false
				}
				break
			}
		}
		if !ready {
			return false
		}
	}
	return true
}

// handleStatus 返回集群健康总览（JSON），便于监控/告警系统轮询判断再平衡是否冻结。
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st := ClusterStatus{Groups: []GroupStatus{}}
	healthy := true
	maxCfg := 0
	for g := range s.c.KVs {
		gs := GroupStatus{Group: g}
		for r := range s.c.KVs[g] {
			d := s.c.KVs[g][r].ShardDebug()
			if d.Leader {
				gs.HasLeader = true
				gs.LeaderReplica = r
				gs.ConfigNum = d.ConfigNum
				gs.OwnedCount = len(d.Owned)
				gs.PendingIn = d.PendingIn
				gs.PendingOut = d.PendingOut
				gs.Incoming = d.Incoming
				gs.StallSeconds = d.StallSeconds
				if d.StallSeconds > stallUnhealthySec {
					healthy = false
				}
				break
			}
		}
		if gs.ConfigNum > maxCfg {
			maxCfg = gs.ConfigNum
		}
		st.Groups = append(st.Groups, gs)
	}
	st.Healthy = healthy
	st.MaxConfigNum = maxCfg
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(st); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleReadyz 是「就绪探针」：集群所有 group 均有 leader 且无迁移卡滞时返回 200，
// 否则返回 503。与 /healthz（存活，恒 200）区分——/readyz 表示集群是否已能正常
// 服务读写，可直接作为 k8s readinessProbe 使用，无需解析 JSON 体。
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.clusterHealthy() {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
}

// handleDebugMigrate 返回每个 group leader 副本的实时迁移进度（纯文本），供 CLI 直接展示。
func (s *Server) handleDebugMigrate(w http.ResponseWriter, r *http.Request) {
	var b strings.Builder
	maxCfg := 0
	for g := range s.c.KVs {
		for r := range s.c.KVs[g] {
			d := s.c.KVs[g][r].ShardDebug()
			if d.ConfigNum > maxCfg {
				maxCfg = d.ConfigNum
			}
			if !d.Leader {
				continue
			}
			fmt.Fprintf(&b, "group %d (leader r%d, config=%d): owned=%d pendingIn=%v pendingOut=%v incoming=%v stall=%.1fs\n",
				g, r, d.ConfigNum, len(d.Owned), d.PendingIn, d.PendingOut, d.Incoming, d.StallSeconds)
			break
		}
	}
	fmt.Fprintf(&b, "latest config=%d\n", maxCfg)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, b.String())
}

// ConfigView 是 /debug/configs 中单段配置的视图。
type ConfigView struct {
	Num    int                      `json:"num"`
	Groups map[int][]string         `json:"groups"`
	Shards [shardmaster.NShards]int `json:"shards"`
}

// handleDebugConfigs 返回 shardmaster 的完整配置历史（JSON），便于复盘 rebalance 轨迹。
func (s *Server) handleDebugConfigs(w http.ResponseWriter, r *http.Request) {
	cfgs := s.c.Configs()
	if len(cfgs) == 0 {
		http.Error(w, "no configs", http.StatusInternalServerError)
		return
	}
	views := make([]ConfigView, 0, len(cfgs))
	for _, cfg := range cfgs {
		views = append(views, ConfigView{Num: cfg.Num, Groups: cfg.Groups, Shards: cfg.Shards})
	}
	out := map[string]interface{}{
		"latest_num": cfgs[len(cfgs)-1].Num,
		"configs":    views,
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleMetrics 返回 shardkv.Metrics 的快照。按客户端 Accept 协商输出格式：
//   - Accept 含 text/plain 或 prometheus → Prometheus 文本 exposition 格式
//     （便于被 Prometheus / scrape 客户端采集）；
//   - 其它（含 application/json 或缺省）→ JSON 快照（counters + histograms 分位数）。
//
// 两种格式数据源相同，差异仅在序列化方式，零行为影响。
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	accept := r.Header.Get("Accept")
	if strings.Contains(accept, "text/plain") || strings.Contains(accept, "prometheus") {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		if err := shardkv.Metrics.WritePrometheus(w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	snap := shardkv.Metrics.Snapshot()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(snap); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// GroupView 是 /debug/groups 中单个 replica group 的视图：成员 server 列表 + 拥有的分片编号。
type GroupView struct {
	GID     int      `json:"gid"`
	Servers []string `json:"servers"`
	Shards  []int    `json:"shards"`
}

// handleDebugGroups 返回当前 shardmaster 的最新 group 成员与分片归属（JSON）：
// 每个 gid 列出其 server 列表，以及 [0..NShards) 中归它所有的分片编号。
func (s *Server) handleDebugGroups(w http.ResponseWriter, r *http.Request) {
	cfgs := s.c.Configs()
	if len(cfgs) == 0 {
		http.Error(w, "no configs", http.StatusInternalServerError)
		return
	}
	latest := cfgs[len(cfgs)-1]
	groups := make([]GroupView, 0, len(latest.Groups))
	for gid, servers := range latest.Groups {
		var owned []int
		for i := 0; i < shardmaster.NShards; i++ {
			if latest.Shards[i] == gid {
				owned = append(owned, i)
			}
		}
		groups = append(groups, GroupView{GID: gid, Servers: servers, Shards: owned})
	}
	out := map[string]interface{}{
		"num":    latest.Num,
		"groups": groups,
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ShardDebugView 把集群中某个 group/副本的 ShardDebug 与其坐标打包，便于 JSON 输出。
type ShardDebugView struct {
	Group   int
	Replica int
	shardkv.ShardDebug
}

// handleDebugShards 返回集群所有 group 所有副本的分片归属与迁移状态（JSON 数组），
// 用于诊断 3-group 再平衡卡死等迁移问题（pendingIn/pendingOut 残留会冻结配置推进）。
func (s *Server) handleDebugShards(w http.ResponseWriter, r *http.Request) {
	var out []ShardDebugView
	for g := range s.c.KVs {
		for r := range s.c.KVs[g] {
			out = append(out, ShardDebugView{
				Group:      g,
				Replica:    r,
				ShardDebug: s.c.KVs[g][r].ShardDebug(),
			})
		}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// statusForErr 把 ShardKV 错误映射成恰当的 HTTP 状态码，使 REST 语义正确：
// 分片不归本组（含迁移中）-> 409 Conflict；非 leader（瞬态）-> 503；
// 超时/不可达 -> 504；其它 -> 500。
func statusForErr(e shardkv.Err) int {
	switch e {
	case shardkv.ErrWrongGroup:
		return http.StatusConflict
	case shardkv.ErrWrongLeader:
		return http.StatusServiceUnavailable
	case shardkv.ErrTimeout:
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	v, err := s.clerk.GetE(key)
	if err != shardkv.OK {
		http.Error(w, string(err), statusForErr(err))
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, v)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	val, _ := io.ReadAll(io.LimitReader(r.Body, s.maxBodySize))
	if err := s.clerk.PutE(key, string(val)); err != shardkv.OK {
		http.Error(w, string(err), statusForErr(err))
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleAppend(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	val, _ := io.ReadAll(io.LimitReader(r.Body, s.maxBodySize))
	if err := s.clerk.AppendE(key, string(val)); err != shardkv.OK {
		http.Error(w, string(err), statusForErr(err))
		return
	}
	w.WriteHeader(http.StatusOK)
}
