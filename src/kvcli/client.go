// client.go —— kvcli 的 HTTP 客户端核心
//
// Client 封装对网关（gateway）的 REST 调用：GET /kv/{key}、PUT /kv/{key}、
// POST /kv/{key}/append。核心方法可单测（对 httptest 起的网关发请求）。
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"raftkv/src/util"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Client 是网关的 HTTP 客户端。
type Client struct {
	base string
	http *http.Client

	// cache 是可选读穿缓存：启用后 Get 命中且未过期直接返回，跳过回源；
	// Put/Append 成功会使对应 key 失效。默认 nil = 关闭（零行为影响）。
	cacheMu  sync.Mutex
	cache    map[string]cacheEntry
	cacheTTL time.Duration
	cacheMax int
	cacheLen int

	// 重试：对网络错误与 503/504 瞬态最多重试 maxRetries 次，指数退避（retryBase 起，上限 2s）。
	maxRetries int
	retryBase  time.Duration

	// 单飞：启用后并发同 key 回源只打一次后端（防缓存击穿）。默认 nil = 关闭。
	sfOnce sync.Once
	sf     *util.Group

	// 缓存可观测性计数器（原子自增，并发安全），经 CacheStats() 暴露。
	hitC       int64
	missC      int64
	coalescedC int64
	negC       int64

	// 请求级超时（纳秒，原子存储）：当传入 ctx 无 deadline 时，回源请求附加此超时；0=不附加。
	reqTimeoutNs int64

	// gzip：开启后回源请求带 Accept-Encoding: gzip 并自动解压响应（省带宽）。
	gzip bool

	// 客户端熔断：开启后下游连续失败达阈值则快速失败，保护下游（复用 util.CircuitBreaker）。
	breaker *util.CircuitBreaker

	// 客户端级可观测指标（原子计数）。
	mReq   int64
	mErr   int64
	mLatNs int64
}

// cacheEntry 是单条缓存值及其绝对过期时刻。
type cacheEntry struct {
	val string
	exp time.Time
}

// BenchResult 汇总一次压测的结果。
type BenchResult struct {
	Ops       int
	Workers   int
	Op        string
	Duration  time.Duration
	OpsPerSec float64
	LatP50    float64
	LatP95    float64
	LatP99    float64
	Errors    int

	// 缓存层可观测性（仅当客户端启用缓存时非零）：压测窗口内的
	// 缓存命中/未命中/单飞合并次数，以及命中率（hits/(hits+misses)）。
	CacheHits    int64
	CacheMisses  int64
	Coalesced    int64
	CacheHitRate float64
}

// defaultBenchTimeout 是 Bench 的整体墙钟上限，防止后端挂死时压测无限拖尾。
const defaultBenchTimeout = 30 * time.Second

// Bench 启动 workers 个并发客户端，共执行 ops 次指定类型的操作
// （op 为 "get" / "put" / "mixed"），测量端到端吞吐与延迟分位数（毫秒）。
// 每个 worker 操作自己独立的 key 命名空间，保证 mixed/get 下读到的都是本 worker
// 写入的数据（不会被并发改写而读到空 / 陈旧值）。
// 整体受 defaultBenchTimeout 墙钟约束；超时后所有在途请求被取消、本轮提前结束。
func (c *Client) Bench(ops, workers int, op string, valueSize int) BenchResult {
	return c.BenchWithTimeout(ops, workers, op, valueSize, defaultBenchTimeout)
}

// BenchWithTimeout 同 Bench，但允许调用方指定整体墙钟超时。超时后本轮提前结束，
// 已完成的操作仍计入统计，在途请求因 ctx 取消而计入 Errors。
func (c *Client) BenchWithTimeout(ops, workers int, op string, valueSize int, timeout time.Duration) BenchResult {
	if workers < 1 {
		workers = 1
	}
	if ops < 1 {
		ops = 1
	}
	value := strings.Repeat("x", valueSize)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	perWorker := make([]int, workers)
	for i := 0; i < workers; i++ {
		perWorker[i] = ops / workers
	}
	for i := 0; i < ops%workers; i++ {
		perWorker[i]++
	}

	// 预热：为每个 worker 的 0 号 key 预写初值，保证 get/mixed 能读到数据。
	for w := 0; w < workers; w++ {
		_ = c.putCtx(ctx, fmt.Sprintf("bench-%d-0", w), value)
	}

	before := c.CacheStats() // 压测窗口起点快照（仅缓存开启时非零）
	var mu sync.Mutex
	var latencies []float64
	var errCount int64
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			local := make([]float64, 0, perWorker[w])
			for i := 0; i < perWorker[w]; i++ {
				key := fmt.Sprintf("bench-%d-%d", w, i)
				t0 := time.Now()
				var err error
				switch op {
				case "get":
					_, err = c.getCtx(ctx, key)
				case "put":
					err = c.putCtx(ctx, key, value)
				default: // mixed
					if err = c.putCtx(ctx, key, value); err == nil {
						_, err = c.getCtx(ctx, key)
					}
				}
				local = append(local, float64(time.Since(t0).Microseconds())/1000.0)
				if err != nil {
					atomic.AddInt64(&errCount, 1)
				}
			}
			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	dur := time.Since(start)

	res := BenchResult{Ops: ops, Workers: workers, Op: op, Duration: dur, Errors: int(errCount)}

	// 压测窗口缓存指标（仅当客户端启用缓存时非零）。
	after := c.CacheStats()
	res.CacheHits = after.Hits - before.Hits
	res.CacheMisses = after.Misses - before.Misses
	res.Coalesced = after.Coalesced - before.Coalesced
	if denom := res.CacheHits + res.CacheMisses; denom > 0 {
		res.CacheHitRate = float64(res.CacheHits) / float64(denom)
	}
	if dur > 0 {
		res.OpsPerSec = float64(ops) / dur.Seconds()
	}
	if len(latencies) > 0 {
		sort.Float64s(latencies)
		res.LatP50 = percentile(latencies, 0.50)
		res.LatP95 = percentile(latencies, 0.95)
		res.LatP99 = percentile(latencies, 0.99)
	}
	return res
}

func percentile(s []float64, q float64) float64 {
	if len(s) == 0 {
		return 0
	}
	idx := int(float64(q)*float64(len(s)-1) + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}

// NewClient 构造客户端，base 为网关根地址（如 http://localhost:8080）。
func NewClient(base string) *Client {
	return &Client{
		base: strings.TrimRight(base, "/"),
		http: &http.Client{Timeout: 5 * time.Second},
	}
}

// Close 释放客户端占用的空闲 TCP 连接，便于调用方在退出/热重载时主动回收资源，
// 避免连接池长期空占。已在进行中的在途请求不受影响（标准库语义）。
func (c *Client) Close() {
	c.http.CloseIdleConnections()
}

// EnableCache 开启读穿缓存：ttl 为条目有效期，max 为容量上限（超出按近似 FIFO
// 淘汰最旧条目）。关闭态（默认）下 Get 始终回源，零缓存行为影响——对现有无缓存
// 调用方完全透明。
func (c *Client) EnableCache(ttl time.Duration, max int) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	if c.cache == nil {
		c.cache = make(map[string]cacheEntry, max)
	}
	c.cacheTTL = ttl
	c.cacheMax = max
}

// storeCache 写入缓存；超出容量时淘汰过期最久（近似 FIFO）的条目。调用方须持 cacheMu。
func (c *Client) storeCache(key, val string) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	if c.cache == nil {
		return
	}
	if c.cacheMax > 0 && c.cacheLen >= c.cacheMax && len(c.cache) >= c.cacheMax {
		var oldestKey string
		var oldest time.Time
		first := true
		for k, e := range c.cache {
			if first || e.exp.Before(oldest) {
				oldest = e.exp
				oldestKey = k
				first = false
			}
		}
		if oldestKey != "" {
			delete(c.cache, oldestKey)
			c.cacheLen--
		}
	}
	c.cache[key] = cacheEntry{val: val, exp: time.Now().Add(c.cacheTTL)}
	c.cacheLen++
}

// invalidateCache 使某 key 的缓存失效（Put/Append 成功后调用）。调用方须持 cacheMu。
func (c *Client) invalidateCache(key string) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	if c.cache == nil {
		return
	}
	if _, ok := c.cache[key]; ok {
		delete(c.cache, key)
		c.cacheLen--
	}
}

// SetRetry 配置客户端级重试：对网络错误与 503/504（瞬态）最多重试 max 次，
// 指数退避（base 起，上限 2s）。max<=0 表示不重试（默认，保持历史行为）。
// Put/Append 经网关 Clerk 幂等去重，重试安全；Get 天然幂等，可安全重试。
// SetRequestTimeout 设置请求级超时：当传入的 ctx 没有 deadline 时，回源请求会附加该超时。
// 设为 0 则不附加（完全依赖传入 ctx）。可在并发使用前调用，内部以原子存储保证并发安全。
func (c *Client) SetRequestTimeout(d time.Duration) {
	atomic.StoreInt64(&c.reqTimeoutNs, int64(d))
}

// ctxForRequest 在 ctx 无 deadline 时包裹请求级超时，返回新 ctx 与 cancel（调用方须 defer cancel）。
// 已有 deadline 则不复盖（避免调用方超时失效），保证回源不会因包装而突破调用方约束。
func (c *Client) ctxForRequest(ctx context.Context) (context.Context, context.CancelFunc) {
	ns := atomic.LoadInt64(&c.reqTimeoutNs)
	if ns <= 0 {
		return ctx, func() {}
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, time.Duration(ns))
}

func (c *Client) SetRetry(max int, base time.Duration) {
	c.maxRetries = max
	if base <= 0 {
		base = 50 * time.Millisecond
	}
	c.retryBase = base
}

// backoffFor 计算第 attempt 次重试（attempt>=1）的退避时长：retryBase * 2^(attempt-1)，上限 2s。
func (c *Client) backoffFor(attempt int) time.Duration {
	d := c.retryBase * time.Duration(1<<uint(attempt-1))
	if d > 2*time.Second {
		d = 2 * time.Second
	}
	return d
}

// EnableSingleFlight 开启回源单飞：并发同 key 的 Get 只回源一次，其余复用，
// 防止热点 key 同时失效时把压力集中打到后端（缓存击穿）。默认关闭，
// 对未启用单飞的调用方完全透明。
func (c *Client) EnableSingleFlight() {
	c.sfOnce.Do(func() {
		c.sf = util.NewGroup()
	})
}

// maxErrBody 是错误响应体在错误信息中保留的最大字节数，避免把巨大响应体塞进 error。
const maxErrBody = 256

// respErr 把一次失败的 HTTP 响应构造成带上下文的 error：包含方法、key、状态码，
// 以及（截断后的）响应体。网关返回的业务错误（如 "key not found"）原本会被
// 静默丢弃，导致排障时无据可查——这里把它显式带出来。
func respErr(method, key string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrBody))
	msg := strings.TrimSpace(string(body))
	if msg == "" {
		msg = "(no body)"
	}
	return fmt.Errorf("%s /kv/%s: status %d: %s", method, key, resp.StatusCode, msg)
}

// Get 读取 key 的当前值。网关返回非 200 时返回错误（含响应体），而非静默返回空串。
func (c *Client) Get(key string) (string, error) {
	return c.getCtx(context.Background(), key)
}

// GetCtx 带 ctx 读取 key（见 Get）。调用方可用 ctx 控制超时/取消；传入的 ctx 若已带 deadline，
// 不会被请求级超时（SetRequestTimeout）覆盖，保证调用方约束优先。
func (c *Client) GetCtx(ctx context.Context, key string) (string, error) {
	return c.getCtx(ctx, key)
}

// fetchGet 是 Get 的纯回源逻辑（含重试、熔断守卫、gzip 解压、指标），不含缓存/单飞。
func (c *Client) fetchGet(ctx context.Context, key string) (string, error) {
	if c.breakerOpen() {
		return "", fmt.Errorf("kvcli: circuit breaker open, fast-fail")
	}
	start := time.Now()
	var lastErr error
	reqCtx, cancel := c.ctxForRequest(ctx)
	defer cancel()
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, c.base+"/kv/"+url.PathEscape(key), nil)
		if err != nil {
			lastErr = err
			break
		}
		if c.gzip {
			req.Header.Set("Accept-Encoding", "gzip")
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err // 网络错误：瞬态，可重试
			if attempt < c.maxRetries {
				time.Sleep(c.backoffFor(attempt + 1))
			}
			continue
		}
		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
			resp.Body.Close()
			lastErr = fmt.Errorf("retryable status %d for GET /kv/%s", resp.StatusCode, key)
			if attempt < c.maxRetries {
				if d, ok := retryAfterSeconds(resp); ok {
					time.Sleep(d)
				} else {
					time.Sleep(c.backoffFor(attempt + 1))
				}
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			err = respErr("GET", key, resp)
			resp.Body.Close()
			c.recordCall(start, err)
			c.breakerRecord(err)
			return "", err
		}
		b, err := readRespBody(resp)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			if attempt < c.maxRetries {
				time.Sleep(c.backoffFor(attempt + 1))
			}
			continue
		}
		c.recordCall(start, nil)
		c.breakerRecord(nil)
		return string(b), nil
	}
	c.recordCall(start, lastErr)
	c.breakerRecord(lastErr)
	return "", lastErr
}

func (c *Client) getCtx(ctx context.Context, key string) (string, error) {
	// 读穿缓存：命中且未过期直接返回，跳过回源（零额外网络开销）。
	if c.cache != nil {
		c.cacheMu.Lock()
		if e, ok := c.cache[key]; ok && time.Now().Before(e.exp) {
			c.cacheMu.Unlock()
			atomic.AddInt64(&c.hitC, 1)
			if e.val == "" {
				atomic.AddInt64(&c.negC, 1) // 命中空值 = 穿透已被缓解的负向结果
			}
			return e.val, nil
		}
		c.cacheMu.Unlock()
		atomic.AddInt64(&c.missC, 1) // 缓存未命中，将回源
	}
	// 回源：若开启单飞，并发同 key 只回源一次（防缓存击穿）；否则直接回源。
	var s string
	var err error
	if c.sf != nil {
		v, ferr, called := c.sf.Do(key, func() (interface{}, error) {
			return c.fetchGet(ctx, key)
		})
		if !called {
			atomic.AddInt64(&c.coalescedC, 1) // 并发同 key 被单飞合并，复用他人回源
		}
		if ferr == nil {
			s = v.(string)
		}
		err = ferr
	} else {
		s, err = c.fetchGet(ctx, key)
	}
	if err != nil {
		return "", err
	}
	// 回源成功则写缓存（无论是否单飞、是否复用结果，幂等无害，且仅刷新 TTL）。
	if c.cache != nil {
		c.storeCache(key, s)
	}
	return s, nil
}

// MGetResult 是 MGet 批量读取的结果：Results 为成功 key→value，Errors 为失败
// key→error（按 key 索引）。部分 key 失败不会阻断其余 key，便于调用方做降级处理。
type MGetResult struct {
	Results map[string]string
	Errors  map[string]error
}

// MGet 并发批量读取多个 key（见 MGetCtx）。无 ctx 版本用 Background，适合一次性批量拉取。
func (c *Client) MGet(keys []string) MGetResult {
	return c.MGetCtx(context.Background(), keys)
}

// MGetCtx 并发批量读取多个 key，复用单 key 的回源/缓存/单飞/重试全链路，
// 连接复用（同一个 *http.Client，HTTP keep-alive）。每个 key 独立成功/失败，
// 互不阻断：成功的进入 Results，失败的进入 Errors。并发由 goroutine+WaitGroup
// 驱动（key 数通常有限；超大批量由调用方自行分批）。空输入安全返回空结果。
// 注意：当某 key 在 keys 中重复出现且已启用单飞时，重复项会被合并为一次回源。
func (c *Client) MGetCtx(ctx context.Context, keys []string) MGetResult {
	res := MGetResult{
		Results: make(map[string]string, len(keys)),
		Errors:  make(map[string]error, 0),
	}
	if len(keys) == 0 {
		return res
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, k := range keys {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			v, err := c.getCtx(ctx, k)
			mu.Lock()
			if err != nil {
				res.Errors[k] = err
			} else {
				res.Results[k] = v
			}
			mu.Unlock()
		}(k)
	}
	wg.Wait()
	return res
}

// MSetResult 批量写入结果：按 key 归集成功/失败，互不阻断。
type MSetResult struct {
	Results map[string]error // 每个 key 的写入结果：nil=成功
	Errors  map[string]error // 仅失败项，便于调用方快速遍历处理
	Failed  int              // 失败总数
	Total   int              // 总 key 数
}

// MSet 并发批量写入多个 key-value（见 MSetCtx）。
func (c *Client) MSet(pairs map[string]string) MSetResult {
	return c.MSetCtx(context.Background(), pairs)
}

// MSetCtx 并发批量写入，复用单 key 的回源/重试/缓存失效全链路。
// 每个 (key,value) 独立成功/失败，互不阻断：成功的在 Results 记为 nil，
// 失败的进入 Results 与 Errors。写成功后 putCtx 已顺带失效本地缓存，保证写后读一致。
// 空输入安全返回空结果。并发由 goroutine+WaitGroup 驱动（key 数通常有限；
// 超大批量由调用方自行分批）。
func (c *Client) MSetCtx(ctx context.Context, pairs map[string]string) MSetResult {
	res := MSetResult{
		Results: make(map[string]error, len(pairs)),
		Errors:  make(map[string]error, 0),
		Total:   len(pairs),
	}
	if len(pairs) == 0 {
		return res
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for k, v := range pairs {
		wg.Add(1)
		go func(k, v string) {
			defer wg.Done()
			err := c.putCtx(ctx, k, v)
			mu.Lock()
			res.Results[k] = err
			if err != nil {
				res.Errors[k] = err
			}
			mu.Unlock()
		}(k, v)
	}
	wg.Wait()
	res.Failed = len(res.Errors)
	return res
}

// WarmUp 并发预热指定 keys 到本地缓存（复用 MGet 回源链路，成功后写入缓存）。
// 适合启动期/定时批量填充热点 key，降低首次访问延迟。部分失败不阻断、返回聚合错误。
// 需先启用缓存（EnableCache），否则返回错误（无缓存则预热无意义）。
func (c *Client) WarmUp(keys []string) error {
	c.cacheMu.Lock()
	enabled := c.cache != nil
	c.cacheMu.Unlock()
	if !enabled {
		return fmt.Errorf("WarmUp 需要启用缓存：先调用 EnableCache")
	}
	res := c.MGet(keys)
	if len(res.Errors) > 0 {
		return fmt.Errorf("warmup 失败 %d/%d 个 key", len(res.Errors), len(keys))
	}
	return nil
}

// CacheStats 汇总客户端缓存层的可观测指标（并发原子计数快照）。
type CacheStats struct {
	Hits      int64 // 缓存命中（含负向空值命中）
	Misses    int64 // 缓存未命中、已回源
	Coalesced int64 // 单飞合并：并发同 key 复用他人回源
	Negative  int64 // 命中的空值条目数（穿透已被缓解的信号）
}

// CacheStats 返回当前缓存计数器快照，便于接入外部 metrics 或排障。
func (c *Client) CacheStats() CacheStats {
	return CacheStats{
		Hits:      atomic.LoadInt64(&c.hitC),
		Misses:    atomic.LoadInt64(&c.missC),
		Coalesced: atomic.LoadInt64(&c.coalescedC),
		Negative:  atomic.LoadInt64(&c.negC),
	}
}

// Put 写入 key = value。网关返回非 200 时返回错误（含响应体）。
func (c *Client) Put(key, value string) error {
	return c.putCtx(context.Background(), key, value)
}

func (c *Client) putCtx(ctx context.Context, key, value string) error {
	if c.breakerOpen() {
		return fmt.Errorf("kvcli: circuit breaker open, fast-fail")
	}
	start := time.Now()
	var lastErr error
	reqCtx, cancel := c.ctxForRequest(ctx)
	defer cancel()
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPut, c.base+"/kv/"+url.PathEscape(key), strings.NewReader(value))
		if err != nil {
			lastErr = err
			break
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			if attempt < c.maxRetries {
				time.Sleep(c.backoffFor(attempt + 1))
			}
			continue
		}
		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
			resp.Body.Close()
			lastErr = fmt.Errorf("retryable status %d for PUT /kv/%s", resp.StatusCode, key)
			if attempt < c.maxRetries {
				if d, ok := retryAfterSeconds(resp); ok {
					time.Sleep(d)
				} else {
					time.Sleep(c.backoffFor(attempt + 1))
				}
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			err = respErr("PUT", key, resp)
			resp.Body.Close()
			c.recordCall(start, err)
			c.breakerRecord(err)
			return err
		}
		resp.Body.Close()
		c.invalidateCache(key)
		c.recordCall(start, nil)
		c.breakerRecord(nil)
		return nil
	}
	c.recordCall(start, lastErr)
	c.breakerRecord(lastErr)
	return lastErr
}

// Append 把 value 追加到 key 的当前值之后。网关返回非 200 时返回错误（含响应体）。
func (c *Client) Append(key, value string) error {
	return c.appendCtx(context.Background(), key, value)
}

func (c *Client) appendCtx(ctx context.Context, key, value string) error {
	if c.breakerOpen() {
		return fmt.Errorf("kvcli: circuit breaker open, fast-fail")
	}
	start := time.Now()
	var lastErr error
	reqCtx, cancel := c.ctxForRequest(ctx)
	defer cancel()
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.base+"/kv/"+url.PathEscape(key)+"/append", strings.NewReader(value))
		if err != nil {
			lastErr = err
			break
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			if attempt < c.maxRetries {
				time.Sleep(c.backoffFor(attempt + 1))
			}
			continue
		}
		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
			resp.Body.Close()
			lastErr = fmt.Errorf("retryable status %d for POST /kv/%s/append", resp.StatusCode, key)
			if attempt < c.maxRetries {
				if d, ok := retryAfterSeconds(resp); ok {
					time.Sleep(d)
				} else {
					time.Sleep(c.backoffFor(attempt + 1))
				}
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			err = respErr("POST", key, resp)
			resp.Body.Close()
			c.recordCall(start, err)
			c.breakerRecord(err)
			return err
		}
		resp.Body.Close()
		c.invalidateCache(key)
		c.recordCall(start, nil)
		c.breakerRecord(nil)
		return nil
	}
	c.recordCall(start, lastErr)
	c.breakerRecord(lastErr)
	return lastErr
}
