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

// fetchGet 是 Get 的纯回源逻辑（含重试），不含缓存/单飞。返回后端原始响应体字符串。
func (c *Client) fetchGet(ctx context.Context, key string) (string, error) {
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(c.backoffFor(attempt))
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/kv/"+url.PathEscape(key), nil)
		if err != nil {
			return "", err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err // 网络错误：瞬态，可重试
			continue
		}
		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
			resp.Body.Close()
			lastErr = fmt.Errorf("retryable status %d for GET /kv/%s", resp.StatusCode, key)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return "", respErr("GET", key, resp)
		}
		b, _ := io.ReadAll(resp.Body)
		return string(b), nil
	}
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
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(c.backoffFor(attempt))
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.base+"/kv/"+url.PathEscape(key), strings.NewReader(value))
		if err != nil {
			return err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
			resp.Body.Close()
			lastErr = fmt.Errorf("retryable status %d for PUT /kv/%s", resp.StatusCode, key)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return respErr("PUT", key, resp)
		}
		c.invalidateCache(key)
		return nil
	}
	return lastErr
}

// Append 把 value 追加到 key 的当前值之后。网关返回非 200 时返回错误（含响应体）。
func (c *Client) Append(key, value string) error {
	return c.appendCtx(context.Background(), key, value)
}

func (c *Client) appendCtx(ctx context.Context, key, value string) error {
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(c.backoffFor(attempt))
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/kv/"+url.PathEscape(key)+"/append", strings.NewReader(value))
		if err != nil {
			return err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
			resp.Body.Close()
			lastErr = fmt.Errorf("retryable status %d for POST /kv/%s/append", resp.StatusCode, key)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return respErr("POST", key, resp)
		}
		c.invalidateCache(key)
		return nil
	}
	return lastErr
}
