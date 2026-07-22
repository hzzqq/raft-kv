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

func (c *Client) getCtx(ctx context.Context, key string) (string, error) {
	// 读穿缓存：命中且未过期直接返回，跳过回源（零额外网络开销）。
	if c.cache != nil {
		c.cacheMu.Lock()
		if e, ok := c.cache[key]; ok && time.Now().Before(e.exp) {
			c.cacheMu.Unlock()
			return e.val, nil
		}
		c.cacheMu.Unlock()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/kv/"+url.PathEscape(key), nil)
	if err != nil {
		return "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", respErr("GET", key, resp)
	}
	b, _ := io.ReadAll(resp.Body)
	if c.cache != nil {
		c.storeCache(key, string(b))
	}
	return string(b), nil
}

// Put 写入 key = value。网关返回非 200 时返回错误（含响应体）。
func (c *Client) Put(key, value string) error {
	return c.putCtx(context.Background(), key, value)
}

func (c *Client) putCtx(ctx context.Context, key, value string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.base+"/kv/"+url.PathEscape(key), strings.NewReader(value))
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return respErr("PUT", key, resp)
	}
	c.invalidateCache(key)
	return nil
}

// Append 把 value 追加到 key 的当前值之后。网关返回非 200 时返回错误（含响应体）。
func (c *Client) Append(key, value string) error {
	return c.appendCtx(context.Background(), key, value)
}

func (c *Client) appendCtx(ctx context.Context, key, value string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/kv/"+url.PathEscape(key)+"/append", strings.NewReader(value))
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return respErr("POST", key, resp)
	}
	c.invalidateCache(key)
	return nil
}
