// client.go —— kvcli 的 HTTP 客户端核心
//
// Client 封装对网关（gateway）的 REST 调用：GET /kv/{key}、PUT /kv/{key}、
// POST /kv/{key}/append。核心方法可单测（对 httptest 起的网关发请求）。
package main

import (
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

// Bench 启动 workers 个并发客户端，共执行 ops 次指定类型的操作
// （op 为 "get" / "put" / "mixed"），测量端到端吞吐与延迟分位数（毫秒）。
// 每个 worker 操作自己独立的 key 命名空间，保证 mixed/get 下读到的都是本 worker
// 写入的数据（不会被并发改写而读到空 / 陈旧值）。
func (c *Client) Bench(ops, workers int, op string, valueSize int) BenchResult {
	if workers < 1 {
		workers = 1
	}
	if ops < 1 {
		ops = 1
	}
	value := strings.Repeat("x", valueSize)
	perWorker := make([]int, workers)
	for i := 0; i < workers; i++ {
		perWorker[i] = ops / workers
	}
	for i := 0; i < ops%workers; i++ {
		perWorker[i]++
	}

	// 预热：为每个 worker 的 0 号 key 预写初值，保证 get/mixed 能读到数据。
	for w := 0; w < workers; w++ {
		_ = c.Put(fmt.Sprintf("bench-%d-0", w), value)
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
					_, err = c.Get(key)
				case "put":
					err = c.Put(key, value)
				default: // mixed
					if err = c.Put(key, value); err == nil {
						_, err = c.Get(key)
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
	idx := int(float64(q) * float64(len(s)-1) + 0.5)
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

// Get 读取 key 的当前值。网关返回非 200 时返回错误（而非静默返回空串）。
func (c *Client) Get(key string) (string, error) {
	resp, err := c.http.Get(c.base + "/kv/" + url.PathEscape(key))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET /kv/%s: status %d", key, resp.StatusCode)
	}
	return string(b), nil
}

// Put 写入 key = value。网关返回非 200 时返回错误。
func (c *Client) Put(key, value string) error {
	req, err := http.NewRequest(http.MethodPut, c.base+"/kv/"+url.PathEscape(key), strings.NewReader(value))
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT /kv/%s: status %d", key, resp.StatusCode)
	}
	return nil
}

// Append 把 value 追加到 key 的当前值之后。网关返回非 200 时返回错误。
func (c *Client) Append(key, value string) error {
	resp, err := c.http.Post(c.base+"/kv/"+url.PathEscape(key)+"/append", "text/plain", strings.NewReader(value))
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("POST /kv/%s/append: status %d", key, resp.StatusCode)
	}
	return nil
}
