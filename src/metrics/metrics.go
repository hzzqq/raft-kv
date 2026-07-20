// metrics.go —— 零依赖、并发安全的轻量级可观测性指标库。
//
// 设计目标：
//   - 无外部依赖（仅标准库），可在 raft / kvraft / shardkv 各包中被引入；
//   - 计数用原子操作、直方图用有界环形缓冲，热路径开销可忽略；
//   - 进程级（best-effort）聚合：各包持有一个 Registry，供网关 / 演示程序读取。
//
// 注意：本库不保证跨多集群实例的精确隔离——同一进程内多个 Raft/KV 实例会共享
// 包级 Registry 的计数。这对"可观测性近似指标"是可接受的；若需严格隔离，可在
// 调用方创建独立 Registry 实例后注入。
package metrics

import (
	"encoding/json"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Counter 是并发安全的单调递增计数器。
type Counter struct {
	v int64
}

// Inc 自增 1，返回新值。
func (c *Counter) Inc() int64 { return atomic.AddInt64(&c.v, 1) }

// Add 增加 n（可为负），返回新值。
func (c *Counter) Add(n int64) int64 { return atomic.AddInt64(&c.v, n) }

// Value 返回当前值。
func (c *Counter) Value() int64 { return atomic.LoadInt64(&c.v) }

// Histogram 记录 float64 样本（如延迟毫秒数），使用固定容量环形缓冲，
// 无论样本量多大都保持内存有界、分位数查询廉价。
type Histogram struct {
	mu    sync.Mutex
	cap   int
	pos   int
	samples []float64
	count int64
	sum   float64
}

const defaultHistCap = 4096

// NewHistogram 创建一个直方图；capacity 省略或 <=0 时使用默认容量 4096。
func NewHistogram(capacity ...int) *Histogram {
	cap := defaultHistCap
	if len(capacity) > 0 && capacity[0] > 0 {
		cap = capacity[0]
	}
	return &Histogram{cap: cap, samples: make([]float64, 0, cap)}
}

// Record 记录一个样本。
func (h *Histogram) Record(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
	h.sum += v
	if len(h.samples) < h.cap {
		h.samples = append(h.samples, v)
		return
	}
	// 缓冲满后按环形覆盖，避免无限增长。
	h.samples[h.pos%h.cap] = v
	h.pos++
}

// HistSnapshot 是直方图的 JSON 友好快照。
type HistSnapshot struct {
	Count int64   `json:"count"`
	Sum   float64 `json:"sum"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
}

// Snapshot 返回当前分位数统计。
func (h *Histogram) Snapshot() HistSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := len(h.samples)
	if n == 0 {
		return HistSnapshot{}
	}
	sorted := append([]float64(nil), h.samples...)
	sort.Float64s(sorted)
	mean := h.sum / float64(n)
	return HistSnapshot{
		Count: h.count,
		Sum:   h.sum,
		Mean:  mean,
		P50:   percentile(sorted, 0.50),
		P95:   percentile(sorted, 0.95),
		P99:   percentile(sorted, 0.99),
	}
}

func percentile(s []float64, q float64) float64 {
	if len(s) == 0 {
		return 0
	}
	idx := int(math.Floor(q*float64(len(s)-1) + 0.5))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}

// Registry 聚合一组命名计数器与直方图，对应一个组件的可观测性指标。
type Registry struct {
	mu         sync.Mutex
	counters   map[string]*Counter
	histograms map[string]*Histogram
}

// NewRegistry 创建一个空的指标注册表。
func NewRegistry() *Registry {
	return &Registry{
		counters:   map[string]*Counter{},
		histograms: map[string]*Histogram{},
	}
}

// Counter 取得（不存在则创建）命名计数器。
func (r *Registry) Counter(name string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[name]; ok {
		return c
	}
	c := &Counter{}
	r.counters[name] = c
	return c
}

// Histogram 取得（不存在则创建）命名直方图。
func (r *Registry) Histogram(name string) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h, ok := r.histograms[name]; ok {
		return h
	}
	h := NewHistogram()
	r.histograms[name] = h
	return h
}

// Snapshot 返回 JSON 友好结构：{"counters": {...}, "histograms": {...}}。
func (r *Registry) Snapshot() map[string]interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	counters := make(map[string]int64, len(r.counters))
	for k, v := range r.counters {
		counters[k] = v.Value()
	}
	hists := make(map[string]HistSnapshot, len(r.histograms))
	for k, v := range r.histograms {
		hists[k] = v.Snapshot()
	}
	return map[string]interface{}{
		"counters":   counters,
		"histograms": hists,
	}
}

// Reset 清空所有计数器与直方图，供独立运行间重置，避免进程级指标跨用例累积。
func (r *Registry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters = map[string]*Counter{}
	r.histograms = map[string]*Histogram{}
}

// DumpJSON 把当前快照序列化为 JSON 字节，便于网关 / 演示程序直接输出。
func (r *Registry) DumpJSON() ([]byte, error) {
	return json.Marshal(r.Snapshot())
}

// StartPeriodicReporter 起一个后台 goroutine，每隔 interval 把快照 JSON 写入 w，
// 直到 stop 被关闭。调用方负责关闭 stop 以回收 goroutine（否则会泄漏）。
// 纯工具函数，不影响任何指标采集路径。
func StartPeriodicReporter(r *Registry, interval time.Duration, w io.Writer, stop <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				b, err := r.DumpJSON()
				if err != nil {
					continue
				}
				_, _ = w.Write(append(b, '\n'))
			}
		}
	}()
}
