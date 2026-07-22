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
	"fmt"
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

// Gauge 是并发安全的瞬时值指标（可任意 Set，用于当前配置号、apply 滞后等）。
type Gauge struct {
	v int64
}

// Set 设置当前值（以 float64 位模式原子存储，避免额外类型转换开销）。
func (g *Gauge) Set(v float64) {
	atomic.StoreInt64(&g.v, int64(math.Float64bits(v)))
}

// Value 返回当前值。
func (g *Gauge) Value() float64 {
	return math.Float64frombits(uint64(atomic.LoadInt64(&g.v)))
}

// Histogram 记录 float64 样本（如延迟毫秒数），使用固定容量环形缓冲，
// 无论样本量多大都保持内存有界、分位数查询廉价。
type Histogram struct {
	mu      sync.Mutex
	cap     int
	pos     int
	samples []float64
	count   int64
	sum     float64
	min     float64
	max     float64
}

const defaultHistCap = 4096

// NewHistogram 创建一个直方图；capacity 省略或 <=0 时使用默认容量 4096。
func NewHistogram(capacity ...int) *Histogram {
	cap := defaultHistCap
	if len(capacity) > 0 && capacity[0] > 0 {
		cap = capacity[0]
	}
	return &Histogram{cap: cap, samples: make([]float64, 0, cap), min: math.Inf(1), max: math.Inf(-1)}
}

// Record 记录一个样本。
func (h *Histogram) Record(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
	h.sum += v
	if v < h.min {
		h.min = v
	}
	if v > h.max {
		h.max = v
	}
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
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
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
		Min:   h.min,
		Max:   h.max,
		P50:   percentile(sorted, 0.50),
		P95:   percentile(sorted, 0.95),
		P99:   percentile(sorted, 0.99),
	}
}

// Timer 是绑定到某直方图的一次计时器。调用方在待测区间起止分别调用 Histogram.Timer()
// 与 Timer.Stop()，经过的毫秒耗时即被 Record 进直方图——比手动 Record 更不易漏写。
type Timer struct {
	h     *Histogram
	start time.Time
}

// Timer 返回一个已起算的计时器，绑定到当前直方图。
func (h *Histogram) Timer() *Timer {
	return &Timer{h: h, start: time.Now()}
}

// Stop 停止计时并把耗时（毫秒）记录进直方图。
func (t *Timer) Stop() {
	t.h.Record(float64(time.Since(t.start).Microseconds()) / 1000.0)
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
	gauges     map[string]*Gauge
}

// NewRegistry 创建一个空的指标注册表。
func NewRegistry() *Registry {
	return &Registry{
		counters:   map[string]*Counter{},
		histograms: map[string]*Histogram{},
		gauges:     map[string]*Gauge{},
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

// Gauge 取得（不存在则创建）命名瞬时值指标。
func (r *Registry) Gauge(name string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.gauges[name]; ok {
		return g
	}
	g := &Gauge{}
	r.gauges[name] = g
	return g
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
	gauges := make(map[string]float64, len(r.gauges))
	for k, v := range r.gauges {
		gauges[k] = v.Value()
	}
	return map[string]interface{}{
		"counters":   counters,
		"histograms": hists,
		"gauges":     gauges,
	}
}

// Reset 清空所有计数器与直方图，供独立运行间重置，避免进程级指标跨用例累积。
func (r *Registry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters = map[string]*Counter{}
	r.histograms = map[string]*Histogram{}
	r.gauges = map[string]*Gauge{}
}

// DumpJSON 把当前快照序列化为 JSON 字节，便于网关 / 演示程序直接输出。
func (r *Registry) DumpJSON() ([]byte, error) {
	return json.Marshal(r.Snapshot())
}

// sanitizeMetricName 把任意注册名转换为合法的 Prometheus 序列名。
// Prometheus 规范：名字必须匹配 [a-zA-Z_:][a-zA-Z0-9_:]*。各包用带点前缀
// （如 "shardkv.op_latency_ms"）或连字符命名时，直接写入 exposition 会被
// scrape 客户端判为非法而整体拒绝——这是一个静默的可观测性缺陷。此处把非法
// 字符统一替换为 '_'，并对以数字开头的名字前置 '_'，保证输出恒为合法格式。
func sanitizeMetricName(name string) string {
	if name == "" {
		return "_"
	}
	b := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == ':' ||
			(c >= '0' && c <= '9' && i > 0)
		if ok {
			b = append(b, c)
		} else {
			b = append(b, '_')
		}
	}
	return string(b)
}

// WritePrometheus 把注册表以 Prometheus 文本 exposition 格式写入 w，便于被
// Prometheus / 任意 scrape 客户端采集。轻量级实现：
//   - counter / gauge 直接输出为同名序列（各自声明对应 TYPE）；
//   - histogram 拆为多条派生序列：_count 声明为 counter，_sum / _p50 / _p95 /
//     _p99 声明为 gauge（分位数就是瞬时值）。注意：不再对聚合名声明
//     "# TYPE <name> histogram"，因为本库输出的是分位数派生序列而非规范要求的
//     `_bucket{le=...}`；错误地声明 histogram 类型会导致 scrape 客户端解析失败。
//
// 所有序列名经 sanitizeMetricName 清洗为合法格式；序列按字母序稳定输出，便于
// 测试断言。Content-Type 由调用方设置。
func (r *Registry) WritePrometheus(w io.Writer) error {
	snap := r.Snapshot()
	counters, _ := snap["counters"].(map[string]int64)
	gauges, _ := snap["gauges"].(map[string]float64)
	hists, _ := snap["histograms"].(map[string]HistSnapshot)

	names := make([]string, 0, len(counters)+len(gauges))
	for k := range counters {
		names = append(names, k)
	}
	for k := range gauges {
		names = append(names, k)
	}
	sort.Strings(names)

	// 先输出纯 counter/gauge 序列
	for _, name := range names {
		sn := sanitizeMetricName(name)
		if v, ok := counters[name]; ok {
			if _, err := fmt.Fprintf(w, "# TYPE %s counter\n%s %d\n", sn, sn, v); err != nil {
				return err
			}
			continue
		}
		if v, ok := gauges[name]; ok {
			if _, err := fmt.Fprintf(w, "# TYPE %s gauge\n%s %g\n", sn, sn, v); err != nil {
				return err
			}
		}
	}

	// 直方图派生序列（顺序稳定，每条序列 TYPE 与其真实语义一致）
	hnames := make([]string, 0, len(hists))
	for k := range hists {
		hnames = append(hnames, k)
	}
	sort.Strings(hnames)
	for _, name := range hnames {
		sn := sanitizeMetricName(name)
		h := hists[name]
		if _, err := fmt.Fprintf(w,
			"# TYPE %s_count counter\n%s_count %d\n"+
				"# TYPE %s_sum gauge\n%s_sum %g\n"+
				"# TYPE %s_p50 gauge\n%s_p50 %g\n"+
				"# TYPE %s_p95 gauge\n%s_p95 %g\n"+
				"# TYPE %s_p99 gauge\n%s_p99 %g\n",
			sn, sn, h.Count, sn, sn, h.Sum, sn, sn, h.P50, sn, sn, h.P95, sn, sn, h.P99); err != nil {
			return err
		}
	}
	return nil
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
