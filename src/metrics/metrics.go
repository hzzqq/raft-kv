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
	"strings"
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

// Sub 减少 n（等价 Add(-n)），返回新值。便于「当前活跃数 = 增 + 减」类 delta 计数。
func (c *Counter) Sub(n int64) int64 { return atomic.AddInt64(&c.v, -n) }

// Dec 自减 1，返回新值。
func (c *Counter) Dec() int64 { return atomic.AddInt64(&c.v, -1) }

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

// Histogram 记录 float64 样本（如延迟毫秒数），按时间滑窗维护分位数，
// 同时用固定容量上限保证内存有界。分位数（p50/p95/p99）只统计滑窗内的样本，
// 因此故障期流量骤降时不会因残留历史样本而把 p99 算「虚低」——这是相对
// 「按观测次数滑窗的样本环形缓冲」的关键修正（后者会把很久以前的样本一直算进来）。
//
// 滑窗语义：Snapshot 仅对 [now-window, now] 内的样本取分位；Record 时懒淘汰窗口外
// 样本；若窗口内样本数超过 cap（极端高 QPS），再丢弃最旧者（内存安全上界）。
type Histogram struct {
	mu      sync.Mutex
	cap     int
	window  time.Duration
	samples []histSample
}

// histSample 是带采集时间戳的单条样本，用于时间滑窗淘汰。
type histSample struct {
	v  float64
	ts time.Time
}

const defaultHistCap = 8192

// defaultHistWindow 是默认时间滑窗宽度；分位数只反映最近该时长内的观测。
// 用 var 而非 const，以便进程启动时通过 SetDefaultHistWindow 覆盖（如按业务
// 观测节奏调大/调小窗口）。已创建的直方图保留各自窗口，不受影响。
var defaultHistWindow = 60 * time.Second

// SetDefaultHistWindow 覆盖默认时间滑窗宽度（interval<=0 时忽略）。
// 应在创建任何直方图之前调用（网关在 main 读 RAFTKV_HIST_WINDOW 后调用）。
func SetDefaultHistWindow(interval time.Duration) {
	if interval > 0 {
		defaultHistWindow = interval
	}
}

// DefaultHistWindow 返回当前默认时间滑窗宽度（供 HELP 文案动态反映配置值）。
func DefaultHistWindow() time.Duration { return defaultHistWindow }

// NewHistogram 创建一个直方图；capacity 省略或 <=0 时使用默认容量 8192，
// 时间滑窗使用默认 60s。
func NewHistogram(capacity ...int) *Histogram {
	return NewTimeWindowHistogram(defaultHistWindow, capacity...)
}

// NewTimeWindowHistogram 创建指定时间滑窗与容量上限的直方图。
// window<=0 时回退到 defaultHistWindow；capacity<=0 时回退到 defaultHistCap。
func NewTimeWindowHistogram(window time.Duration, capacity ...int) *Histogram {
	cap := defaultHistCap
	if len(capacity) > 0 && capacity[0] > 0 {
		cap = capacity[0]
	}
	if window <= 0 {
		window = defaultHistWindow
	}
	return &Histogram{cap: cap, window: window}
}

// Record 记录一个样本（以当前时间作为采集时刻）。
func (h *Histogram) Record(v float64) {
	h.recordAt(v, time.Now())
}

// recordAt 以指定采集时刻记录样本（供测试注入确定性时间戳）。
func (h *Histogram) recordAt(v float64, ts time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.samples = append(h.samples, histSample{v: v, ts: ts})
	// 懒淘汰：先丢窗口外（过期）样本，再按容量上限丢最旧（窗口内超量）。
	cutoff := ts.Add(-h.window)
	i := 0
	for i < len(h.samples) && h.samples[i].ts.Before(cutoff) {
		i++
	}
	if i > 0 {
		h.samples = h.samples[i:]
	}
	if len(h.samples) > h.cap {
		h.samples = h.samples[len(h.samples)-h.cap:]
	}
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

// Snapshot 返回当前时间滑窗内的分位数统计（只统计 [now-window, now] 的样本）。
func (h *Histogram) Snapshot() HistSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-h.window)
	var vals []float64
	var sum, mn, mx float64
	first := true
	for _, s := range h.samples {
		if s.ts.Before(cutoff) {
			continue
		}
		vals = append(vals, s.v)
		sum += s.v
		if first || s.v < mn {
			mn = s.v
		}
		if first || s.v > mx {
			mx = s.v
		}
		first = false
	}
	n := int64(len(vals))
	if n == 0 {
		return HistSnapshot{}
	}
	sort.Float64s(vals)
	return HistSnapshot{
		Count: n,
		Sum:   sum,
		Mean:  sum / float64(n),
		Min:   mn,
		Max:   mx,
		P50:   percentile(vals, 0.50),
		P95:   percentile(vals, 0.95),
		P99:   percentile(vals, 0.99),
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
	mu          *sync.Mutex
	counters    map[string]*Counter
	counterVecs map[string]*CounterVec // 带标签维度的 counter 向量（cycle #117）
	histograms  map[string]*Histogram
	gauges      map[string]*Gauge
	funcGauges  map[string]*FuncGauge // 函数式瞬时指标（延迟取值）
	descs       map[string]string     // 指标 HELP 描述（Prometheus 规范推荐）
	prefix      string                // 非空表示该表是某父表的子系统，所有名字加此前缀
}

// NewRegistry 创建一个空的指标注册表。
func NewRegistry() *Registry {
	return &Registry{
		mu:          &sync.Mutex{},
		counters:    map[string]*Counter{},
		counterVecs: map[string]*CounterVec{},
		histograms:  map[string]*Histogram{},
		gauges:      map[string]*Gauge{},
		funcGauges:  map[string]*FuncGauge{},
		descs:       map[string]string{},
	}
}

// Subsystem 返回以 name 为前缀的子注册表，与父共享底层存储与锁。
// 在子表上注册的指标名自动加 "name_" 前缀，导出（Snapshot/WritePrometheus）时
// 以父表为真相源——便于按组件（raft / shardkv / gateway）分组命名空间，避免跨组件
// 同名指标冲突，且保持单一注册表便于一次性 scrape。子表可再嵌套（前缀累加）。
// 注意：子表与父表共享同一组 map，Reset() 对子表仅清除其前缀下的指标，不影响父表其余指标。
func (r *Registry) Subsystem(name string) *Registry {
	return &Registry{
		mu:          r.mu,
		counters:    r.counters,
		counterVecs: r.counterVecs,
		histograms:  r.histograms,
		gauges:      r.gauges,
		funcGauges:  r.funcGauges,
		descs:       r.descs,
		prefix:      r.prefix + name + "_",
	}
}

// name 返回带本表前缀的指标名（子系统下自动加前缀，根表原样返回）。
func (r *Registry) name(raw string) string {
	return r.prefix + raw
}

// Counter 取得（不存在则创建）命名计数器。
func (r *Registry) Counter(name string) *Counter {
	name = r.name(name)
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
	name = r.name(name)
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
	name = r.name(name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.gauges[name]; ok {
		return g
	}
	g := &Gauge{}
	r.gauges[name] = g
	return g
}

// CounterWithHelp 取得命名计数器并登记 HELP 描述（供 WritePrometheus 输出 # HELP）。
func (r *Registry) CounterWithHelp(name, help string) *Counter {
	c := r.Counter(name)
	r.setDesc(name, help)
	return c
}

// CounterVec 取得（不存在则创建）命名 counter 向量（带标签维度，cycle #117）。
// labelNames 为标签维度名（如 "code","method"），首次创建时确定；适合「同一指标按
// 状态码/方法等维度切片」的场景，如 http_responses_total{code,method}。
func (r *Registry) CounterVec(name string, labelNames ...string) *CounterVec {
	name = r.name(name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if v, ok := r.counterVecs[name]; ok {
		return v
	}
	v := NewCounterVec(labelNames...)
	r.counterVecs[name] = v
	return v
}

// CounterVecWithHelp 取得命名 counter 向量并登记 HELP 描述。
func (r *Registry) CounterVecWithHelp(name, help string, labelNames ...string) *CounterVec {
	v := r.CounterVec(name, labelNames...)
	r.setDesc(name, help)
	return v
}

// GaugeWithHelp 取得命名瞬时值并登记 HELP 描述。
func (r *Registry) GaugeWithHelp(name, help string) *Gauge {
	g := r.Gauge(name)
	r.setDesc(name, help)
	return g
}

// HistWithHelp 取得命名直方图并登记 HELP 描述。
func (r *Registry) HistWithHelp(name, help string) *Histogram {
	h := r.Histogram(name)
	r.setDesc(name, help)
	return h
}

// FuncGauge 是延迟取值的瞬时指标：持有取值函数，每次快照/导出时调用，反映实时状态
// （如运行时 goroutine 数、内存占用、连接池使用中数量等无法靠事件 Set 的瞬时量）。
// 与 Gauge（手动 Set）互补：FuncGauge 的数据源是外部状态，自动刷新。
type FuncGauge struct {
	mu   sync.Mutex
	fn   func() float64
	desc string
}

// NewFuncGauge 创建一个函数式 gauge；fn 为 nil 时 Value 恒返回 0。
func NewFuncGauge(fn func() float64) *FuncGauge {
	return &FuncGauge{fn: fn}
}

// Value 调用取值函数返回当前瞬时值（线程安全）。
func (g *FuncGauge) Value() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fn == nil {
		return 0
	}
	return g.fn()
}

// SetDesc 设置 HELP 描述（供独立导出或 Registry 同步）。
func (g *FuncGauge) SetDesc(desc string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.desc = desc
}

// Desc 返回当前描述。
func (g *FuncGauge) Desc() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.desc
}

// FuncGauge 取得（不存在则创建）命名函数式瞬时指标，注册取值函数。
func (r *Registry) FuncGauge(name string, fn func() float64) *FuncGauge {
	name = r.name(name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.funcGauges[name]; ok {
		g.fn = fn
		return g
	}
	g := NewFuncGauge(fn)
	r.funcGauges[name] = g
	return g
}

// FuncGaugeWithHelp 取得函数式 gauge 并登记 HELP 描述。
func (r *Registry) FuncGaugeWithHelp(name, help string, fn func() float64) *FuncGauge {
	g := r.FuncGauge(name, fn)
	g.SetDesc(help)
	r.setDesc(name, help)
	return g
}

// setDesc 登记指标的 HELP 描述（多行归一成单行，避免破坏 exposition）。
// 与注册方法一致加本表前缀，保证描述键与指标序列名（可能带 subsystem 前缀）对齐。
func (r *Registry) setDesc(name, help string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.descs[r.name(name)] = strings.ReplaceAll(help, "\n", " ")
}

// desc 返回指标描述（无则空串）。调用方须在 Snapshot 释放锁后调用。
func (r *Registry) desc(name string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.descs[name]
}

// helpLine 返回指标的 # HELP 行（含换行）；无描述时返回空串。
func (r *Registry) helpLine(name, sn string) string {
	if d := r.desc(name); d != "" {
		return "# HELP " + sn + " " + d + "\n"
	}
	return ""
}

// Snapshot 返回 JSON 友好结构：{"counters": {...}, "histograms": {...}}。
// 子系统表仅返回其前缀下的指标（键保留前缀，作为完整序列名）。
func (r *Registry) Snapshot() map[string]interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	counters := make(map[string]int64, len(r.counters))
	for k, v := range r.counters {
		if r.prefix == "" || strings.HasPrefix(k, r.prefix) {
			counters[k] = v.Value()
		}
	}
	hists := make(map[string]HistSnapshot, len(r.histograms))
	for k, v := range r.histograms {
		if r.prefix == "" || strings.HasPrefix(k, r.prefix) {
			hists[k] = v.Snapshot()
		}
	}
	cvecs := make(map[string]map[string]int64, len(r.counterVecs))
	for k, v := range r.counterVecs {
		if r.prefix == "" || strings.HasPrefix(k, r.prefix) {
			cvecs[k] = v.Snapshot()
		}
	}
	gauges := make(map[string]float64, len(r.gauges))
	for k, v := range r.gauges {
		if r.prefix == "" || strings.HasPrefix(k, r.prefix) {
			gauges[k] = v.Value()
		}
	}
	for k, v := range r.funcGauges {
		if r.prefix == "" || strings.HasPrefix(k, r.prefix) {
			gauges[k] = v.Value()
		}
	}
	return map[string]interface{}{
		"counters":    counters,
		"counterVecs": cvecs,
		"histograms":  hists,
		"gauges":      gauges,
	}
}

// Reset 清空计数器与直方图。根表清空全部；子系统表仅清除其前缀下的指标，
// 不影响父表其余指标（因共享同一组 map，逐键删除而非重建 map）。
func (r *Registry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.prefix == "" {
		r.counters = map[string]*Counter{}
		r.counterVecs = map[string]*CounterVec{}
		r.histograms = map[string]*Histogram{}
		r.gauges = map[string]*Gauge{}
		r.funcGauges = map[string]*FuncGauge{}
		r.descs = map[string]string{}
		return
	}
	for k := range r.counters {
		if strings.HasPrefix(k, r.prefix) {
			delete(r.counters, k)
		}
	}
	for k := range r.histograms {
		if strings.HasPrefix(k, r.prefix) {
			delete(r.histograms, k)
		}
	}
	for k := range r.gauges {
		if strings.HasPrefix(k, r.prefix) {
			delete(r.gauges, k)
		}
	}
	for k := range r.funcGauges {
		if strings.HasPrefix(k, r.prefix) {
			delete(r.funcGauges, k)
		}
	}
	for k := range r.descs {
		if strings.HasPrefix(k, r.prefix) {
			delete(r.descs, k)
		}
	}
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
			if _, err := io.WriteString(w, r.helpLine(name, sn)); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "# TYPE %s counter\n%s %d\n", sn, sn, v); err != nil {
				return err
			}
			continue
		}
		if v, ok := gauges[name]; ok {
			if _, err := io.WriteString(w, r.helpLine(name, sn)); err != nil {
				return err
			}
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
		if _, err := io.WriteString(w, r.helpLine(name, sn)); err != nil {
			return err
		}
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

	// 带标签维度的 counter 向量（cycle #117）：每个向量按 label 组合输出多条序列。
	r.mu.Lock()
	cvecNames := make([]string, 0, len(r.counterVecs))
	for k := range r.counterVecs {
		cvecNames = append(cvecNames, k)
	}
	r.mu.Unlock()
	sort.Strings(cvecNames)
	for _, name := range cvecNames {
		sn := sanitizeMetricName(name)
		if _, err := io.WriteString(w, r.helpLine(name, sn)); err != nil {
			return err
		}
		if err := r.counterVecs[name].WritePrometheus(w, name, ""); err != nil {
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
