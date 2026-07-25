// counter_vec.go —— 带标签维度的 counter 向量（cycle #117）。
//
// 与 GaugeVec 对称：同一指标名按 label 组合（如 method="GET"/code="200"）拆为多个
// 子序列，便于按维度切片观测（错误率、按方法拆分 QPS 等）。WithLabelValues 取得
// （不存在则创建）对应子 counter。与 Registry 解耦，可独立使用或自行导出；
// WritePrometheus 输出带标签的序列。
//
// 设计要点：每个子序列一旦创建，其 *Counter 指针在整个生命周期内保持稳定，
// 因此调用方可在热路径上缓存 WithLabelValues 的结果避免重复查表（见 #119 优化）。
package metrics

import (
	"fmt"
	"io"
	"strings"
	"sync"
)

// CounterVec 带标签维度的 counter 向量。
type CounterVec struct {
	mu         sync.Mutex
	labelNames []string
	counters   map[string]*Counter
}

// NewCounterVec 创建带指定标签名的 counter 向量。
func NewCounterVec(labelNames ...string) *CounterVec {
	return &CounterVec{
		labelNames: labelNames,
		counters:   make(map[string]*Counter),
	}
}

// LabelNames 返回标签名列表。
func (v *CounterVec) LabelNames() []string { return v.labelNames }

// WithLabelValues 取得（不存在则创建）给定标签值对应的子 counter。
// 标签值数量须与 LabelNames 一致；不一致时退化为用全部值拼接作 key（不报错，便于容错调用）。
func (v *CounterVec) WithLabelValues(vals ...string) *Counter {
	key := strings.Join(vals, "\x1f")
	v.mu.Lock()
	defer v.mu.Unlock()
	if c, ok := v.counters[key]; ok {
		return c
	}
	c := &Counter{}
	v.counters[key] = c
	return c
}

// Snapshot 返回 标签组合key -> 当前值 的快照。
func (v *CounterVec) Snapshot() map[string]int64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make(map[string]int64, len(v.counters))
	for k, c := range v.counters {
		out[k] = c.Value()
	}
	return out
}

// Keys 返回所有已注册的标签组合 key。
func (v *CounterVec) Keys() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	keys := make([]string, 0, len(v.counters))
	for k := range v.counters {
		keys = append(keys, k)
	}
	return keys
}

// WritePrometheus 把向量以 Prometheus 文本格式写入 w：序列名经 sanitizeMetricName 清洗，
// 每个标签值作为 {label="value"} 后缀附加。name 由调用方传入（向量自身不含名字，便于复用形态）。
func (v *CounterVec) WritePrometheus(w io.Writer, name, help string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	sn := sanitizeMetricName(name)
	if help != "" {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n", sn, help); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "# TYPE %s counter\n", sn); err != nil {
		return err
	}
	for key, c := range v.counters {
		labels := strings.Split(key, "\x1f")
		var sb strings.Builder
		sb.WriteString(sn)
		if len(labels) > 0 && len(labels) == len(v.labelNames) {
			sb.WriteString("{")
			for i, ln := range v.labelNames {
				if i > 0 {
					sb.WriteString(",")
				}
				sb.WriteString(fmt.Sprintf("%s=\"%s\"", ln, labels[i]))
			}
			sb.WriteString("}")
		}
		if _, err := fmt.Fprintf(w, "%s %d\n", sb.String(), c.Value()); err != nil {
			return err
		}
	}
	return nil
}
