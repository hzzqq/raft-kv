package metrics

import (
	"fmt"
	"io"
	"strings"
	"sync"
)

// GaugeVec 带标签维度的 gauge 向量：同一指标名按 label 组合（如 method="GET"/"PUT"）
// 拆为多个子序列，便于按维度切片观测。WithLabelValues 取得（不存在则创建）对应子 gauge。
// 与 Registry 解耦，可独立使用或自行导出；WritePrometheus 输出带标签的序列。
type GaugeVec struct {
	mu         sync.Mutex
	labelNames []string
	gauges     map[string]*Gauge
}

// NewGaugeVec 创建带指定标签名的 gauge 向量。
func NewGaugeVec(labelNames ...string) *GaugeVec {
	return &GaugeVec{
		labelNames: labelNames,
		gauges:     make(map[string]*Gauge),
	}
}

// LabelNames 返回标签名列表。
func (v *GaugeVec) LabelNames() []string { return v.labelNames }

// WithLabelValues 取得（不存在则创建）给定标签值对应的子 gauge。
// 标签值数量须与 LabelNames 一致；不一致时退化为用全部值拼接作 key（不报错，便于容错调用）。
func (v *GaugeVec) WithLabelValues(vals ...string) *Gauge {
	key := strings.Join(vals, "\x1f")
	v.mu.Lock()
	defer v.mu.Unlock()
	if g, ok := v.gauges[key]; ok {
		return g
	}
	g := &Gauge{}
	v.gauges[key] = g
	return g
}

// Snapshot 返回 标签组合key -> 当前值 的快照。
func (v *GaugeVec) Snapshot() map[string]float64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make(map[string]float64, len(v.gauges))
	for k, g := range v.gauges {
		out[k] = g.Value()
	}
	return out
}

// Keys 返回所有已注册的标签组合 key。
func (v *GaugeVec) Keys() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	keys := make([]string, 0, len(v.gauges))
	for k := range v.gauges {
		keys = append(keys, k)
	}
	return keys
}

// WritePrometheus 把向量以 Prometheus 文本格式写入 w：序列名经 sanitizeMetricName 清洗，
// 每个标签值作为 {label="value"} 后缀附加。name 由调用方传入（向量自身不含名字，便于复用形态）。
// 调用方须持锁外部已确保无并发写（内部自行加锁遍历子 gauge）。
func (v *GaugeVec) WritePrometheus(w io.Writer, name, help string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	sn := sanitizeMetricName(name)
	if help != "" {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n", sn, help); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "# TYPE %s gauge\n", sn); err != nil {
		return err
	}
	for key, g := range v.gauges {
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
		if _, err := fmt.Fprintf(w, "%s %g\n", sb.String(), g.Value()); err != nil {
			return err
		}
	}
	return nil
}
