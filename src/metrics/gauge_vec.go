// gauge_vec.go —— 带标签维度的 gauge 向量（cycle #117，热路径免锁优化见 #119）。
//
// 与 Registry 解耦，可独立使用或自行导出；WritePrometheus 输出带标签的序列。
// 底层存储复用 labelVec（#119）：子序列一旦创建指针稳定，读路径免锁、免分配，
// 解决每请求 WithLabelValues 的锁竞争与分配开销。
package metrics

import (
	"fmt"
	"io"
	"strings"
)

// GaugeVec 带标签维度的 gauge 向量：同一指标名按 label 组合（如 method="GET"/"PUT"）
// 拆为多个子序列，便于按维度切片观测。WithLabelValues 取得（不存在则创建）对应子 gauge。
type GaugeVec struct {
	labelNames []string
	store      *labelVec
}

// NewGaugeVec 创建带指定标签名的 gauge 向量。
func NewGaugeVec(labelNames ...string) *GaugeVec {
	return &GaugeVec{labelNames: labelNames, store: newLabelVec()}
}

// LabelNames 返回标签名列表。
func (v *GaugeVec) LabelNames() []string { return v.labelNames }

// WithLabelValues 取得（不存在则创建）给定标签值对应的子 gauge。
// 标签值数量须与 LabelNames 一致；不一致时退化为用全部值拼接作 key（不报错，便于容错调用）。
// 命中读路径免锁、免分配（见 #119 labelVec）。
func (v *GaugeVec) WithLabelValues(vals ...string) *Gauge {
	return v.store.getOrCreate(joinKey(vals), func() any { return &Gauge{} }).(*Gauge)
}

// Snapshot 返回 标签组合key -> 当前值 的快照。
func (v *GaugeVec) Snapshot() map[string]float64 {
	out := make(map[string]float64)
	v.store.forEach(func(key string, val any) {
		out[key] = val.(*Gauge).Value()
	})
	return out
}

// Keys 返回所有已注册的标签组合 key。
func (v *GaugeVec) Keys() []string {
	keys := make([]string, 0)
	v.store.forEach(func(key string, val any) {
		keys = append(keys, key)
	})
	return keys
}

// WritePrometheus 把向量以 Prometheus 文本格式写入 w：序列名经 sanitizeMetricName 清洗，
// 每个标签值作为 {label="value"} 后缀附加。name 由调用方传入（向量自身不含名字，便于复用形态）。
func (v *GaugeVec) WritePrometheus(w io.Writer, name, help string) error {
	sn := sanitizeMetricName(name)
	if help != "" {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n", sn, help); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "# TYPE %s gauge\n", sn); err != nil {
		return err
	}
	var sb strings.Builder
	v.store.forEach(func(key string, val any) {
		labels := strings.Split(key, "\x1f")
		sb.Reset()
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
		_, _ = fmt.Fprintf(w, "%s %g\n", sb.String(), val.(*Gauge).Value())
	})
	return nil
}
