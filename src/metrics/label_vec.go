// label_vec.go —— 带标签向量（GaugeVec/CounterVec）共享的免锁读缓存存储（cycle #119）。
//
// 设计动机：网关每请求调用 WithLabelValues 解析标签序列，旧实现每次都取独占锁并
// 分配拼接 key，在多核高并发下成为热点（锁竞争 + 每调用 1 次分配）。但子序列一旦
// 创建，其指针在整个生命周期内稳定——因此读路径可走「免锁快照」：
//   - 命中：atomic.Load 只读快照 + map 查找，无锁、无分配；
//   - 未命中（新标签组合，极少）：加锁创建并重建快照。
// GaugeVec 与 CounterVec 通过内嵌 *labelVec 复用同一份实现，避免两份易漂移的并发代码。
package metrics

import (
	"strings"
	"sync"
	"sync/atomic"
)

// labelVec 以 label 组合为 key 维护一组子序列指针，并提供读路径免锁解析。
type labelVec struct {
	mu    sync.Mutex
	m     map[string]any      // 真实存储，写路径与遍历受 mu 保护
	cache atomic.Value        // 只读快照 map[string]any，读路径免锁
}

func newLabelVec() *labelVec {
	lv := &labelVec{m: make(map[string]any)}
	lv.cache.Store(map[string]any{})
	return lv
}

// load 读路径：从免锁快照取子序列；未命中返回 (nil, false)。
func (lv *labelVec) load(key string) (any, bool) {
	if m, _ := lv.cache.Load().(map[string]any); m != nil {
		if v, ok := m[key]; ok {
			return v, true
		}
	}
	return nil, false
}

// getOrCreate 写路径：未命中才加锁创建并重建只读快照（double-check 防重复创建）。
// makeFn 构造子序列（*Gauge 或 *Counter）；返回既有或新建的指针。
func (lv *labelVec) getOrCreate(key string, makeFn func() any) any {
	if v, ok := lv.load(key); ok {
		return v
	}
	lv.mu.Lock()
	defer lv.mu.Unlock()
	if v, ok := lv.m[key]; ok {
		return v
	}
	v := makeFn()
	lv.m[key] = v
	// 重建只读快照：拷贝当前全部子序列，O(标签组合数)，仅在新增标签时发生。
	nm := make(map[string]any, len(lv.m))
	for k, val := range lv.m {
		nm[k] = val
	}
	lv.cache.Store(nm)
	return v
}

// forEach 在写锁内遍历所有已注册 key（供 Snapshot/Keys/WritePrometheus）。
func (lv *labelVec) forEach(fn func(key string, val any)) {
	lv.mu.Lock()
	defer lv.mu.Unlock()
	for k, v := range lv.m {
		fn(k, v)
	}
}

// joinKey 把标签值拼成稳定 key（控制字符 \x1f 分隔，避免业务值含常见分隔符冲突）。
func joinKey(vals []string) string { return strings.Join(vals, "\x1f") }
