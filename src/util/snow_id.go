package util

import (
	"errors"
	"sync"
	"time"
)

const (
	snowNodeBits  = 10
	snowSeqBits   = 12
	snowNodeMax   = (1 << snowNodeBits) - 1 // 1023
	snowSeqMax    = (1 << snowSeqBits) - 1  // 4095
	snowNodeShift = snowSeqBits
	snowTimeShift = snowNodeBits + snowSeqBits
)

// SnowID 是 Twitter Snowflake 风格的唯一 ID 生成器（零依赖）。
// 64 位布局：41 位毫秒时间戳 | 10 位节点 | 12 位序列，单机每毫秒最多 4096 个、集群 1024 节点。
// 用于需要全局唯一且趋势递增的标识（日志追踪号、请求 ID、分片任务号）。
type SnowID struct {
	mu     sync.Mutex
	node   int64
	epoch  int64
	lastTs int64
	seq    int64
	now    func() time.Time // 可注入时钟，便于白盒测试
}

// NewSnowID 创建节点 node(0..1023) 的 ID 生成器；node 越界返回 error。
func NewSnowID(node int64) (*SnowID, error) {
	if node < 0 || node > snowNodeMax {
		return nil, errors.New("util: snow node out of range [0,1023]")
	}
	return &SnowID{
		node:  node,
		epoch: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
		now:   time.Now,
	}, nil
}

// NextID 生成下一个唯一 ID：同毫秒内序列递增；跨毫秒序列归零；
// 同毫秒序列耗尽或时钟回拨时返回 error（不做忙等，避免测试/高负载下挂死）。
func (s *SnowID) NextID() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts := s.now().UnixMilli() - s.epoch
	if ts < s.lastTs {
		return 0, errors.New("util: snow clock moved backwards")
	}
	if ts == s.lastTs {
		s.seq = (s.seq + 1) & snowSeqMax
		if s.seq == 0 {
			return 0, errors.New("util: snow sequence overflow in current ms")
		}
	} else {
		s.seq = 0
	}
	s.lastTs = ts
	return (ts << snowTimeShift) | (s.node << snowNodeShift) | s.seq, nil
}

// Component 拆出 ID 的 timestamp/node/seq 三部分，便于排查与日志关联。
func (s *SnowID) Component(id int64) (ts time.Time, node int64, seq int64) {
	seq = id & snowSeqMax
	node = (id >> snowNodeShift) & snowNodeMax
	tsMs := (id >> snowTimeShift) + s.epoch
	return time.UnixMilli(tsMs), node, seq
}
