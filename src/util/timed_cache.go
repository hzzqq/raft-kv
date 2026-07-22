package util

import (
	"sync"
	"time"
)

// timedEntry 是缓存条目：value 与过期时刻（零值表示永不过期）。
type timedEntry struct {
	value    interface{}
	expireAt time.Time
}

// TimedCache 是带 TTL 的并发安全缓存（零依赖）。
// 适合短期凭据、会话、热点查询结果等需自动过期的场景：
//   - Set 时指定 ttl，过期后 Get 自动返回 miss 并顺手清除；
//   - GC 主动批量清除已过期条目，避免过期项长期占位。
type TimedCache struct {
	mu    sync.RWMutex
	items map[string]timedEntry
	now   func() time.Time // 可注入时钟，便于白盒测试
}

// NewTimedCache 创建空缓存。
func NewTimedCache() *TimedCache {
	return &TimedCache{
		items: make(map[string]timedEntry),
		now:   time.Now,
	}
}

// Get 返回 key 的值；若 key 不存在或已过期返回 (nil, false)（过期条目同时被清除）。
func (c *TimedCache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok {
		return nil, false
	}
	if !e.expireAt.IsZero() && c.now().After(e.expireAt) {
		delete(c.items, key)
		return nil, false
	}
	return e.value, true
}

// Set 写入 key=value；ttl<=0 表示永不过期。
func (c *TimedCache) Set(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var exp time.Time
	if ttl > 0 {
		exp = c.now().Add(ttl)
	}
	c.items[key] = timedEntry{value: value, expireAt: exp}
}

// Delete 删除 key（不存在则无操作）。
func (c *TimedCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

// Len 返回条目数（含可能已过期但尚未清除的）。
func (c *TimedCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// GC 清除所有已过期条目，返回被清除的数量。
func (c *TimedCache) GC() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	n := 0
	for k, e := range c.items {
		if !e.expireAt.IsZero() && now.After(e.expireAt) {
			delete(c.items, k)
			n++
		}
	}
	return n
}
