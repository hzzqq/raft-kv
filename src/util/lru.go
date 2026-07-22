// lru.go —— 有界最近最少使用（LRU）缓存（零依赖，纯结构 + 方法）。
//
// 容量达上限时，插入新键会淘汰最久未被访问（Get/Put）的条目。用于需要限制内存、又
// 希望热点数据常驻的场景（如 kvcli 缓存、网关路由表、连接池等）。
//
// 并发不安全——调用方需自加锁（保持轻量、零额外依赖）。若需并发，用 sync.Mutex 包裹。
package util

import "container/list"

// lruEntry 是链表节点承载的键值。
type lruEntry struct {
	key string
	val interface{}
}

// LRU 是有界最近最少使用缓存。
type LRU struct {
	cap   int
	ll    *list.List
	items map[string]*list.Element
}

// NewLRU 创建容量为 cap 的 LRU 缓存（cap<1 时按 1 处理）。
func NewLRU(cap int) *LRU {
	if cap < 1 {
		cap = 1
	}
	return &LRU{
		cap:   cap,
		ll:    list.New(),
		items: make(map[string]*list.Element),
	}
}

// Get 返回 key 的值并将其标记为最近使用（移到表头）。不存在返回 (nil, false)。
func (c *LRU) Get(key string) (interface{}, bool) {
	if e, ok := c.items[key]; ok {
		c.ll.MoveToFront(e)
		return e.Value.(*lruEntry).val, true
	}
	return nil, false
}

// Put 写入 key=val。若已存在则更新值并标记为最近使用；否则插入表头，容量超限时淘汰
// 表尾（最久未使用）条目。
func (c *LRU) Put(key string, val interface{}) {
	if e, ok := c.items[key]; ok {
		e.Value.(*lruEntry).val = val
		c.ll.MoveToFront(e)
		return
	}
	if c.ll.Len() >= c.cap {
		if old := c.ll.Back(); old != nil {
			delete(c.items, old.Value.(*lruEntry).key)
			c.ll.Remove(old)
		}
	}
	e := c.ll.PushFront(&lruEntry{key: key, val: val})
	c.items[key] = e
}

// Delete 删除 key（不存在则无操作）。
func (c *LRU) Delete(key string) {
	if e, ok := c.items[key]; ok {
		c.ll.Remove(e)
		delete(c.items, key)
	}
}

// Len 返回当前条目数。
func (c *LRU) Len() int { return c.ll.Len() }

// Cap 返回容量上限。
func (c *LRU) Cap() int { return c.cap }
