// singleflight.go —— 并发同键合并（singleflight）
//
// 用于保护热点 key 的回源：当 N 个 goroutine 同时请求同一个 key 时，
// 只有第一个真正执行 fn，其余直接复用其结果，避免把压力打到下游（缓存击穿）。
package util

import "sync"

// call 表示一次进行中的调用及其结果广播。
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group 实现单飞：对同一个 key，并发的多个 Do 只有一个会执行 fn，
// 其余等待并复用其结果。Group 本身并发安全。
type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

// NewGroup 构造一个单飞组。
func NewGroup() *Group {
	return &Group{m: make(map[string]*call)}
}

// Do 执行 fn 并返回其结果。返回值：
//
//	val, err —— fn 的执行结果（被复用或本 goroutine 实际执行）；
//	called  —— 本次是否由本 goroutine 真正执行了 fn（true=回源，false=复用）。
//
// 并发语义：若同一 key 已有进行中的调用，本调用会等待其完成并复用结果，
// 不会重复执行 fn。新到达的、在旧调用结束后才进入的同类请求会开启新一波。
func (g *Group) Do(key string, fn func() (interface{}, error)) (val interface{}, err error, called bool) {
	g.mu.Lock()
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait() // 等待进行中的调用完成，复用其结果
		return c.val, c.err, false
	}
	c := &call{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	// 本 goroutine 是第一个拿到该 key 的，真正执行 fn。
	c.val, c.err = fn()

	g.mu.Lock()
	delete(g.m, key) // 释放槽位，允许下一波同 key 请求
	g.mu.Unlock()
	c.wg.Done() // 唤醒所有等待复用结果的 goroutine

	return c.val, c.err, true
}
