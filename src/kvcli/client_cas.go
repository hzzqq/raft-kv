package main

// Cas 执行比较并交换：读取 key 当前值，若等于 expect 则写入 newVal 并返回
// (true, nil)；否则不写入、返回 (false, nil)。key 不存在时视作当前值 ""（故
// expect="" 的 Cas 等价于"仅在不存在时设置"，但语义更明确者应改用 SetNX）。
// 底层 Get/Put 经网关 Clerk 幂等去重，重试安全；非 404 的读取错误会直接向上返回。
// 注意：本实现是"读-改-写"两步，非服务端原子 CAS——在极高并发争用下可能
// check-and-set 之间被他人改写；对强一致单写者场景（如 leader 租约内的配置写入）
// 足够，分布式强一致 CAS 应由 Raft 状态机内的 Op 保证。
func (c *Client) Cas(key, expect, newVal string) (bool, error) {
	cur, err := c.Get(key)
	if err != nil {
		// 读取错误（含 404 视为不存在）→ 以 "" 参与比较，但保留真实网络错误。
		cur = ""
	}
	if cur != expect {
		return false, nil
	}
	if err := c.Put(key, newVal); err != nil {
		return false, err
	}
	return true, nil
}
