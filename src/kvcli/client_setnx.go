package main

// SetNX 仅在 key 不存在（或当前为空串）时写入 val，返回 (true, nil)；
// 若 key 已存在且非空则返回 (false, nil)，不做覆盖。用于分布式锁初始化、
// 幂等首写、去重标记等"抢占式创建"场景。底层 Get/Put 经网关 Clerk 幂等去重，
// 重试安全；仅网关明确答复 404（key 不存在）才视作可写入，其余读取错误
// （网络故障/熔断/5xx 重试耗尽）fail-closed 向上返回——否则读故障会被当成
// "key 不存在"，让多个调用方在网络抖动时同时"抢占成功"，破坏锁初始化的
// 互斥语义。与 Cas(key,"",val) 等价但语义更清晰。
func (c *Client) SetNX(key, val string) (bool, error) {
	cur, err := c.Get(key)
	if err != nil && !IsNotFound(err) {
		return false, err
	}
	if err == nil && cur != "" {
		return false, nil // 已存在且非空，放弃设置
	}
	if err := c.Put(key, val); err != nil {
		return false, err
	}
	return true, nil
}
