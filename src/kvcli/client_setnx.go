package main

// SetNX 仅在 key 不存在（或当前为空串）时写入 val，返回 (true, nil)；
// 若 key 已存在且非空则返回 (false, nil)，不做覆盖。用于分布式锁初始化、
// 幂等首写、去重标记等"抢占式创建"场景。底层 Get/Put 经网关 Clerk 幂等去重，
// 重试安全；读取错误（非 404）会向上返回。与 Cas(key,"",val) 等价但语义更清晰。
func (c *Client) SetNX(key, val string) (bool, error) {
	cur, err := c.Get(key)
	if err == nil && cur != "" {
		return false, nil // 已存在且非空，放弃设置
	}
	if err := c.Put(key, val); err != nil {
		return false, err
	}
	return true, nil
}
