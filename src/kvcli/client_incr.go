package main

import "strconv"

// Incr 对 key 做原子自增：读取当前值（缺省为 0，非整数按 0 处理），+1 后写回，
// 返回新值。用于计数器、序列号生成。底层 Get/Put 经网关 Clerk 幂等去重，重试安全。
// 与 Cas 同理：这是客户端两步"读-改-写"，非服务端原子；单写者/低争用计数器足够，
// 高并发强一致计数应由状态机内 Op 保证。写入错误向上返回。
func (c *Client) Incr(key string) (int, error) {
	cur, err := c.Get(key)
	n := 0
	if err == nil && cur != "" {
		if v, perr := strconv.Atoi(cur); perr == nil {
			n = v
		}
	}
	n++
	if err := c.Put(key, strconv.Itoa(n)); err != nil {
		return 0, err
	}
	return n, nil
}
