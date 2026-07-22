package main

// AppendGet 先追加 value 到 key 当前值之后，再读取并返回追加后的完整新值。
// 等价于「Append + Get」的便捷封装，常用于日志追加后立刻确认落库结果、
// 或构建"追加并返回最新值"的原子感语义（尽管底层仍是两步，单写者下一致）。
// 任一阶段出错都会向上返回。
func (c *Client) AppendGet(key, value string) (string, error) {
	if err := c.Append(key, value); err != nil {
		return "", err
	}
	return c.Get(key)
}
