package main

import (
	"context"
	"sync"
)

// Exists 并发探测多个 key 是否存在（GET 命中即存在，404/错误视作不存在）。
// 返回 key→bool 映射，便于调用方做"哪些需要回源、哪些已存在"的预判。
// 各 key 独立探测，互不阻断；空输入安全返回空映射。注意：网络错误也会被判为
// 不存在（无法确认），调用方对强一致存在性要求应走 Get 自行裁决。
func (c *Client) Exists(keys []string) map[string]bool {
	out := make(map[string]bool, len(keys))
	if len(keys) == 0 {
		return out
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, k := range keys {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			_, err := c.getCtx(context.Background(), k)
			mu.Lock()
			out[k] = err == nil
			mu.Unlock()
		}(k)
	}
	wg.Wait()
	return out
}
