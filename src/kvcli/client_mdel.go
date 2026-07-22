package main

import "sync"

// MDelResult 批量删除结果：Deleted 为成功删除数；Errors 为失败 key→error；Total 为总数。
type MDelResult struct {
	Deleted int
	Errors  map[string]error
	Total   int
}

// MDel 并发批量删除多个 key（见 Del 的单键语义，删除幂等）。各 key 独立成功/失败，
// 互不阻断：成功的计入 Deleted，失败的进入 Errors。空输入安全返回空结果。
// 复用单键 Del 的回源/重试链路与连接复用（keep-alive）。
func (c *Client) MDel(keys []string) MDelResult {
	res := MDelResult{
		Errors: make(map[string]error, 0),
		Total:  len(keys),
	}
	if len(keys) == 0 {
		return res
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, k := range keys {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			err := c.Del(k)
			mu.Lock()
			if err != nil {
				res.Errors[k] = err
			} else {
				res.Deleted++
			}
			mu.Unlock()
		}(k)
	}
	wg.Wait()
	return res
}
