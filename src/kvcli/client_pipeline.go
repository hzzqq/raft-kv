package main

import (
	"fmt"
	"sync"
)

// BatchOp 是 Pipeline 的单个操作描述。Kind 为 "get" / "put" / "append"；
// Key 必填；put/append 时 Value 为写入/追加内容；get 时 Value 忽略。
type BatchOp struct {
	Kind  string
	Key   string
	Value string
}

// BatchResult 是 Pipeline 中单个操作的执行结果：get 的 Value 为读到的值，
// put/append 的 Value 为空；Err 非 nil 表示该操作失败。按输入顺序一一对应。
type BatchResult struct {
	Value string
	Err   error
}

// Pipeline 并发执行一组混合类型的操作（get/put/append），各操作互不依赖、互不阻断，
// 按输入顺序返回一一对应的结果。适合"一次组装多类操作、降低往返编排复杂度"的场景
// （如初始化一批 key、批量读取后再批量追加）。空输入安全返回空切片。
func (c *Client) Pipeline(ops []BatchOp) []BatchResult {
	res := make([]BatchResult, len(ops))
	if len(ops) == 0 {
		return res
	}
	var wg sync.WaitGroup
	for i, op := range ops {
		wg.Add(1)
		go func(i int, op BatchOp) {
			defer wg.Done()
			switch op.Kind {
			case "get":
				v, e := c.Get(op.Key)
				res[i] = BatchResult{Value: v, Err: e}
			case "put":
				res[i] = BatchResult{Err: c.Put(op.Key, op.Value)}
			case "append":
				res[i] = BatchResult{Err: c.Append(op.Key, op.Value)}
			default:
				res[i] = BatchResult{Err: fmt.Errorf("unknown batch op kind %q", op.Kind)}
			}
		}(i, op)
	}
	wg.Wait()
	return res
}
