package shardkv

import "fmt"

// DedupKey 返回 op 的客户端去重键（"ClientId:Seq"）。同一客户端的请求按 (ClientId, Seq)
// 唯一标识，apply 后据此判重——重复到达的同一请求直接复用首次结果，不做第二次执行。
// 抽成纯函数便于 Clerk 去重表、测试断言、以及跨包（gateway 侧预去重）复用同一键格式，
// 避免「两边各写一套拼接逻辑、分隔符不一致」导致的去重失效。
func DedupKey(op Op) string {
	return fmt.Sprintf("%d:%d", op.ClientId, op.Seq)
}
