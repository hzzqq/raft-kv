package shardkv

// IsRetryableErr 判断一个业务错误是否可由客户端安全重试（无需换 key / 换配置）。
//   - ErrWrongLeader：当前副本非 leader，稍后重试大概率落到新 leader，瞬态；
//   - ErrTimeout：请求超时，可能网络抖动或 leader 切换，重试安全（Clerk 幂等去重兜底）。
//
// 不可重试的情形（返回 false）：ErrWrongGroup（分片不归本组，需等待配置就绪、
// 重试只会持续打错组）、OK（无错误）、以及其它未知错误（缺省保守不重试，
// 避免对未预期错误盲目重试放大故障）。
func IsRetryableErr(e Err) bool {
	switch e {
	case ErrWrongLeader, ErrTimeout:
		return true
	default:
		return false
	}
}
