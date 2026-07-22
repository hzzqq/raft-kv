package shardkv

// IsReadOp 判断 op 是否为只读操作（Get）。只读 op 不修改状态机、天然幂等，
// 可被网关在多副本间重试、可在迁移期间走 stale-read 兜底。
func IsReadOp(op Op) bool {
	return op.OpType == "Get"
}

// IsWriteOp 判断 op 是否为写入操作（Put / Append）。写入 op 会改变分片状态，
// 需经 Raft 复制并受制于幂等去重（ClientId+Seq）。
func IsWriteOp(op Op) bool {
	return op.OpType == "Put" || op.OpType == "Append"
}

// OpKind 返回 op 的可读分类："read" / "write" / "unknown"。便于日志、指标打标。
func OpKind(op Op) string {
	switch {
	case IsReadOp(op):
		return "read"
	case IsWriteOp(op):
		return "write"
	default:
		return "unknown"
	}
}
