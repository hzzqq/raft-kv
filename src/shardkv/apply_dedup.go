package shardkv

// OpIdentity 返回 op 的客户端去重身份串（ClientId:Seq，经 DedupKey），
// 供日志/排障把同一条命令与其重试关联起来。
func OpIdentity(op Op) string { return DedupKey(op) }

// ApplyDedup 判定一条写 op 是否应当执行，并就地推进去重簿。
//
// 语义：
//   - 已见（op.Seq <= 该客户端最大已确认 seq）→ 重复，返回 false，不执行。
//   - 未见 → 标记该 (ClientId, Seq) 已确认，返回 true，调用方须真正执行该 op。
//
// 契约：raft 日志保证 op 按 seq 顺序、确定性地执行，故「标记即视为已生效」不会造成
// 数据空洞；一旦 ApplyDedup 返回 true，同 ClientId+Seq 的后续重发一律判重跳过，
// 这正是跨 rebalance 迁移后仍能保持幂等的关键。去重身份仅由 (ClientId, Seq) 决定，
// 与 Value 内容无关（同序号的重复重试即使 Value 不同也应被去重）。
func (d *DedupStore) ApplyDedup(op Op) bool {
	if d.Seen(op.ClientId, op.Seq) {
		return false
	}
	d.Mark(op.ClientId, op.Seq)
	return true
}
