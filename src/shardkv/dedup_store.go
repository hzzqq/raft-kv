package shardkv

import "sync"

// DedupStore 是分片状态机的幂等去重簿：记录每个 Clerk 客户端已确认执行的最大 Seq。
// 判定重复只需比较 seq <= 该客户端的最大已确认 seq（单调序号即可，无需保存每个 op）。
//
// 关键能力是 Snapshot/Restore：分片在 rebalance 迁移到新副本时，去重簿必须随数据一起
// 搬运，否则迁移后旧 op 会被「误判为新」而重复执行（破坏线性一致）。Restore 用源副本的
// 快照覆盖目标副本，保证去重连续性。纯逻辑、可序列化、cluster-free 可测。
type DedupStore struct {
	mu      sync.Mutex
	lastSeq map[int64]int64 // clientID -> 该客户端已确认执行的最大 seq
}

// NewDedupStore 构造空去重簿。
func NewDedupStore() *DedupStore {
	return &DedupStore{lastSeq: make(map[int64]int64)}
}

// Seen 报告 (clientID, seq) 是否已经执行过：seq <= 该客户端已记录的最大 seq 即视为重复。
func (d *DedupStore) Seen(clientID, seq int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return seq <= d.lastSeq[clientID]
}

// Mark 标记 (clientID, seq) 已执行，推进该客户端最大 seq（仅增不减）。
// 调用方应在真正执行完 op 后调用，确保「标记即已生效」。
func (d *DedupStore) Mark(clientID, seq int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if seq > d.lastSeq[clientID] {
		d.lastSeq[clientID] = seq
	}
}

// MaxSeq 返回某客户端已确认的最大 seq（从未见过返回 0）。供快照/调试读取。
func (d *DedupStore) MaxSeq(clientID int64) int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastSeq[clientID]
}

// Snapshot 返回去重状态的不可变拷贝（深拷贝 map），供迁移时随分片数据一起搬运。
func (d *DedupStore) Snapshot() map[int64]int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	cp := make(map[int64]int64, len(d.lastSeq))
	for k, v := range d.lastSeq {
		cp[k] = v
	}
	return cp
}

// Restore 用快照覆盖当前去重状态。迁移目标副本接收分片时调用，保证去重连续：
// 源副本已确认的最大 seq 在目标副本同样被认作「已见」，旧 op 不会重复执行。
// 覆盖语义（非合并）符合分片所有权转移——目标副本此前对该分片无状态。
func (d *DedupStore) Restore(snap map[int64]int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastSeq = make(map[int64]int64, len(snap))
	for k, v := range snap {
		d.lastSeq[k] = v
	}
}
