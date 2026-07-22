package shardkv

import (
	"bytes"
	"encoding/gob"
)

// Snapshot 将分片状态序列化为字节（gob，零外部依赖），用于 Raft 快照落盘与跨节点
// 传输。配合 Restore 可做持久化与迁移前的深拷贝（避免共享底层 map 的并发竞态）。
func (sd *ShardData) Snapshot() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(sd); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Restore 从 Snapshot 字节恢复分片状态到接收者（解码失败返回 error）。
// 接收者原有字段被整体覆盖——即"以快照为准"，适合回放 Raft 快照或接收迁移数据。
func (sd *ShardData) Restore(b []byte) error {
	var tmp ShardData
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&tmp); err != nil {
		return err
	}
	sd.Data = tmp.Data
	sd.LastSeq = tmp.LastSeq
	sd.LastResult = tmp.LastResult
	return nil
}

// Merge 把 other 的分片状态并入 sd（用于接收迁移进来的分片）：
// Data / LastResult 以 other 为准（other 赢），LastSeq 按 client 取较大 seq，
// 保留去重连续性。other 为 nil 时为空操作。
func (sd *ShardData) Merge(other *ShardData) {
	if other == nil {
		return
	}
	if sd.Data == nil {
		sd.Data = map[string]string{}
	}
	if sd.LastSeq == nil {
		sd.LastSeq = map[int64]int64{}
	}
	if sd.LastResult == nil {
		sd.LastResult = map[int64]string{}
	}
	for k, v := range other.Data {
		sd.Data[k] = v
	}
	for k, v := range other.LastResult {
		sd.LastResult[k] = v
	}
	for cid, seq := range other.LastSeq {
		if cur, ok := sd.LastSeq[cid]; !ok || seq > cur {
			sd.LastSeq[cid] = seq
		}
	}
}

// Subtract 从 sd 中移除 other 拥有的键（用于把分片迁出给其它组）：
// Data 删除 other 中存在之键；LastSeq / LastResult 删除 other 中去重身份（client id）
// 对应的条目，使去重状态随分片一起迁移。other 为 nil 时为空操作。
func (sd *ShardData) Subtract(other *ShardData) {
	if other == nil {
		return
	}
	for k := range other.Data {
		delete(sd.Data, k)
	}
	for cid := range other.LastSeq {
		delete(sd.LastSeq, cid)
		delete(sd.LastResult, cid)
	}
}
