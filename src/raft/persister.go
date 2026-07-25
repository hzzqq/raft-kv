// persister.go —— 内存版持久化层（仿 MIT 6.824 的 Persister）
// 真实部署时换成写磁盘即可；测试里用内存模拟掉电重启。
package raft

import "sync"
// Persister 提供 Raft 状态与快照的持久化存储接口（测试用内存实现）。
type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}
// MakeEmptyPersister 创建一个空的内存 Persister。
func MakeEmptyPersister() *Persister {
	return &Persister{}
}
// Copy 返回当前 Persister 的快照副本，隔离后续修改。
func (p *Persister) Copy() *Persister {
	p.mu.Lock()
	defer p.mu.Unlock()
	np := &Persister{}
	np.raftstate = append([]byte{}, p.raftstate...)
	np.snapshot = append([]byte{}, p.snapshot...)
	return np
}
// SaveRaftState 持久化 Raft 状态机（任期/投票/日志）。
func (p *Persister) SaveRaftState(data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.raftstate = append([]byte{}, data...)
}
// ReadRaftState 读取已持久化的 Raft 状态字节。
func (p *Persister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.raftstate
}
// SaveSnapshot 持久化状态机快照。
func (p *Persister) SaveSnapshot(snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshot = append([]byte{}, snapshot...)
}
// ReadSnapshot 读取已持久化的快照字节。
func (p *Persister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshot
}
