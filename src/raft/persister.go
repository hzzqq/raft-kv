// persister.go —— 持久化层（仿 MIT 6.824 的 Persister）
//
// Persister 是 raft / shardmaster 实际依赖的持久化接口；本文件提供两种实现：
//   * MemoryPersister —— 内存实现，测试里用它模拟掉电重启（cluster-free）；
//   * FilePersister    —— 落盘实现（临时文件写 + fsync + 原子 rename），真实部署用，
//     崩溃后复用同一目录即可恢复（见 file_persister.go）。
package raft

import "sync"

// Persister 是 Raft 状态机持久化的接口契约。raft / shardmaster 仅依赖此接口，
// 真实部署可把内存 MemoryPersister 换成落盘的 FilePersister，核心逻辑无需改动。
type Persister interface {
	// SaveRaftState 持久化 Raft 状态（任期/投票/日志）。
	SaveRaftState(data []byte)
	// ReadRaftState 读取已持久化的 Raft 状态字节（未持久化时返回 nil）。
	ReadRaftState() []byte
	// SaveSnapshot 持久化状态机快照。
	SaveSnapshot(snapshot []byte)
	// ReadSnapshot 读取已持久化的快照字节（未持久化时返回 nil）。
	ReadSnapshot() []byte
}

// MemoryPersister 是 Persister 的内存实现（测试用）。
type MemoryPersister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

// MakeEmptyPersister 创建一个空的内存 Persister。
func MakeEmptyPersister() *MemoryPersister {
	return &MemoryPersister{}
}

// Copy 返回当前 MemoryPersister 的快照副本，隔离后续修改（仅内存实现需要）。
func (p *MemoryPersister) Copy() *MemoryPersister {
	p.mu.Lock()
	defer p.mu.Unlock()
	np := &MemoryPersister{}
	np.raftstate = append([]byte{}, p.raftstate...)
	np.snapshot = append([]byte{}, p.snapshot...)
	return np
}

// SaveRaftState 持久化 Raft 状态机（任期/投票/日志）。
func (p *MemoryPersister) SaveRaftState(data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.raftstate = append([]byte{}, data...)
}

// ReadRaftState 读取已持久化的 Raft 状态字节。
func (p *MemoryPersister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.raftstate
}

// SaveSnapshot 持久化状态机快照。
func (p *MemoryPersister) SaveSnapshot(snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshot = append([]byte{}, snapshot...)
}

// ReadSnapshot 读取已持久化的快照字节。
func (p *MemoryPersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshot
}
