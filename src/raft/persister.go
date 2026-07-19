// persister.go —— 内存版持久化层（仿 MIT 6.824 的 Persister）
// 真实部署时换成写磁盘即可；测试里用内存模拟掉电重启。
package raft

import "sync"

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakeEmptyPersister() *Persister {
	return &Persister{}
}

func (p *Persister) Copy() *Persister {
	p.mu.Lock()
	defer p.mu.Unlock()
	np := &Persister{}
	np.raftstate = append([]byte{}, p.raftstate...)
	np.snapshot = append([]byte{}, p.snapshot...)
	return np
}

func (p *Persister) SaveRaftState(data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.raftstate = append([]byte{}, data...)
}

func (p *Persister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.raftstate
}

func (p *Persister) SaveSnapshot(snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshot = append([]byte{}, snapshot...)
}

func (p *Persister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshot
}
