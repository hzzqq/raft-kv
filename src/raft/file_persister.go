// file_persister.go —— Persister 的落盘实现（真实部署用）
//
// 与内存 MemoryPersister 不同，FilePersister 把 Raft 状态/快照写入磁盘目录，
// 支持进程崩溃后的状态恢复。写入采用「临时文件 + fsync + 原子 rename」保证
// 崩溃安全：即使写的过程中进程被杀，已落盘的状态也不会损坏（rename 在同一
// 文件系统上是原子的）。重启时复用同一目录即可读取上一次持久化的状态。
package raft

import (
	"os"
	"path/filepath"
	"sync"
)

// FilePersister 把 Raft 状态/快照落盘到 dir 目录，支持真实部署的崩溃恢复。
// 一个节点对应一个 FilePersister（通常 dir 形如 data/node-g0-r0）。
type FilePersister struct {
	dir string
	mu  sync.Mutex
}

// NewFilePersister 创建一个落盘 Persister，dir 为状态/快照存放目录（自动创建）。
func NewFilePersister(dir string) *FilePersister {
	_ = os.MkdirAll(dir, 0o700)
	return &FilePersister{dir: dir}
}

func (f *FilePersister) statePath() string { return filepath.Join(f.dir, "raft_state.bin") }
func (f *FilePersister) snapPath() string  { return filepath.Join(f.dir, "snapshot.bin") }

// writeSyncAtomic 先把数据写到同目录临时文件，fsync 后再原子 rename 覆盖目标，
// 保证崩溃安全（rename 在同一文件系统内原子）。
func writeSyncAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// SaveRaftState 持久化 Raft 状态机（任期/投票/日志）。
func (f *FilePersister) SaveRaftState(data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_ = writeSyncAtomic(f.statePath(), append([]byte{}, data...))
}

// ReadRaftState 读取已持久化的 Raft 状态字节；未持久化时返回 nil。
func (f *FilePersister) ReadRaftState() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, err := os.ReadFile(f.statePath())
	if err != nil {
		return nil
	}
	return b
}

// SaveSnapshot 持久化状态机快照。
func (f *FilePersister) SaveSnapshot(snapshot []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_ = writeSyncAtomic(f.snapPath(), append([]byte{}, snapshot...))
}

// ReadSnapshot 读取已持久化的快照字节；未持久化时返回 nil。
func (f *FilePersister) ReadSnapshot() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, err := os.ReadFile(f.snapPath())
	if err != nil {
		return nil
	}
	return b
}
