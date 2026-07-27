package cluster

import (
	"testing"
	"time"
)

// TestClusterCrashRecovery 验证真部署化路径：用 FilePersister 落盘的集群在「进程崩溃」
// （Cleanup 模拟）后，复用同一数据目录重启，能够恢复此前已提交的 KV 数据。
//
// 这是 T4(FilePersister 落盘) 与 T5(demo / gateway 接线) 的端到端交付验证——
// 把 raft 的崩溃安全从「单元测试里的字节读写」升级为「真实集群重启后业务数据可恢复」。
func TestClusterCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	cfg := StartClusterWithPersister(2, 3, 3, 0, FilePersisterFactory(dir))
	ck := cfg.Clerk()
	cfg.Join(0)
	cfg.WaitConfig(0, 0, 1)
	cfg.Join(1)
	cfg.WaitConfig(1, 0, 2)

	ck.Put("crashkey", "crashval")
	// 等待写入在多数副本上提交并落盘（FilePersister 写入即 fsync）。
	time.Sleep(600 * time.Millisecond)

	// 模拟进程崩溃：杀掉所有节点。状态已落盘到 dir，不应丢失。
	cfg.Cleanup()

	// 复用同一数据目录重启集群（ShardMaster 与 ShardKV 各自从磁盘恢复 raft 状态）。
	cfg2 := StartClusterWithPersister(2, 3, 3, 0, FilePersisterFactory(dir))
	defer cfg2.Cleanup()
	ck2 := cfg2.Clerk()

	// 崩溃恢复后轮询读取：重启的 raft 会重放持久化日志，重建 KV 存储。
	deadline := time.Now().Add(6 * time.Second)
	got := ""
	recovered := false
	for time.Now().Before(deadline) {
		got = ck2.Get("crashkey")
		if got == "crashval" {
			recovered = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !recovered {
		t.Fatalf("崩溃恢复后未读回 crashval, got=%q", got)
	}
}
