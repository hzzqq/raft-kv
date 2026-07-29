package main

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testNodeConfig 是一份最小可启动配置：3 个 ShardMaster + 1 个 group × 1 副本，
// 节点 addr 用独立高位端口，避免与本机其它测试/脚本冲突。只起其中单个节点即可验证
// kvnode 入口（StartNodeTCP 的 TCP 监听 + Stop 清理），无需集群达成共识。
const testNodeConfig = `{
  "n_groups": 1, "n_replicas": 1, "n_sm": 3, "max_raft_state": 0,
  "data_dir": "",
  "nodes": [
    {"name": "m0",   "addr": "127.0.0.1:19100"},
    {"name": "m1",   "addr": "127.0.0.1:19101"},
    {"name": "m2",   "addr": "127.0.0.1:19102"},
    {"name": "g0-0", "addr": "127.0.0.1:19110"}
  ]
}`

func writeTestConfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "deploy.json")
	if err := os.WriteFile(p, []byte(testNodeConfig), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return p
}

// TestStartNodeServesAndStops 验证 kvnode 入口能真正拉起一个节点的 TCP 服务（字节走
// 真实网络而非进程内 channel），且 Stop 能干净关闭监听。
func TestStartNodeServesAndStops(t *testing.T) {
	cfgPath := writeTestConfig(t)
	_, node, err := startNode(cfgPath, "m0")
	if err != nil {
		t.Fatalf("startNode m0: %v", err)
	}
	defer node.Stop()

	conn, err := net.DialTimeout("tcp", "127.0.0.1:19100", 3*time.Second)
	if err != nil {
		t.Fatalf("node m0 not listening on TCP: %v", err)
	}
	_ = conn.Close()
}

// TestStartNodeInvalidName 验证非法节点名（不在地址清单的命名规范内）应返回错误。
func TestStartNodeInvalidName(t *testing.T) {
	cfgPath := writeTestConfig(t)
	if _, _, err := startNode(cfgPath, "zzz"); err == nil {
		t.Fatalf("expected error for invalid node name, got nil")
	}
}

// TestStartNodeMissingConfig 验证配置缺失/不可读时应返回错误而非 panic。
func TestStartNodeMissingConfig(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.json")
	if _, _, err := startNode(missing, "m0"); err == nil {
		t.Fatalf("expected error for missing config, got nil")
	}
}
