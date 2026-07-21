package shardkv

import (
	"testing"
	"time"
)

// TestLinearizableReadLease：集成级验证 I1——leader 持租约时 Get 走 ReadIndex 快路径
// （read_leases 计数增长），且返回值线性一致正确。这是 HasLeaderLease 真正被 AE 应答
// 维护、并支撑本地线性一致读的证明。
func TestLinearizableReadLease(t *testing.T) {
	Metrics.Reset()
	cfg := makeSKVConfig(t, 1, 3, 3, 0)
	defer cfg.cleanup()
	ck := cfg.makeClerk()
	cfg.joinGroup(0)
	cfg.waitGroupConfig(0, 0, 1)

	ck.Put("lease-key", "lease-val")
	// 等待 leader 建立租约（心跳接触多数派），再高频 Get 触发 ReadIndex 快路径。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v := ck.Get("lease-key"); v != "lease-val" {
			t.Fatalf("Get 返回不一致: got %q want lease-val", v)
		}
		if Metrics.Counter("read_leases").Value() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if Metrics.Counter("read_leases").Value() == 0 {
		t.Fatalf("leader 租约有效时 Get 应走 ReadIndex 快路径（read_leases>0），但计数始终为 0")
	}
	// 再确认值仍正确。
	if v := ck.Get("lease-key"); v != "lease-val" {
		t.Fatalf("lease 路径后 Get 值不一致: %q", v)
	}
}

// TestReadLeaseLostOnPartition：集成级验证 leader 租约的「安全性」一半——
// 当 leader 与多数派失联（网络分区）时，HasLeaderLease 必须转为 false，
// 从而 Get 回退到 propose（而非凭过期租约做 stale 本地读）。这是 I1 ReadIndex
// 快读路径「不会返回陈旧读」的安全前提；若租约在无接触时仍为真，快读将违反线性一致。
func TestReadLeaseLostOnPartition(t *testing.T) {
	cfg := makeSKVConfig(t, 1, 3, 3, 0)
	defer cfg.cleanup()
	cfg.joinGroup(0)
	cfg.waitGroupConfig(0, 0, 1)

	leader := cfg.leaderOf(0)
	if leader < 0 {
		t.Fatalf("no leader before partition")
	}
	// 与另外两副本失联，使 leader 无法接触多数派（3 副本需 2 票）。
	others := []int{(leader + 1) % 3, (leader + 2) % 3}
	for _, r := range others {
		cfg.net.Enable(serverId(0, r), false)
	}

	// 等待超过 ElectionTimeoutMin，确认租约因无多数派接触而失效。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !cfg.groups[0][leader].rf.HasLeaderLease() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("partitioned leader %d still reports HasLeaderLease()==true; ReadIndex fast-path would serve stale reads", leader)
}
