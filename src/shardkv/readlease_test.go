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
