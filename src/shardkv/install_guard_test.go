// install_guard_test.go —— I14：applyInstallShard「定居守卫」白盒回归测试。
//
// 背景：raft 快照死锁修复（SnapshotValid 正常投递）后，TestSKVReadIndex 100% 复现
// 静默丢写——分片已「定居」（本组持有 + 无未决迁移）并接收客户端写之后，残留 fetcher
// 的僵尸 InstallShard 以更高 MigrateConfigNum 携带陈旧空副本到达，LWW 将其整体替换，
// 已 ack 的客户端写凭空消失。修复=定居守卫：settled && !pendingIn 时直接拒绝安装。
//
// 本文件用纯白盒方式（不起 raft/网络）钉死该守卫的三条语义，防未来重构回退：
//  1. 定居后到达的僵尸 InstallShard（哪怕 migCfg 更高）必须被拒绝，数据保持不变；
//  2. pendingIn=true 期间的合法 InstallShard 必须正常安装（守卫不能误伤迁移主路径）;
//  3. 本组从未持有过该分片（未定居）时的首次安装必须放行。
package shardkv

import (
	"testing"

	"raftkv/src/shardmaster"
)

// newGuardKV 构造一个只含 applyInstallShard 依赖字段的最小 ShardKV（白盒）。
func newGuardKV(gid int, cfgNum int, ownShard int) *ShardKV {
	cfg := shardmaster.Config{Num: cfgNum, Groups: map[int][]string{}}
	cfg.Shards[ownShard] = gid
	return &ShardKV{
		gid:             gid,
		config:          cfg,
		shards:          map[int]*ShardData{},
		incoming:        map[int]*ShardData{},
		pendingIn:       map[int]bool{},
		pendingOut:      map[int]bool{},
		installedCfgNum: map[int]int{},
	}
}

func installOp(shard, cfgNum int, data map[string]string) Op {
	sd := &ShardData{Data: map[string]string{}, LastSeq: map[int64]int64{}, LastResult: map[int64]string{}}
	for k, v := range data {
		sd.Data[k] = v
	}
	return Op{Kind: "InstallShard", MigrateShard: shard, MigrateData: sd, MigrateConfigNum: cfgNum}
}

// TestInstallGuardRejectsZombieAfterSettled：分片定居后（持有 + !pendingIn），
// 更高配置号的空副本 InstallShard 必须被拒绝，已写入的数据不能被覆盖。
// 这正是 TestSKVReadIndex 丢写轨迹的最小化重放：
// g1 cfg3 装入 s0 → Put ric-0-1="c0-r1" ack → 僵尸 InstallShard(migCfg=4, nkeys=0)。
func TestInstallGuardRejectsZombieAfterSettled(t *testing.T) {
	kv := newGuardKV(1, 3, 0)
	// 分片 0 已定居：持有数据、installedCfgNum=3、无 pendingIn。
	kv.shards[0] = &ShardData{
		Data:       map[string]string{"ric-0-1": "c0-r1"},
		LastSeq:    map[int64]int64{766: 9},
		LastResult: map[int64]string{},
	}
	kv.installedCfgNum[0] = 3

	// 僵尸提案：migCfg=4 > installed=3，但携带空副本。
	var res applyResult
	kv.applyInstallShard(installOp(0, 4, nil), &res)

	if res.err != OK {
		t.Fatalf("zombie install should be acked OK (idempotent), got %v", res.err)
	}
	if got := kv.shards[0].Data["ric-0-1"]; got != "c0-r1" {
		t.Fatalf("settled shard overwritten by zombie install: ric-0-1=%q want %q", got, "c0-r1")
	}
	if kv.installedCfgNum[0] != 3 {
		t.Fatalf("installedCfgNum bumped by rejected install: got %d want 3", kv.installedCfgNum[0])
	}
}

// TestInstallGuardAllowsPendingIn：pendingIn=true（本组正在等待该分片）期间的
// InstallShard 是迁移主路径，必须正常安装并清除 pendingIn——守卫不能误伤。
func TestInstallGuardAllowsPendingIn(t *testing.T) {
	kv := newGuardKV(1, 4, 0)
	// 上一配置遗留的旧副本仍在（A→B→A 来回再平衡场景），但已标记 pendingIn 等待新副本。
	kv.shards[0] = &ShardData{Data: map[string]string{"k": "stale"}, LastSeq: map[int64]int64{}, LastResult: map[int64]string{}}
	kv.installedCfgNum[0] = 2
	kv.pendingIn[0] = true

	var res applyResult
	kv.applyInstallShard(installOp(0, 4, map[string]string{"k": "fresh"}), &res)

	if res.err != OK {
		t.Fatalf("legit install during pendingIn rejected: %v", res.err)
	}
	if got := kv.shards[0].Data["k"]; got != "fresh" {
		t.Fatalf("pendingIn install not applied: k=%q want %q", got, "fresh")
	}
	if kv.pendingIn[0] {
		t.Fatalf("pendingIn not cleared after install")
	}
	if kv.installedCfgNum[0] != 4 {
		t.Fatalf("installedCfgNum=%d want 4", kv.installedCfgNum[0])
	}
}

// TestInstallGuardAllowsFirstInstall：本组从未持有该分片（未定居）时的首次安装
// 必须放行——守卫只拦「已定居」后的重复安装。
func TestInstallGuardAllowsFirstInstall(t *testing.T) {
	kv := newGuardKV(1, 2, 0)
	// shards 里没有分片 0（未定居），也未标 pendingIn（自愈回源路径）。
	var res applyResult
	kv.applyInstallShard(installOp(0, 2, map[string]string{"a": "1"}), &res)

	if res.err != OK {
		t.Fatalf("first install rejected: %v", res.err)
	}
	sd, ok := kv.shards[0]
	if !ok || sd.Data["a"] != "1" {
		t.Fatalf("first install not applied: %+v", sd)
	}
	if kv.installedCfgNum[0] != 2 {
		t.Fatalf("installedCfgNum=%d want 2", kv.installedCfgNum[0])
	}
}
