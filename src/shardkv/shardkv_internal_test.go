package shardkv

import (
	"sync"
	"testing"
	"time"

	"raftkv/src/shardmaster"
)

// 这些白盒单测直接驱动迁移状态机的核心纯函数，补强 src/shardkv 覆盖率
// （当前最薄弱的迁移合并/安装路径），不依赖完整集群，运行极快。

// TestShardDataCopy：深拷贝语义——原对象与副本互不影响。
func TestShardDataCopy(t *testing.T) {
	sd := &ShardData{
		Data:       map[string]string{"k": "v"},
		LastSeq:    map[int64]int64{1: 5},
		LastResult: map[int64]string{1: "r"},
	}
	cp := sd.copy()

	// 修改原对象不应影响副本
	sd.Data["k"] = "changed"
	sd.LastSeq[1] = 99
	if cp.Data["k"] != "v" {
		t.Fatalf("copy.Data 被原对象修改: got %q want v", cp.Data["k"])
	}
	if cp.LastSeq[1] != 5 {
		t.Fatalf("copy.LastSeq 被原对象修改: got %d want 5", cp.LastSeq[1])
	}

	// 修改副本不应影响原对象
	cp.Data["new"] = "x"
	cp.LastSeq[9] = 7
	if _, ok := sd.Data["new"]; ok {
		t.Fatalf("原对象 Data 被副本修改")
	}
	if _, ok := sd.LastSeq[9]; ok {
		t.Fatalf("原对象 LastSeq 被副本修改")
	}
}

// TestMergeShardData：合并只补充本组缺失的 key，且 LastSeq/LastResult 取较大者，
// 不冲掉本组已有的（通常更新的）value。这是迁移窗口内"新 owner 已直接写入"不被
// 旧 owner 快照覆盖的正确性核心。
func TestMergeShardData(t *testing.T) {
	kv := &ShardKV{
		gid:     1,
		config:  shardmaster.Config{Shards: [NShards]int{}},
		shards:  map[int]*ShardData{},
	}
	kv.config.Shards[3] = 1
	kv.shards[3] = &ShardData{
		Data:       map[string]string{"a": "base", "b": "base"},
		LastSeq:    map[int64]int64{1: 10, 2: 5},
		LastResult: map[int64]string{1: "base1", 2: "base2"},
	}
	incoming := &ShardData{
		Data:       map[string]string{"b": "newB", "c": "newC"},
		LastSeq:    map[int64]int64{2: 20, 3: 1},
		LastResult: map[int64]string{2: "new2", 3: "new3"},
	}
	kv.mergeShardData(3, incoming)
	sd := kv.shards[3]

	if sd.Data["a"] != "base" {
		t.Fatalf("已有 key 'a' 被覆盖: %q", sd.Data["a"])
	}
	if sd.Data["b"] != "base" {
		t.Fatalf("已有 key 'b' 被 incoming 覆盖: %q", sd.Data["b"])
	}
	if sd.Data["c"] != "newC" {
		t.Fatalf("缺失 key 'c' 未补入: %q", sd.Data["c"])
	}
	if sd.LastSeq[2] != 20 {
		t.Fatalf("LastSeq[2] 未取较大者: got %d want 20", sd.LastSeq[2])
	}
	if sd.LastSeq[1] != 10 {
		t.Fatalf("LastSeq[1] 退化: got %d want 10", sd.LastSeq[1])
	}
	if sd.LastResult[2] != "new2" {
		t.Fatalf("LastResult[2] 未取较大者: %q", sd.LastResult[2])
	}
	if sd.LastResult[1] != "base1" {
		t.Fatalf("LastResult[1] 退化: %q", sd.LastResult[1])
	}
}

// TestApplyInstallShardIdempotent：同一分片重复安装应幂等——首次装入、再次合并
// （不覆盖），数据不丢失、不重复。对应 applyInstallShard 中"已拥有则该分片合并"
// 分支，是迁移去重正确性的护栏。
func TestApplyInstallShardIdempotent(t *testing.T) {
	kv := &ShardKV{
		gid:       1,
		config:    shardmaster.Config{Shards: [NShards]int{}},
		shards:    map[int]*ShardData{},
		pendingIn: map[int]bool{},
		incoming:  map[int]*ShardData{},
	}
	kv.config.Shards[5] = 1

	data := &ShardData{
		Data:       map[string]string{"x": "y"},
		LastSeq:    map[int64]int64{1: 1},
		LastResult: map[int64]string{1: "y"},
	}
	op := Op{Kind: "InstallShard", MigrateShard: 5, MigrateData: data}
	var res applyResult
	kv.applyInstallShard(op, &res)
	if res.err != OK {
		t.Fatalf("首次安装 err=%v", res.err)
	}
	if kv.shards[5] == nil || kv.shards[5].Data["x"] != "y" {
		t.Fatalf("分片未装入")
	}
	if _, ok := kv.pendingIn[5]; ok {
		t.Fatalf("首次安装后 pendingIn[5] 应已清除")
	}

	// 第二次安装同一分片：应幂等（已拥有 -> 合并，不再整体覆盖）
	op2 := Op{Kind: "InstallShard", MigrateShard: 5, MigrateData: data.copy()}
	var res2 applyResult
	kv.applyInstallShard(op2, &res2)
	if res2.err != OK {
		t.Fatalf("二次安装 err=%v", res2.err)
	}
	if kv.shards[5].Data["x"] != "y" {
		t.Fatalf("幂等安装破坏了数据: %q", kv.shards[5].Data["x"])
	}
	// 深拷贝护栏：传入的 data 指针不应与本组运行态分片别名。
	if kv.shards[5] == data {
		t.Fatalf("applyInstallShard 未深拷贝，运行态与 Raft 日志共享同一 ShardData 指针")
	}
}

// TestApplyNewConfigClearsPendingInOnIncoming：applyNewConfig 在把缓冲于 incoming 的
// 分片数据装入本组 shards 时，必须清除 pendingIn[s]——否则 pollConfig 被 pendingIn 门控
// 而无法推进配置，形成"收方等配置推进清 pendingIn / 配置推进又被 pendingIn 阻塞"的死锁
// （cycle 48 根因修复的回归护栏）。这是 3-group / ReMigration churn 冻结的核心修复点。
func TestApplyNewConfigClearsPendingInOnIncoming(t *testing.T) {
	kv := &ShardKV{
		gid:        1,
		config:     shardmaster.Config{Num: 0, Shards: [NShards]int{}},
		prevConfig: shardmaster.Config{Num: 0, Shards: [NShards]int{}},
		shards:     map[int]*ShardData{},
		incoming:   map[int]*ShardData{},
		pendingIn:  map[int]bool{},
		pendingOut: map[int]bool{},
		fetchEpoch: map[int]int64{},
	}
	// 模拟"配置尚未推进到拥有 s 时就收到 InstallShard，数据缓冲在 incoming"。
	kv.incoming[7] = &ShardData{
		Data:       map[string]string{"k": "v7"},
		LastSeq:    map[int64]int64{1: 1},
		LastResult: map[int64]string{1: "v7"},
	}
	// 注意：applyNewConfig 会先把 kv.prevConfig = kv.config，故"上一版 owner"设在
	// 旧配置（kv.config）上；新配置 cfg 让本组拥有分片 7。
	kv.config.Shards[7] = 2 // 旧配置下分片 7 的 owner 是 group 2
	// 配置推进到 1：本组现在拥有分片 7（但数据仍在 incoming，尚未装入 shards）。
	cfg := shardmaster.Config{Num: 1, Shards: [NShards]int{}}
	cfg.Shards[7] = 1
	kv.applyNewConfig(cfg)

	if kv.shards[7] == nil || kv.shards[7].Data["k"] != "v7" {
		t.Fatalf("分片 7 未从 incoming 装入: shards[7]=%v", kv.shards[7])
	}
	if _, ok := kv.incoming[7]; ok {
		t.Fatalf("incoming[7] 未清除")
	}
	if _, ok := kv.pendingIn[7]; ok {
		t.Fatalf("cycle 48 回归: applyNewConfig 消费 incoming 后未清除 pendingIn[7]，将导致配置死锁")
	}
}

// TestSnapshotMigrationRoundTrip：快照编码→安装往返必须完整保留迁移态
// （shards / incoming / pendingIn / pendingOut / config / prevConfig 及内嵌的
// 客户端去重 LastSeq/LastResult），否则崩溃恢复会丢数据或卡死配置推进。
// 同时验证 installSnapshot 会把看门狗时间戳重置为「现在」，避免沿用崩溃前
// 的陈旧时间误判卡死、让 /debug/shards 的 StallSeconds 失真。
func TestSnapshotMigrationRoundTrip(t *testing.T) {
	old := time.Now().Add(-10 * time.Minute) // 模拟崩溃前的陈旧时间戳
	kv := &ShardKV{
		mu:             sync.Mutex{},
		shards:         map[int]*ShardData{},
		incoming:       map[int]*ShardData{},
		pendingIn:      map[int]bool{},
		pendingOut:     map[int]bool{},
		pendingInSince: map[int]time.Time{},
		pendingOutSince: map[int]time.Time{},
	}
	kv.shards[0] = &ShardData{
		Data:       map[string]string{"k": "v"},
		LastSeq:    map[int64]int64{1: 2},
		LastResult: map[int64]string{1: "v"},
	}
	kv.incoming[1] = &ShardData{Data: map[string]string{"a": "b"}}
	kv.pendingIn[1] = true
	kv.pendingOut[2] = true
	kv.pendingInSince[1] = old
	kv.pendingOutSince[2] = old
	kv.config = shardmaster.Config{Num: 5, Groups: map[int][]string{1: {"s1"}, 2: {"s2"}}}
	kv.config.Shards[0] = 1
	kv.config.Shards[1] = 2
	kv.config.Shards[2] = 1
	kv.prevConfig = shardmaster.Config{Num: 4, Groups: map[int][]string{1: {"s1"}, 2: {"s2"}}}
	kv.prevConfig.Shards[1] = 1

	data := kv.encodeSnapshot()

	// 破坏源状态，证明 install 是从快照重建、而非读残留。
	kv.shards = nil
	kv.incoming = nil
	kv.pendingIn = nil
	kv.pendingOut = nil
	kv.pendingInSince = nil
	kv.pendingOutSince = nil

	kv2 := &ShardKV{mu: sync.Mutex{}}
	kv2.installSnapshot(data)

	if kv2.shards[0] == nil || kv2.shards[0].Data["k"] != "v" {
		t.Fatalf("shards[0] 未恢复: %v", kv2.shards[0])
	}
	if kv2.shards[0].LastSeq[1] != 2 || kv2.shards[0].LastResult[1] != "v" {
		t.Fatalf("shards[0] 去重态未恢复: %+v", kv2.shards[0])
	}
	if kv2.incoming[1] == nil || kv2.incoming[1].Data["a"] != "b" {
		t.Fatalf("incoming[1] 未恢复")
	}
	if !kv2.pendingIn[1] {
		t.Fatalf("pendingIn[1] 未恢复")
	}
	if !kv2.pendingOut[2] {
		t.Fatalf("pendingOut[2] 未恢复")
	}
	if kv2.config.Num != 5 || kv2.config.Shards[0] != 1 {
		t.Fatalf("config 未恢复: num=%d shards[0]=%d", kv2.config.Num, kv2.config.Shards[0])
	}
	if kv2.prevConfig.Num != 4 {
		t.Fatalf("prevConfig 未恢复: num=%d", kv2.prevConfig.Num)
	}

	// 看门狗时间戳必须重置为「现在」附近，而非沿用崩溃前的 old（-10min）。
	pin, okIn := kv2.pendingInSince[1]
	pout, okOut := kv2.pendingOutSince[2]
	if !okIn || !okOut {
		t.Fatalf("installSnapshot 未重置看门狗时间戳")
	}
	if pin.Before(time.Now().Add(-30 * time.Second)) {
		t.Fatalf("pendingInSince 仍沿用陈旧时间戳（崩溃前），应重置为现在: %v", pin)
	}
	if pout.Before(time.Now().Add(-30 * time.Second)) {
		t.Fatalf("pendingOutSince 仍沿用陈旧时间戳（崩溃前），应重置为现在: %v", pout)
	}
	_ = old
}

// TestApplyGCKeepsOwnedShard：GC 守卫——本组当前配置仍拥有该分片（A→B→A 快速回摆）时，
// applyGC 不得删除其权威数据，仅清除 pendingOut 标记。否则 migratePump 触发的自 GC 会
// 删掉本组仍持有的分片造成丢数据。
func TestApplyGCKeepsOwnedShard(t *testing.T) {
	kv := &ShardKV{
		gid:        1,
		config:     shardmaster.Config{Shards: [NShards]int{}},
		shards:     map[int]*ShardData{},
		pendingOut: map[int]bool{},
	}
	kv.config.Shards[3] = 1 // 本组仍拥有分片 3
	kv.shards[3] = &ShardData{Data: map[string]string{"k": "v"}, LastSeq: map[int64]int64{}, LastResult: map[int64]string{}}
	kv.pendingOut[3] = true // 残留待迁出标记（churn 回摆遗留）

	kv.applyGC(3)

	if kv.shards[3] == nil || kv.shards[3].Data["k"] != "v" {
		t.Fatalf("applyGC 误删本组仍拥有的分片: %v", kv.shards[3])
	}
	if _, ok := kv.pendingOut[3]; ok {
		t.Fatalf("applyGC 未清除 pendingOut[3]")
	}
}

// TestApplyGCDeletesUnownedShard：本组不再拥有该分片（正常迁移完成）时，applyGC 必须
// 删除副本并清除 pendingOut，否则内存泄漏且 pendingOut 残留可能干扰看门狗。
func TestApplyGCDeletesUnownedShard(t *testing.T) {
	kv := &ShardKV{
		gid:        1,
		config:     shardmaster.Config{Shards: [NShards]int{}},
		shards:     map[int]*ShardData{},
		pendingOut: map[int]bool{},
	}
	kv.config.Shards[3] = 2 // 分片 3 已属 group 2
	kv.shards[3] = &ShardData{Data: map[string]string{"k": "v"}, LastSeq: map[int64]int64{}, LastResult: map[int64]string{}}
	kv.pendingOut[3] = true

	kv.applyGC(3)

	if _, ok := kv.shards[3]; ok {
		t.Fatalf("applyGC 未删除本组不再拥有的分片")
	}
	if _, ok := kv.pendingOut[3]; ok {
		t.Fatalf("applyGC 未清除 pendingOut[3]")
	}
}

// TestResolveShardForTransfer：GetShard 的数据解析核心——优先返回已生效 shards[s]，
// 缺失时回退 incoming[s]（3-group 多跳 rebalance 孤儿 incoming 的冻结修复核心），
// 二者皆无才 ErrWrongGroup。白盒验证 cycle 57 的 GetShard 回退修复不回归。
func TestResolveShardForTransfer(t *testing.T) {
	kv := &ShardKV{
		gid:      1,
		shards:   map[int]*ShardData{},
		incoming: map[int]*ShardData{},
	}
	// 既无 shards 也无 incoming：应 ErrWrongGroup。
	if _, err := kv.resolveShardForTransfer(5); err != ErrWrongGroup {
		t.Fatalf("两者皆无应 ErrWrongGroup, got %v", err)
	}

	// 仅 incoming 有（本组不拥有该分片，但缓冲了迟到推送）：应回退返回 incoming，
	// 这正是 3-group churn 冻结根因的修复点——真正主人能直接拉走中转数据。
	kv.incoming[5] = &ShardData{Data: map[string]string{"x": "y"}, LastSeq: map[int64]int64{}, LastResult: map[int64]string{}}
	data, err := kv.resolveShardForTransfer(5)
	if err != OK {
		t.Fatalf("incoming 回退应 OK, got %v", err)
	}
	if data.Data["x"] != "y" {
		t.Fatalf("回退未返回 incoming 数据: %v", data.Data)
	}

	// shards 优先于 incoming：已结算态优先，不会误返回陈旧 incoming（ReMigration 不回归）。
	kv.shards[5] = &ShardData{Data: map[string]string{"x": "settled"}, LastSeq: map[int64]int64{}, LastResult: map[int64]string{}}
	data2, err2 := kv.resolveShardForTransfer(5)
	if err2 != OK {
		t.Fatalf("shards 优先应 OK, got %v", err2)
	}
	if data2.Data["x"] != "settled" {
		t.Fatalf("应优先返回 shards 而非 incoming: %v", data2.Data)
	}
}

// TestApplyInstallShardBuffersWhenNotOwner：applyInstallShard 在「本组配置尚未推进到
// 拥有该分片」时应把数据缓冲进 incoming 而非装入 shards——这是孤儿 incoming 路径的
// 前置条件，也是 GetShard 回退能派上用场的前提。验证缓冲语义正确。
func TestApplyInstallShardBuffersWhenNotOwner(t *testing.T) {
	kv := &ShardKV{
		gid:       1,
		config:    shardmaster.Config{Shards: [NShards]int{}},
		shards:    map[int]*ShardData{},
		incoming:  map[int]*ShardData{},
		pendingIn: map[int]bool{},
	}
	// 本组配置下分片 5 的 owner 是 group 2，但本组收到推送：应缓冲进 incoming。
	kv.config.Shards[5] = 2

	data := &ShardData{Data: map[string]string{"k": "v"}, LastSeq: map[int64]int64{1: 1}, LastResult: map[int64]string{1: "v"}}
	op := Op{Kind: "InstallShard", MigrateShard: 5, MigrateData: data}
	var res applyResult
	kv.applyInstallShard(op, &res)
	if res.err != OK {
		t.Fatalf("缓冲分支 err=%v", res.err)
	}
	if _, ok := kv.shards[5]; ok {
		t.Fatalf("非 owner 时不应装入 shards（应缓冲 incoming）")
	}
	if kv.incoming[5] == nil || kv.incoming[5].Data["k"] != "v" {
		t.Fatalf("应缓冲进 incoming: %v", kv.incoming[5])
	}
	// 深拷贝护栏：传入的 data 指针不应与本组运行态别名。
	if kv.incoming[5] == data {
		t.Fatalf("applyInstallShard 未深拷贝，运行态与 Raft 日志共享同一 ShardData 指针")
	}
}
