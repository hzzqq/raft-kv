package shardkv

import (
	"testing"

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
