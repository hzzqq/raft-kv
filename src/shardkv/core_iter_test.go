package shardkv

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"raftkv/src/shardmaster"
)

// 本轮迭代 I2/I3/I4/I5/I9/I15 的白盒单测，直接驱动迁移状态机核心路径，
// 不依赖完整集群，运行快。每个用例先 Reset 包级 Metrics 避免跨用例累积。

// I2：InstallShard 按 MigrateConfigNum 幂等——重复安装（相同/更旧配置号）应幂等，
// 不重复触发迁移耗时计数与数据抖动；更新配置号则刷新。
func TestInstallShardConfigNumIdempotent(t *testing.T) {
	Metrics.Reset()
	kv := &ShardKV{
		gid:             1,
		config:          shardmaster.Config{Shards: [NShards]int{}},
		shards:          map[int]*ShardData{},
		pendingIn:       map[int]bool{},
		incoming:        map[int]*ShardData{},
		installedCfgNum: map[int]int{},
	}
	kv.config.Shards[5] = 1
	data := &ShardData{
		Data:       map[string]string{"x": "y"},
		LastSeq:    map[int64]int64{1: 1},
		LastResult: map[int64]string{1: "y"},
	}
	op := Op{Kind: "InstallShard", MigrateShard: 5, MigrateData: data, MigrateConfigNum: 3}
	var res applyResult
	kv.applyInstallShard(op, &res)
	if res.err != OK || kv.shards[5] == nil || kv.shards[5].Data["x"] != "y" {
		t.Fatalf("首次安装失败: %v", res.err)
	}
	// 相同配置号重复安装 -> 幂等。
	kv.applyInstallShard(op, &res)
	if res.err != OK || kv.shards[5].Data["x"] != "y" {
		t.Fatalf("重复安装数据被破坏: %v", res.err)
	}
	// 更旧配置号 -> 视为过期，丢弃（不缓冲、不安装）。
	oldOp := Op{Kind: "InstallShard", MigrateShard: 5, MigrateData: data, MigrateConfigNum: 2}
	kv.applyInstallShard(oldOp, &res)
	if res.err != OK {
		t.Fatalf("过期安装应返回 OK")
	}
	if _, ok := kv.incoming[5]; ok {
		t.Fatalf("过期分片不应缓冲")
	}
	// 更新配置号 -> 刷新安装。
	newOp := Op{Kind: "InstallShard", MigrateShard: 5, MigrateData: data, MigrateConfigNum: 4}
	kv.applyInstallShard(newOp, &res)
	if res.err != OK {
		t.Fatalf("更新配置号安装失败: %v", res.err)
	}
}

// I3：过期 incoming 分片（MigrateConfigNum < config.Num）应直接丢弃，不污染状态。
func TestDropStaleIncoming(t *testing.T) {
	Metrics.Reset()
	kv := &ShardKV{
		gid:             1,
		config:          shardmaster.Config{Num: 5, Shards: [NShards]int{}},
		shards:          map[int]*ShardData{},
		pendingIn:       map[int]bool{},
		incoming:        map[int]*ShardData{},
		installedCfgNum: map[int]int{},
	}
	data := &ShardData{Data: map[string]string{"old": "1"}}
	// 分片 5 当前不归本组（配置已推进到 5），收到配置号 3 的旧迁移 -> 丢弃。
	op := Op{Kind: "InstallShard", MigrateShard: 5, MigrateData: data, MigrateConfigNum: 3}
	var res applyResult
	kv.applyInstallShard(op, &res)
	if res.err != OK {
		t.Fatalf("丢弃过期分片应返回 OK")
	}
	if _, ok := kv.incoming[5]; ok {
		t.Fatalf("过期分片不应缓冲到 incoming")
	}
	if _, ok := kv.shards[5]; ok {
		t.Fatalf("过期分片不应安装")
	}
	// 同配置号（5）的迁移应正常缓冲（config.Shards[5]!=gid，进 incoming）。
	op2 := Op{Kind: "InstallShard", MigrateShard: 5, MigrateData: data, MigrateConfigNum: 5}
	kv.applyInstallShard(op2, &res)
	if _, ok := kv.incoming[5]; !ok {
		t.Fatalf("同配置号迁移应缓冲到 incoming")
	}
}

// I4：配置推进（仅 gain-only，不触发 fetch/send 协程）后 pendingIn/pendingOut 无残留
// 泄漏；GC 完成后 pendingOut 应清除。
func TestNoPendingLeakAfterConfigAdvance(t *testing.T) {
	Metrics.Reset()
	kv := &ShardKV{
		gid:             1,
		config:          shardmaster.Config{Num: 0, Shards: [NShards]int{}},
		prevConfig:      shardmaster.Config{Num: 0, Shards: [NShards]int{}},
		shards:          map[int]*ShardData{},
		incoming:        map[int]*ShardData{},
		pendingIn:       map[int]bool{},
		pendingOut:      map[int]bool{},
		installedCfgNum: map[int]int{},
	}
	cfg1 := shardmaster.Config{Num: 1, Shards: [NShards]int{}}
	cfg1.Shards[3] = 1
	cfg1.Shards[4] = 1
	kv.applyNewConfig(cfg1)
	if len(kv.pendingIn) != 0 || len(kv.pendingOut) != 0 {
		t.Fatalf("gain-only 配置推进后不应有 pending 泄漏: in=%v out=%v", kv.pendingIn, kv.pendingOut)
	}
	if kv.shards[3] == nil || kv.shards[4] == nil {
		t.Fatalf("gain 的分片应已初始化")
	}
	cfg2 := shardmaster.Config{Num: 2, Shards: [NShards]int{}}
	for _, s := range []int{3, 4, 5} {
		cfg2.Shards[s] = 1
	}
	kv.applyNewConfig(cfg2)
	if len(kv.pendingIn) != 0 || len(kv.pendingOut) != 0 {
		t.Fatalf("第二次 gain-only 配置推进后仍不应有 pending 泄漏: in=%v out=%v", kv.pendingIn, kv.pendingOut)
	}
	// GC 路径：模拟迁移完成后清除 pendingOut（真实集群由 sendShard 触发）。
	kv.config.Shards[5] = 2
	kv.shards[5] = &ShardData{Data: map[string]string{}, LastSeq: map[int64]int64{}, LastResult: map[int64]string{}}
	kv.pendingOut[5] = true
	kv.applyGC(5)
	if kv.pendingOut[5] {
		t.Fatalf("GC 后 pendingOut[5] 应清除")
	}
	if _, ok := kv.shards[5]; ok {
		t.Fatalf("GC 后已迁出分片应删除")
	}
}

// I5：配置变更计数 + 快照可编码/往返。
func TestConfigChangeSnapshot(t *testing.T) {
	Metrics.Reset()
	kv := &ShardKV{
		gid:             1,
		config:          shardmaster.Config{Num: 0, Shards: [NShards]int{}},
		prevConfig:      shardmaster.Config{Num: 0, Shards: [NShards]int{}},
		shards:          map[int]*ShardData{},
		incoming:        map[int]*ShardData{},
		pendingIn:       map[int]bool{},
		pendingOut:      map[int]bool{},
		installedCfgNum: map[int]int{},
	}
	cfg1 := shardmaster.Config{Num: 1, Shards: [NShards]int{}}
	cfg1.Shards[3] = 1
	kv.applyNewConfig(cfg1)
	cfg2 := shardmaster.Config{Num: 2, Shards: [NShards]int{}}
	cfg2.Shards[3] = 1
	cfg2.Shards[4] = 1
	kv.applyNewConfig(cfg2)
	if got := Metrics.Counter("config_changes").Value(); got != 2 {
		t.Fatalf("config_changes 应为 2, got %d", got)
	}
	data := kv.encodeSnapshot()
	if len(data) == 0 {
		t.Fatalf("encodeSnapshot 返回空")
	}
	kv2 := &ShardKV{mu: sync.Mutex{}}
	kv2.installSnapshot(data)
	if kv2.config.Num != 2 {
		t.Fatalf("快照恢复后配置号应为 2, got %d", kv2.config.Num)
	}
}

// I9：config_num / apply_lag 等 Gauge 指标可写可读，并经 Snapshot 暴露。
func TestMetricsGauges(t *testing.T) {
	Metrics.Reset()
	kv := &ShardKV{
		gid:             1,
		config:          shardmaster.Config{Num: 6, Shards: [NShards]int{}},
		prevConfig:      shardmaster.Config{Num: 6, Shards: [NShards]int{}},
		shards:          map[int]*ShardData{},
		incoming:        map[int]*ShardData{},
		pendingIn:       map[int]bool{},
		pendingOut:      map[int]bool{},
		installedCfgNum: map[int]int{},
	}
	cfg1 := shardmaster.Config{Num: 7, Shards: [NShards]int{}}
	cfg1.Shards[3] = 1
	kv.applyNewConfig(cfg1)
	if got := Metrics.Gauge("config_num").Value(); got != 7 {
		t.Fatalf("config_num gauge 应为 7, got %v", got)
	}
	Metrics.Gauge("apply_lag").Set(3)
	if got := Metrics.Gauge("apply_lag").Value(); got != 3 {
		t.Fatalf("apply_lag gauge 应为 3, got %v", got)
	}
	snap := Metrics.Snapshot()
	gauges, ok := snap["gauges"].(map[string]float64)
	if !ok {
		t.Fatalf("Snapshot 未包含 gauges")
	}
	if gauges["config_num"] != 7 {
		t.Fatalf("Snapshot gauges 中 config_num 应为 7, got %v", gauges["config_num"])
	}
}

// I15：超大分片安装触发 shard_bytes_overflow 告警，且 shard_bytes 直方图记录样本。
func TestLargeShardMetric(t *testing.T) {
	Metrics.Reset()
	kv := &ShardKV{
		gid:             1,
		config:          shardmaster.Config{Num: 5, Shards: [NShards]int{}},
		shards:          map[int]*ShardData{},
		incoming:        map[int]*ShardData{},
		pendingIn:       map[int]bool{},
		installedCfgNum: map[int]int{},
	}
	kv.config.Shards[3] = 1
	big := &ShardData{Data: map[string]string{}, LastSeq: map[int64]int64{}, LastResult: map[int64]string{}}
	for i := 0; i < 60000; i++ {
		big.Data[fmt.Sprintf("key-%d", i)] = strings.Repeat("x", 100)
	}
	op := Op{Kind: "InstallShard", MigrateShard: 3, MigrateData: big, MigrateConfigNum: 5}
	var res applyResult
	kv.applyInstallShard(op, &res)
	if res.err != OK {
		t.Fatalf("超大分片安装失败: %v", res.err)
	}
	if Metrics.Counter("shard_bytes_overflow").Value() < 1 {
		t.Fatalf("超大分片应触发 shard_bytes_overflow 告警")
	}
	if Metrics.Histogram("shard_bytes").Snapshot().Count < 1 {
		t.Fatalf("shard_bytes 直方图应记录样本")
	}
}
