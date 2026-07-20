// shardkv.go —— Lab 4 分片容错 KV 存储
// 每个 replica group 按当前配置只服务属于它的分片集合；分片随配置变更在
// group 之间迁移（push + pull 双路，GC 在对方确认提交后才执行），并保证
// 线性一致与跨迁移的客户端幂等。
package shardkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/src/metrics"
	"raftkv/src/raft"
	"raftkv/src/shardmaster"
)

// Metrics 是本进程内 ShardKV 组件的可观测性指标（best-effort 聚合，跨 group 共享）。
// 网关 / 演示程序可读取 shardkv.Metrics.Snapshot() 展示实时吞吐、延迟与错误率。
var Metrics = metrics.NewRegistry()

// ============================== 常量与类型 ==============================

const NShards = shardmaster.NShards

type Err string

const (
	OK             Err = "OK"
	ErrWrongGroup  Err = "ErrWrongGroup" // key 所在分片不归本 group 负责（含迁移中）
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
)

// Op 是写入 Raft 日志的操作。Cmd = 客户端读写；NewConfig = 配置变更；
// InstallShard = 接收他组推送的分片数据；GCShard = 本组已迁出分片的回收。
// NotifyId 在 Start 前分配，用于唤醒等待者，避免丢失唤醒竞态。
type Op struct {
	Kind      string
	ClientId  int64
	Seq       int64
	Shard     int
	Key       string
	Value     string
	OpType    string // "Get" / "Put" / "Append"
	NotifyId  int64
	Config    shardmaster.Config
	MigrateShard     int
	MigrateData      *ShardData
	MigrateConfigNum int
	GCShard  int
}

// ShardData 是一个分片的完整状态，随分片一起迁移。
type ShardData struct {
	Data       map[string]string
	LastSeq    map[int64]int64
	LastResult map[int64]string
}

func (sd *ShardData) copy() *ShardData {
	c := &ShardData{
		Data:       map[string]string{},
		LastSeq:    map[int64]int64{},
		LastResult: map[int64]string{},
	}
	for k, v := range sd.Data {
		c.Data[k] = v
	}
	for k, v := range sd.LastSeq {
		c.LastSeq[k] = v
	}
	for k, v := range sd.LastResult {
		c.LastResult[k] = v
	}
	return c
}

// kvSnapshot 是压缩进 Raft 快照的 KV 状态。
type kvSnapshot struct {
	Shards     map[int]*ShardData
	Config     shardmaster.Config
	PrevConfig shardmaster.Config
	Incoming   map[int]*ShardData
	PendingIn  map[int]bool
	PendingOut map[int]bool
}

func init() {
	gob.Register(Op{})
	gob.Register(ShardData{})
	gob.Register(kvSnapshot{})
}

func key2shard(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % NShards)
}

// ============================== RPC 参数 ==============================

type GetArgs struct {
	Key      string
	ClientId int64
	Seq      int64
}
type GetReply struct {
	Err   Err
	Value string
}

type PutAppendArgs struct {
	Key    string
	Value  string
	OpType string // "Put" / "Append"
	ClientId int64
	Seq      int64
}
type PutAppendReply struct {
	Err Err
}

type SendShardArgs struct {
	Shard     int
	Data      *ShardData
	ConfigNum int
}
type SendShardReply struct {
	Err Err
}

type GetShardArgs struct {
	Shard int
}
type GetShardReply struct {
	Err  Err
	Data *ShardData
}

// ============================== ShardKV 结构体 ==============================

type ShardKV struct {
	mu      sync.Mutex
	gid     int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	make_end func(string) *raft.ClientEnd
	mck     *shardmaster.Clerk

	config     shardmaster.Config
	prevConfig shardmaster.Config

	shards     map[int]*ShardData // 本 group 拥有且已生效的分片
	incoming   map[int]*ShardData // 收到但尚未生效（配置尚未推进到拥有它）
	pendingIn  map[int]bool       // 需要接收（失->我）且尚未收到
	pendingOut map[int]bool       // 需要迁出（我->失）且尚未 GC
	fetchEpoch map[int]int64      // 每个分片当前拉取任务的版本号：配置推进时自增，使为同一分片服务的旧 fetcher 协程及时退出
	pendingSince time.Time       // 卡死看门狗：hasPending 首次为 true 的时间戳（零值=当前无未决迁移）

	notified map[int64]chan applyResult
	notifyId int64

	// appliedIndex 是本组状态机已应用的最后一条日志的绝对索引（与 raft 的
	// commitIndex/lastApplied 同尺度）。ReadIndex 线性一致读用它判断"是否已 apply
	// 到一致性点"——必须是一个绝对索引，因此由 applier 在每条消息处理后置为
	// msg.CommandIndex / msg.SnapshotIndex（而非简单自增计数器）。
	appliedIndex int

	dead         int32
	maxraftstate int
	killCh       chan struct{} // 关闭即通知 applier 退出（raft 不会关 applyCh，否则向其发送会 panic）
}

type applyResult struct {
	err   Err
	value string
}

// ============================== 构造 ==============================

func MakeShardKV(gid int, masters []string, make_end func(string) *raft.ClientEnd,
	rf *raft.Raft, applyCh chan raft.ApplyMsg, maxraftstate int) *ShardKV {
	kv := &ShardKV{
		gid:          gid,
		rf:           rf,
		applyCh:      applyCh,
		make_end:     make_end,
		mck:          shardmaster.MakeClerk(masters, make_end),
		config:       shardmaster.Config{Num: 0, Groups: map[int][]string{}},
		prevConfig:   shardmaster.Config{Num: 0, Groups: map[int][]string{}},
		shards:       map[int]*ShardData{},
		incoming:     map[int]*ShardData{},
		pendingIn:    map[int]bool{},
		pendingOut:   map[int]bool{},
		fetchEpoch:   map[int]int64{},
		notified:     map[int64]chan applyResult{},
		maxraftstate: maxraftstate,
		killCh:       make(chan struct{}),
	}
	// 初始：拉取最新配置（可能已有 group）
	go kv.pollConfig()
	go kv.applier()
	return kv
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// 关闭 killCh 唤醒阻塞在 <-applyCh 的 applier，使其随 cleanup 及时退出，
	// 避免每个实例泄漏一个 goroutine（测试会创建大量实例，累积泄漏会拖慢/拖垮 CI）。
	select {
	case <-kv.killCh:
	default:
		close(kv.killCh)
	}
}
func (kv *ShardKV) killed() bool { return atomic.LoadInt32(&kv.dead) == 1 }

// ============================== 配置轮询 ==============================

// pollConfig 周期性向 shardmaster 查询最新配置；只有当本组无未决迁移、
// 且最新配置比当前领先恰好一步时才推进，保证迁移串行、避免重叠。
func (kv *ShardKV) pollConfig() {
	for !kv.killed() {
		time.Sleep(80 * time.Millisecond)
		latest := kv.mck.Query(-1)
		kv.mu.Lock()
		cur := kv.config.Num
		hasPending := len(kv.pendingIn) > 0 || len(kv.pendingOut) > 0
		now := time.Now()
		if !hasPending {
			// 无未决迁移：重置看门狗计时，并允许推进到下一配置。
			kv.pendingSince = time.Time{}
		} else if kv.pendingSince.IsZero() {
			// 刚进入未决状态：记录起点。
			kv.pendingSince = now
		} else if now.Sub(kv.pendingSince) > 3*time.Second {
			// 卡死看门狗：迁移长时间（>3s）未完成，pendingIn/pendingOut 残留会冻结
			// 配置推进、令客户端读挂死（3+ group 快速 churn / ReMigration 漂移的根因）。
			// 对每个卡滞分片以最新 ShardMaster 配置重算 owner 并重拉取/重推送，bump
			// fetchEpoch 让陈旧 fetcher/sender 自退。仅卡死时触发，正常快路径零开销。
			kv.rekickStuckMigrations(latest)
			kv.pendingSince = now // 限频：每 3s 最多重踢一次
		}
		kv.mu.Unlock()
		if latest.Num <= cur {
			continue
		}
		if hasPending {
			continue // 等当前迁移完成再推进
		}
		next := kv.mck.Query(cur + 1)
		if next.Num != cur+1 {
			continue
		}
		kv.propose(Op{Kind: "NewConfig", Config: next})
	}
}

// rekickStuckMigrations 仅由 pollConfig 看门狗在迁移卡死时调用（调用方持有 kv.mu）。
// 对每个仍 pending 的分片：若最新配置下本组已是其 owner 或无主，清掉残留标记让配置
// 继续推进；否则以最新配置的 owner 重拉取（pendingIn）/重推送（pendingOut），并 bump
// 对应 epoch 使陈旧的后台协程自退。这是「配置推进快于单跳迁移」冻结的兜底自愈；同时
// 自增 config_stalls 计数，由 /metrics 暴露以便观测卡滞频率。
func (kv *ShardKV) rekickStuckMigrations(latest shardmaster.Config) {
	for s := range kv.pendingIn {
		owner := latest.Shards[s]
		if owner == kv.gid || owner == 0 {
			// 最新配置下本组已是 owner（或无主）：数据由 incoming / 后续配置装载负责，
			// 残留 pendingIn 只冻结配置推进，清掉让 pollConfig 继续。
			delete(kv.pendingIn, s)
			continue
		}
		kv.fetchEpoch[s]++
		servers := append([]string{}, latest.Groups[owner]...)
		epoch := kv.fetchEpoch[s]
		go kv.fetchShard(s, servers, epoch)
	}
	for s := range kv.pendingOut {
		newG := kv.config.Shards[s]
		if newG == kv.gid || newG == 0 {
			delete(kv.pendingOut, s)
			continue
		}
		go kv.sendShard(s, newG)
	}
	Metrics.Counter("config_stalls").Inc()
}

// ============================== 读取 RPC ==============================

// waitApplied 等待本组状态机 apply 到绝对索引 idx（ReadIndex 读路径用）。
// 期间若失去领导权或超时，返回 false，由调用方回退到常规 propose（日志追加，
// 绝对正确且线性一致）。绝不死等，避免卡住客户端。
func (kv *ShardKV) waitApplied(idx int) bool {
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		kv.mu.Lock()
		done := kv.appliedIndex >= idx
		kv.mu.Unlock()
		if done {
			return true
		}
		if _, isLeader := kv.rf.GetState(); !isLeader {
			return false
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	if _, owned := kv.shards[shard]; !owned {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	// 线性一致读优化（ReadIndex）：leader 上直接读本地状态机，省去一次日志追加。
	// 一致性点 = leader 当前 commitIndex；等待本组状态机 apply 到该索引后，本地
	// 状态即反映所有已提交写，等价于一次线性一致读。等待期间若失去领导权或超时，
	// 回退到常规 propose 路径（同样线性一致，只是多一次日志追加）。
	if idx, ok := kv.rf.ReadIndex(); ok {
		if kv.waitApplied(idx) {
			kv.mu.Lock()
			// 等待期间配置可能把分片迁走，需重新校验归属，否则读到的不是本组数据。
			if kv.config.Shards[shard] == kv.gid {
				if sd, ok2 := kv.shards[shard]; ok2 {
					val := sd.Data[args.Key]
					kv.mu.Unlock()
					reply.Err = OK
					reply.Value = val
					start := time.Now()
					Metrics.Histogram("op_latency_ms").Record(float64(time.Since(start).Microseconds()) / 1000.0)
					Metrics.Counter("ops_total").Inc()
					return
				}
			}
			kv.mu.Unlock()
		}
	}

	// 兜底：走常规 propose（日志追加，绝对正确且线性一致）。
	start := time.Now()
	op := Op{Kind: "Cmd", ClientId: args.ClientId, Seq: args.Seq, Shard: shard, Key: args.Key, OpType: "Get"}
	res := kv.propose(op)
	Metrics.Histogram("op_latency_ms").Record(float64(time.Since(start).Microseconds()) / 1000.0)
	Metrics.Counter("ops_total").Inc()
	if res.err != OK {
		Metrics.Counter("ops_errors").Inc()
	}
	reply.Err = res.err
	reply.Value = res.value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	if _, owned := kv.shards[shard]; !owned {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	start := time.Now()
	op := Op{Kind: "Cmd", ClientId: args.ClientId, Seq: args.Seq, Shard: shard,
		Key: args.Key, Value: args.Value, OpType: args.OpType}
	res := kv.propose(op)
	Metrics.Histogram("op_latency_ms").Record(float64(time.Since(start).Microseconds()) / 1000.0)
	Metrics.Counter("ops_total").Inc()
	if res.err != OK {
		Metrics.Counter("ops_errors").Inc()
	}
	reply.Err = res.err
}

// ============================== 迁移 RPC ==============================

// SendShard 由旧 owner 推送给新 owner：新 owner 在本组 Raft 提交该分片数据后回 ack，
// 旧 owner 收到 ack 才 GC——保证数据在对方提交前不丢失。
func (kv *ShardKV) SendShard(args *SendShardArgs, reply *SendShardReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{Kind: "InstallShard", MigrateShard: args.Shard, MigrateData: args.Data, MigrateConfigNum: args.ConfigNum}
	r := kv.propose(op)
	reply.Err = r.err
}

// GetShard 由新 owner 拉取：旧 owner 返回其分片数据副本（GC 前仍保留）。
// 仅 leader 响应，避免从落后 follower 拉到尚未应用客户端写操作的陈旧/空数据
// （否则迁移会把空分片装到新 group，造成数据丢失）。
func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	sd, ok := kv.shards[args.Shard]
	if !ok {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	reply.Data = sd.copy()
	kv.mu.Unlock()
	reply.Err = OK
}

// ============================== propose / applier ==============================

func (kv *ShardKV) propose(op Op) applyResult {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return applyResult{ErrWrongLeader, ""}
	}
	nid := atomic.AddInt64(&kv.notifyId, 1)
	op.NotifyId = nid
	ch := make(chan applyResult, 1)
	kv.mu.Lock()
	kv.notified[nid] = ch
	kv.mu.Unlock()

	_, _, ok := kv.rf.Start(op)
	if !ok {
		kv.mu.Lock()
		delete(kv.notified, nid)
		kv.mu.Unlock()
		return applyResult{ErrWrongLeader, ""}
	}

	select {
	case r := <-ch:
		return r
	case <-time.After(3 * time.Second):
		kv.mu.Lock()
		delete(kv.notified, nid)
		kv.mu.Unlock()
		return applyResult{ErrTimeout, ""}
	}
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		// 经 killCh 退出：raft.Kill() 不会关闭 applyCh（否则向已关闭 channel 发送
		// 会 panic），故 ShardKV 自带 killCh，避免 cleanup 后 applier 永久阻塞在
		// <-applyCh 上造成 goroutine 泄漏（每个实例泄漏一个）。
		var msg raft.ApplyMsg
		select {
		case m, ok := <-kv.applyCh:
			if !ok {
				return
			}
			msg = m
		case <-kv.killCh:
			return
		}
		if msg.SnapshotValid {
			// installSnapshot 会整体重写 kv.shards / kv.config / 迁移状态，
			// 必须与 Get / pollConfig 等持有 kv.mu 的读路径互斥，否则快照恢复
			// 与客户端读/配置推进并发会触发数据竞争（maxraftstate>0 时必现）。
			kv.mu.Lock()
			kv.installSnapshot(msg.Snapshot)
			kv.appliedIndex = msg.SnapshotIndex
			kv.mu.Unlock()
			Metrics.Counter("snapshots_installed").Inc()
			continue
		}
		if !msg.CommandValid {
			continue
		}
		Metrics.Counter("entries_applied").Inc()
		op, isOp := msg.Command.(Op)
		idx := msg.CommandIndex
		if !isOp {
			// no-op 条目（如 leader 任期开始的空命令）：也要更新 appliedIndex，
			// 保持与 raft.lastApplied 同尺度（绝对索引），否则 ReadIndex 读路径会误判。
			kv.mu.Lock()
			kv.appliedIndex = idx
			kv.mu.Unlock()
			continue
		}
		kv.mu.Lock()
		var res applyResult
		switch op.Kind {
		case "Cmd":
			kv.applyCmd(op, &res)
		case "NewConfig":
			kv.applyNewConfig(op.Config)
		case "InstallShard":
			kv.applyInstallShard(op, &res)
		case "GCShard":
			delete(kv.shards, op.GCShard)
			// 关键：GC 完成后必须清除待迁出标记，否则 hasPending 永远为 true，
			// pollConfig 无法推进配置，KV 冻结在旧配置，客户端读到空/陈旧分片而丢数据。
			delete(kv.pendingOut, op.GCShard)
		}
		nid := op.NotifyId
		ch := kv.notified[nid]
		delete(kv.notified, nid)
		kv.appliedIndex = idx
		kv.mu.Unlock()

		if ch != nil {
			ch <- res
		}
		// 快照压缩（可选）：日志超过阈值时把 KV 状态压进 Raft 快照。
		if kv.maxraftstate > 0 && kv.rf.RaftStateSize() > kv.maxraftstate {
			kv.mu.Lock()
			data := kv.encodeSnapshot()
			idx := msg.CommandIndex
			kv.mu.Unlock()
			kv.rf.Snapshot(idx, data)
			Metrics.Counter("snapshots_taken").Inc()
		}
	}
}

// reconcile 相关逻辑已在自循环 cycle 9 评估后回退：实验性「周期兜底转发 orphan
// incoming」实现会破坏 2-group 漂移测试（TestSKVReMigration 配置冻结），证明该
// 缓解路径与现有「配置变迁时处理 incoming」机制存在状态冲突，非安全修复。保留
// 根因分析于 docs/lab4-shardkv-design.md §7，待 redesign 专项修复。

func (kv *ShardKV) applyCmd(op Op, res *applyResult) {
	sd, owned := kv.shards[op.Shard]
	if !owned {
		res.err = ErrWrongGroup
		return
	}
	if last, ok := sd.LastSeq[op.ClientId]; !ok || op.Seq > last {
		switch op.OpType {
		case "Get":
			res.value = sd.Data[op.Key]
		case "Put":
			sd.Data[op.Key] = op.Value
		case "Append":
			sd.Data[op.Key] += op.Value
		}
		sd.LastSeq[op.ClientId] = op.Seq
		sd.LastResult[op.ClientId] = res.value
	} else {
		res.value = sd.LastResult[op.ClientId]
	}
	res.err = OK
}

// applyNewConfig 仅当 cfg.Num == 当前+1 时生效（串行推进保证）。
// 关键是"逐分片重新评估"：本组新拥有的分片若尚未持有则拉取；本组不再拥有的
// 分片若仍持有则推送；其余情况清除对应的待接收/待迁出标记——尤其要处理
// 分片在两组间来回漂移（A→B→A）的情形：否则 pendingOut/pendingIn 会残留为
// true，导致 pollConfig 永远认为"有未决迁移"而不再推进配置，KV 冻结在旧配置、
// 客户端按最新配置访问到空分片而丢数据。
func (kv *ShardKV) applyNewConfig(cfg shardmaster.Config) {
	if cfg.Num != kv.config.Num+1 {
		return
	}
	kv.prevConfig = kv.config
	kv.config = cfg
	for s := 0; s < NShards; s++ {
		_, have := kv.shards[s]
		if cfg.Shards[s] == kv.gid {
			// 本组现在拥有分片 s
			if oldG := kv.prevConfig.Shards[s]; oldG != kv.gid {
				if oldG == 0 {
					// 此前未分配（Config 初始全 0 = 未分配哨兵），无旧 owner，
					// 不存在需要迁移的数据，直接初始化为空的 ShardData 即可。
					kv.shards[s] = &ShardData{
						Data:       map[string]string{},
						LastSeq:    map[int64]int64{},
						LastResult: map[int64]string{},
					}
				} else if data, ok := kv.incoming[s]; ok {
					if _, exists := kv.shards[s]; exists {
						// 本组在配置推进前已直接写入该分片：合并而非覆盖，
						// 否则会把新 owner 的本地写冲掉（测试见 c0_k3 丢失）。
						kv.mergeShardData(s, data)
					} else {
						kv.shards[s] = data
					}
					delete(kv.incoming, s)
				} else if !have {
					// 既未持有也未在 incoming 中缓冲：从上一版配置记录的旧 owner 拉取。
					kv.pendingIn[s] = true
					oldServers := append([]string{}, kv.prevConfig.Groups[oldG]...)
					kv.fetchEpoch[s]++
					epoch := kv.fetchEpoch[s]
					go kv.fetchShard(s, oldServers, epoch)
				}
				// have==true：分片已在上一轮迁移中到位，无需动作。
			}
			delete(kv.pendingOut, s) // 本组现在拥有 s，不再迁出它
		} else {
			// 本组现在不拥有分片 s
			if have {
				// 仍持有 s，需推给新 owner。
				kv.pendingOut[s] = true
				go kv.sendShard(s, cfg.Shards[s])
			} else if data, ok := kv.incoming[s]; ok {
				// 已从旧 owner 拉到该分片数据（缓冲在 incoming），但配置尚未推进到
				// "本组拥有它"就又被迁走了。数据真实有效，先安装成本组数据再随迁出推送，
				// 绝不可丢弃——否则在配置推进快于迁移完成的抖动下会丢数据。
				// 若本组已直接写入该分片（配置滞后窗口内的客户端写），则合并而非覆盖。
				if _, exists := kv.shards[s]; exists {
					kv.mergeShardData(s, data)
				} else {
					kv.shards[s] = data
				}
				delete(kv.incoming, s)
				kv.pendingOut[s] = true
				go kv.sendShard(s, cfg.Shards[s])
			} else {
				// 既不拥有也不持有：取消待接收（配置已变，不再需要该分片）。
				delete(kv.pendingIn, s)
			}
		}
	}
}

func (kv *ShardKV) applyInstallShard(op Op, res *applyResult) {
	s := op.MigrateShard
	// 关键并发安全修复：必须深拷贝迁移来的 ShardData 再写入本组状态，绝不能直接存
	// op.MigrateData 指针。原因：op.MigrateData 的指针同时被存进了本组 Raft 日志
	// （InstallShard 的 Op 内）；rf.Start 会立即 rf.persist()，而 persist() 对整条日志
	// 做 gob 编码时会读取该 ShardData 的 Data/LastSeq/LastResult 映射。若把同一指针
	// 直接放入 kv.shards，则本组 applier 后续对该分片的客户端写（applyCmd / mergeShardData
	// 改写这些映射）会与 Raft persist 的 gob 编码并发读写同一份映射，触发
	// "concurrent map read and map write"（TestSKVConcurrent 在高频 churn + 并发写下必现）。
	// 深拷贝后，日志里的副本与运行态副本是两份独立对象，互不干扰，竞态彻底消除。
	incoming := op.MigrateData.copy()
	if _, exists := kv.shards[s]; exists {
		// 已拥有该分片：必须"合并"而非"覆盖"。
		// 新 owner 在迁移期间可能已经直接接收了客户端写（旧 owner 的快照里没有这些写），
		// 若直接覆盖会把本组已写入的数据冲掉，造成迁移丢数据。合并只补充旧 owner 快照中
		// 多出来的 key，并取较大的 LastSeq/LastResult 以保住本组的较新写与幂等基线。
		kv.mergeShardData(s, incoming)
		if res != nil {
			res.err = OK
		}
		return // 幂等：已拥有
	}
	if kv.config.Shards[s] == kv.gid {
		kv.shards[s] = incoming
		delete(kv.pendingIn, s)
	} else {
		// 配置尚未推进到"拥有该分片"，先缓冲，待 NewConfig 生效时安装。
		kv.incoming[s] = incoming
	}
	if res != nil {
		res.err = OK
	}
}

// mergeShardData 将迁移来的分片数据合并进本组已持有的分片：
// 只补充本组缺失的 key，保留本组已有的（通常更新的）value；LastSeq/LastResult 取较大者，
// 从而既保住迁移来源的数据，又不冲掉本组在迁移窗口内已直接接收的客户端写。
func (kv *ShardKV) mergeShardData(s int, incoming *ShardData) {
	sd := kv.shards[s]
	if sd == nil || incoming == nil {
		return
	}
	for k, v := range incoming.Data {
		if _, ok := sd.Data[k]; !ok {
			sd.Data[k] = v
		}
	}
	for cid, seq := range incoming.LastSeq {
		if cur, ok := sd.LastSeq[cid]; !ok || seq > cur {
			sd.LastSeq[cid] = seq
			if r, ok2 := incoming.LastResult[cid]; ok2 {
				sd.LastResult[cid] = r
			}
		}
	}
}

func (kv *ShardKV) fetchShard(s int, oldServers []string, epoch int64) {
	for !kv.killed() {
		kv.mu.Lock()
		if !kv.pendingIn[s] {
			kv.mu.Unlock()
			return
		}
		// 若有更新的拉取任务（配置已再次推进）取代本协程，立即退出：避免多个
		// fetcher 为同一分片「抢来源」而彼此干扰——3-group 快速 churn 下这正是
		// pendingIn 冻结、配置停滞的根因之一（旧 fetcher 用过期来源空转，新
		// 配置所需的拉取被拖死）。
		if kv.fetchEpoch[s] != epoch {
			kv.mu.Unlock()
			return
		}
		// 每次循环重新计算「上一版配置」的 owner：配置推进后 prevConfig 会随之
		// 更新，本组需要的数据始终由「当前 prevConfig 的 owner」提供。
		oldG := kv.prevConfig.Shards[s]
		kv.mu.Unlock()
		if oldG == kv.gid || oldG == 0 {
			// prevConfig 已不含该分片的旧 owner：无需拉取。若当前配置下本组就是
			// owner，残留的 pendingIn 只会冻结配置推进，清掉让 pollConfig 继续
			// （分片数据由 incoming 或后续配置装载负责）。否则同样清除以免冻结。
			kv.mu.Lock()
			delete(kv.pendingIn, s)
			kv.mu.Unlock()
			return
		}
		got := false
		for _, srv := range oldServers {
			end := kv.make_end(srv)
			reply := &GetShardReply{}
			if end.Call("ShardKV.GetShard", &GetShardArgs{Shard: s}, reply) && reply.Err == OK && reply.Data != nil {
				kv.propose(Op{Kind: "InstallShard", MigrateShard: s, MigrateData: reply.Data})
				got = true
				break
			}
		}
		if !got {
			// 自愈回源（cycle 27/33，本轮增强）：记录的旧 owner 不可达（快速再平衡下可能已
			// 因更新的配置把该分片 GC 掉并转发给更靠后的 group），改回源 ShardMaster 取「最新」
			// 配置的 owner 重新拉取——比仅查 prevConfig.Num 更 live，可跨多跳迁移链找回数据；
			// 若最新配置下本组已是 owner 或无主，清掉残留 pendingIn 让配置推进（否则 have==false
			// 且 pendingIn 残留会永久卡死）。
			if cur := kv.mck.Query(-1); cur.Num > 0 {
				curOwner := cur.Shards[s]
				if curOwner != kv.gid && curOwner != 0 {
					for _, srv := range cur.Groups[curOwner] {
						end := kv.make_end(srv)
						reply := &GetShardReply{}
						if end.Call("ShardKV.GetShard", &GetShardArgs{Shard: s}, reply) && reply.Err == OK && reply.Data != nil {
							kv.propose(Op{Kind: "InstallShard", MigrateShard: s, MigrateData: reply.Data})
							got = true
							break
						}
					}
				} else if curOwner == kv.gid {
					kv.mu.Lock()
					delete(kv.pendingIn, s)
					kv.mu.Unlock()
					return
				}
			}
		}
		if got {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) sendShard(s, newG int) {
	for !kv.killed() {
		kv.mu.Lock()
		if !kv.pendingOut[s] {
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		// 关键正确性约束：仅由本组 LEADER 推送分片。
		// follower 的状态可能落后于已提交的客户端写操作；若由 follower 发送，
		// 会把"陈旧/空"的分片数据装到新 group，造成迁移丢数据。leader 的状态
		// 始终包含所有已提交条目（Raft 保证），是权威数据源。新 group 的
		// SendShard 也只接受其自身 leader 的提交，因此推送方必须是 leader 才能
		// 保证数据完整。非 leader 时只休眠等待，不发送——一旦本副本成为 leader，
		// 其自身早已在 applyNewConfig 中拉起本 goroutine，会自动接管推送。
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		kv.mu.Lock()
		cfg := kv.config
		if cfg.Shards[s] != newG { // 分片归属又变了，旧协程退出（新配置会重新发起）
			kv.mu.Unlock()
			return
		}
		data := kv.shards[s]
		if data == nil {
			// 我们其实并没有这个分片（迁移未完成或被覆盖），无法发送。
			// 放弃本次迁出并清除标记，避免 pendingOut 残留导致配置停滞。
			delete(kv.pendingOut, s)
			kv.mu.Unlock()
			return
		}
		data = data.copy()
		kv.mu.Unlock()

		servers := cfg.Groups[newG]
		if len(servers) == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		sent := false
		for _, srv := range servers {
			end := kv.make_end(srv)
			reply := &SendShardReply{}
			if end.Call("ShardKV.SendShard", &SendShardArgs{Shard: s, Data: data, ConfigNum: cfg.Num}, reply) && reply.Err == OK {
				sent = true
				break
			}
		}
		if sent {
			// 对方已提交该分片数据，本组可安全回收。
			kv.propose(Op{Kind: "GCShard", GCShard: s})
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ============================== 快照 ==============================

func (kv *ShardKV) encodeSnapshot() []byte {
	snap := kvSnapshot{
		Shards:     map[int]*ShardData{},
		Config:     kv.config,
		PrevConfig: kv.prevConfig,
		Incoming:   map[int]*ShardData{},
		PendingIn:  map[int]bool{},
		PendingOut: map[int]bool{},
	}
	for k, v := range kv.shards {
		snap.Shards[k] = v.copy()
	}
	for k, v := range kv.incoming {
		snap.Incoming[k] = v.copy()
	}
	for k, v := range kv.pendingIn {
		snap.PendingIn[k] = v
	}
	for k, v := range kv.pendingOut {
		snap.PendingOut[k] = v
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(snap)
	return buf.Bytes()
}

func (kv *ShardKV) installSnapshot(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	snap := kvSnapshot{}
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&snap); err != nil {
		return
	}
	// 注意：本函数仅由 applier 在持有 kv.mu 的情况下调用（见 applier 中对
	// SnapshotValid 分支的处理），因此此处【不再】加锁，否则会与调用方的锁形成
	// 嵌套加锁（sync.Mutex 不可重入）导致死锁——这在 maxraftstate>0（会真正触发
	// 快照恢复）时必现。调用方负责保证互斥。
	kv.shards = map[int]*ShardData{}
	for k, v := range snap.Shards {
		kv.shards[k] = v
	}
	kv.incoming = map[int]*ShardData{}
	for k, v := range snap.Incoming {
		kv.incoming[k] = v
	}
	kv.pendingIn = map[int]bool{}
	for k, v := range snap.PendingIn {
		kv.pendingIn[k] = v
	}
	kv.pendingOut = map[int]bool{}
	for k, v := range snap.PendingOut {
		kv.pendingOut[k] = v
	}
	kv.config = snap.Config
	kv.prevConfig = snap.PrevConfig
}

// ============================== Clerk（客户端） ==============================

type Clerk struct {
	mu       sync.Mutex
	config   shardmaster.Config
	mck      *shardmaster.Clerk
	make_end func(string) *raft.ClientEnd
	clientId int64
	seq      int64
}

func MakeClerk(masters []string, make_end func(string) *raft.ClientEnd) *Clerk {
	return &Clerk{
		mck:      shardmaster.MakeClerk(masters, make_end),
		make_end: make_end,
		clientId: nrand(),
		seq:      0,
	}
}

func (ck *Clerk) refresh() {
	c := ck.mck.Query(-1)
	ck.mu.Lock()
	ck.config = c
	ck.mu.Unlock()
}

func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	ck.seq++
	seq := ck.seq
	ck.mu.Unlock()
	shard := key2shard(key)
	for {
		ck.refresh()
		// 在锁内捕获配置快照：refresh() 在 ck.mu 下写入 ck.config，若不持锁读取，
		// 会与另一 goroutine 的 refresh() 形成 struct/map 的并发读写竞态
		// （Config 内含 Groups map，本机无 -race 时不崩，但 GitHub CI 的 -race
		// 会报 data race）。捕获的是提交时刻的不可变快照（底层 map 不会被原地改写），
		// 故下面的 cfg.Groups/cfg.Shards 读取是安全的。
		ck.mu.Lock()
		cfg := ck.config
		ck.mu.Unlock()
		if cfg.Num == 0 {
			continue
		}
		gid := cfg.Shards[shard]
		servers := cfg.Groups[gid]
		if len(servers) == 0 {
			continue
		}
		ok := false
		for _, srv := range servers {
			end := ck.make_end(srv)
			reply := &GetReply{}
			if ck.callWithTimeout(end, "ShardKV.Get", &GetArgs{Key: key, ClientId: ck.clientId, Seq: seq}, reply) {
				if reply.Err == OK {
					return reply.Value
				}
				if reply.Err == ErrWrongGroup {
					ok = true
					break
				}
			}
		}
		if ok {
			continue // 分片已迁走，重新查配置
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) PutAppend(key, value, opType string) {
	ck.mu.Lock()
	ck.seq++
	seq := ck.seq
	ck.mu.Unlock()
	shard := key2shard(key)
	for {
		ck.refresh()
		// 在锁内捕获配置快照：refresh() 在 ck.mu 下写入 ck.config，若不持锁读取，
		// 会与另一 goroutine 的 refresh() 形成 struct/map 的并发读写竞态
		// （Config 内含 Groups map，本机无 -race 时不崩，但 GitHub CI 的 -race
		// 会报 data race）。捕获的是提交时刻的不可变快照（底层 map 不会被原地改写），
		// 故下面的 cfg.Groups/cfg.Shards 读取是安全的。
		ck.mu.Lock()
		cfg := ck.config
		ck.mu.Unlock()
		if cfg.Num == 0 {
			continue
		}
		gid := cfg.Shards[shard]
		servers := cfg.Groups[gid]
		if len(servers) == 0 {
			continue
		}
		ok := false
		for _, srv := range servers {
			end := ck.make_end(srv)
			reply := &PutAppendReply{}
			if ck.callWithTimeout(end, "ShardKV.PutAppend", &PutAppendArgs{Key: key, Value: value, OpType: opType, ClientId: ck.clientId, Seq: seq}, reply) {
				if reply.Err == OK {
					return
				}
				if reply.Err == ErrWrongGroup {
					ok = true
					break
				}
			}
		}
		if ok {
			continue
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key, value string)   { ck.PutAppend(key, value, "Put") }
func (ck *Clerk) Append(key, value string) { ck.PutAppend(key, value, "Append") }

// clerkBoundedRetries 是有界重试窗口：GetE/PutE/AppendE 在此窗口内重试，超时即
// 返回最后的错误（而非无限阻塞）。供网关把错误映射成 HTTP 状态码——集群不可达时
// 网关快速失败（5xx）而非挂起，否则遇到 3-group 再平衡冻结会令 HTTP 请求永久挂死。
const clerkBoundedRetries = 5 * time.Second

// GetE 是 Get 的有界重试版本：成功返回 (value, OK)；窗口内始终未成功则返回最后的
// Err（通常是 ErrWrongLeader/ErrTimeout）。ErrWrongGroup 视为"分片已迁走"，会继续
// 重新查配置重试，不会当成终态错误。
func (ck *Clerk) GetE(key string) (string, Err) {
	ck.mu.Lock()
	ck.seq++
	seq := ck.seq
	ck.mu.Unlock()
	shard := key2shard(key)
	deadline := time.Now().Add(clerkBoundedRetries)
	for {
		if time.Now().After(deadline) {
			return "", ErrTimeout
		}
		ck.refresh()
		ck.mu.Lock()
		cfg := ck.config
		ck.mu.Unlock()
		if cfg.Num == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		gid := cfg.Shards[shard]
		servers := cfg.Groups[gid]
		if len(servers) == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		wrongGroup := false
		for _, srv := range servers {
			end := ck.make_end(srv)
			reply := &GetReply{}
			if ck.callWithTimeout(end, "ShardKV.Get", &GetArgs{Key: key, ClientId: ck.clientId, Seq: seq}, reply) {
				if reply.Err == OK {
					return reply.Value, OK
				}
				if reply.Err == ErrWrongGroup {
					wrongGroup = true
					break
				}
			}
		}
		if wrongGroup {
			continue // 分片已迁走，重新查配置
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// PutE 是 Put 的有界重试版本，语义同 GetE。
func (ck *Clerk) PutE(key, value string) Err {
	return ck.putAppendE(key, value, "Put")
}

// AppendE 是 Append 的有界重试版本，语义同 GetE。
func (ck *Clerk) AppendE(key, value string) Err {
	return ck.putAppendE(key, value, "Append")
}

func (ck *Clerk) putAppendE(key, value, opType string) Err {
	ck.mu.Lock()
	ck.seq++
	seq := ck.seq
	ck.mu.Unlock()
	shard := key2shard(key)
	deadline := time.Now().Add(clerkBoundedRetries)
	for {
		if time.Now().After(deadline) {
			return ErrTimeout
		}
		ck.refresh()
		ck.mu.Lock()
		cfg := ck.config
		ck.mu.Unlock()
		if cfg.Num == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		gid := cfg.Shards[shard]
		servers := cfg.Groups[gid]
		if len(servers) == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		wrongGroup := false
		for _, srv := range servers {
			end := ck.make_end(srv)
			reply := &PutAppendReply{}
			if ck.callWithTimeout(end, "ShardKV.PutAppend", &PutAppendArgs{Key: key, Value: value, OpType: opType, ClientId: ck.clientId, Seq: seq}, reply) {
				if reply.Err == OK {
					return OK
				}
				if reply.Err == ErrWrongGroup {
					wrongGroup = true
					break
				}
			}
		}
		if wrongGroup {
			continue
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// callWithTimeout 在单 RPC 上施加超时，避免副本网络层挂起拖死客户端
// （交接文档 §6 的开放问题：当前无超时，副本网络层挂起会拖慢客户端）。
// 超时即视为不可达，交由上层重试循环处理；safe-by-construction：
// 仅当 goroutine 通过 done channel 返回 ok 时才读取 reply（channel 收发建立
// happens-before，reply 写入对主协程可见），超时分支不读 reply，无数据竞争。
func (ck *Clerk) callWithTimeout(end *raft.ClientEnd, method string, args, reply interface{}) bool {
	const timeout = 1 * time.Second
	done := make(chan bool, 1)
	go func() {
		done <- end.Call(method, args, reply)
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(timeout):
		return false
	}
}

func nrand() int64 {
	return rand.Int63()
}

// ConfigNum 返回本副本当前生效的配置版本号（供 cluster 包等外部调用者读取，
// 避免暴露未导出的 config 字段）。
func (kv *ShardKV) ConfigNum() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Num
}

// DebugState 返回本副本的迁移/配置状态，供测试 watchdog 诊断卡死原因。
func (kv *ShardKV) DebugState() string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	owned := []int{}
	for s := range kv.shards {
		owned = append(owned, s)
	}
	incoming := []int{}
	for s := range kv.incoming {
		incoming = append(incoming, s)
	}
	pendIn := []int{}
	for s := range kv.pendingIn {
		pendIn = append(pendIn, s)
	}
	pendOut := []int{}
	for s := range kv.pendingOut {
		pendOut = append(pendOut, s)
	}
	return fmt.Sprintf("gid=%d leader=%v configNum=%d owned=%v incoming=%v pendingIn=%v pendingOut=%v",
		kv.gid, isLeader, kv.config.Num, owned, incoming, pendIn, pendOut)
}

// ShardDebug 是 DebugState 的机器可读版本：返回本副本迁移/配置状态的结构化副本，
// 供网关 /debug/shards 等观测端点直接 JSON 序列化（无需解析 DebugState 的文本）。
type ShardDebug struct {
	GID        int
	Leader     bool
	ConfigNum  int
	Owned      []int
	Incoming   []int
	PendingIn  []int
	PendingOut []int
}

func (kv *ShardKV) ShardDebug() ShardDebug {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	owned := []int{}
	for s := range kv.shards {
		owned = append(owned, s)
	}
	incoming := []int{}
	for s := range kv.incoming {
		incoming = append(incoming, s)
	}
	pendIn := []int{}
	for s := range kv.pendingIn {
		pendIn = append(pendIn, s)
	}
	pendOut := []int{}
	for s := range kv.pendingOut {
		pendOut = append(pendOut, s)
	}
	return ShardDebug{
		GID:        kv.gid,
		Leader:     isLeader,
		ConfigNum:  kv.config.Num,
		Owned:      owned,
		Incoming:   incoming,
		PendingIn:  pendIn,
		PendingOut: pendOut,
	}
}
