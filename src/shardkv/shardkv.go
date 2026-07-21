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
	Kind             string
	ClientId         int64
	Seq              int64
	Shard            int
	Key              string
	Value            string
	OpType           string // "Get" / "Put" / "Append"
	NotifyId         int64
	Config           shardmaster.Config
	MigrateShard     int
	MigrateData      *ShardData
	MigrateConfigNum int
	GCShard          int
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

// shardVersion 返回分片状态的逻辑版本：所有 client LastSeq 的最大值（无 client 时为 0）。
// 用作迁移传输的二次判据——同源在「同一迁移配置号」下可能发出数据不同的两份传输
// （源端在两次发送间又提交了本组未见过的写），版本更高者才是更完整的快照。
func shardVersion(sd *ShardData) int64 {
	if sd == nil {
		return 0
	}
	var v int64
	for _, seq := range sd.LastSeq {
		if seq > v {
			v = seq
		}
	}
	return v
}

// kvSnapshot 是压缩进 Raft 快照的 KV 状态。
type kvSnapshot struct {
	Shards     map[int]*ShardData
	Config     shardmaster.Config
	PrevConfig shardmaster.Config
	Incoming   map[int]*ShardData
	PendingIn  map[int]bool
	PendingOut map[int]bool
	Installed  map[int]int // 各分片已安装时的配置号（I2 幂等去重，随快照恢复）
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
	Key      string
	Value    string
	OpType   string // "Put" / "Append"
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
	Err    Err
	Data   *ShardData
	Config int // 来源当前生效配置号：拉取端据此判断来源是否已"安定"
}

// ============================== ShardKV 结构体 ==============================

type ShardKV struct {
	mu       sync.Mutex
	gid      int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *raft.ClientEnd
	mck      *shardmaster.Clerk

	config     shardmaster.Config
	prevConfig shardmaster.Config

	shards          map[int]*ShardData // 本 group 拥有且已生效的分片
	incoming        map[int]*ShardData // 收到但尚未生效（配置尚未推进到拥有它）
	pendingIn       map[int]bool       // 需要接收（失->我）且尚未收到
	pendingOut      map[int]bool       // 需要迁出（我->失）且尚未 GC
	fetchEpoch      map[int]int64      // 每个分片当前拉取任务的版本号：配置推进时自增，使为同一分片服务的旧 fetcher 协程及时退出
	pendingInSince  map[int]time.Time  // 卡死看门狗：每个 pendingIn 分片首次进入未决态的时间戳（用于 /debug/shards 暴露卡滞时长 + 驱动 rekick）
	pendingOutSince map[int]time.Time  // 同上，针对 pendingOut

	// 迁移幂等 / 配置推进去重辅助状态。
	installedCfgNum   map[int]int // 每个分片「已安装/已拥有」时对应的配置号，InstallShard 幂等去重依据（I2）
	proposedConfigNum int         // 本组已向 Raft 提议的最高配置号，pollConfig 去重、避免重复写日志（I8）

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
		gid:             gid,
		rf:              rf,
		applyCh:         applyCh,
		make_end:        make_end,
		mck:             shardmaster.MakeClerk(masters, make_end),
		config:          shardmaster.Config{Num: 0, Groups: map[int][]string{}},
		prevConfig:      shardmaster.Config{Num: 0, Groups: map[int][]string{}},
		shards:          map[int]*ShardData{},
		incoming:        map[int]*ShardData{},
		pendingIn:       map[int]bool{},
		pendingOut:      map[int]bool{},
		pendingInSince:  map[int]time.Time{},
		pendingOutSince: map[int]time.Time{},
		installedCfgNum: map[int]int{},
		fetchEpoch:      map[int]int64{},
		notified:        map[int64]chan applyResult{},
		maxraftstate:    maxraftstate,
		killCh:          make(chan struct{}),
	}
	// 初始：拉取最新配置（可能已有 group）
	go kv.pollConfig()
	go kv.migratePump()
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
		// 配置推进仅以 pendingIn（本组待收分片）门控：收到数据前本组无法正确服务该
		// 分片，必须先等；pendingOut（本组待迁出、等待 GC）不再阻塞推进——旧 owner 在
		// 配置推进后将不再"拥有"该分片（applyCmd 返回 ErrWrongGroup），不会错误服务，
		// 迁出/GC 由后台 sendShard 完成即可。这消除了 3-group churn 下"发送侧
		// pendingOut 与接收侧 pendingIn 互相卡住"的两侧死锁（cycle 48 根因修复）。
		blockAdvance := len(kv.pendingIn) > 0
		now := time.Now()
		if !hasPending {
			// 无未决迁移：重置看门狗计时，并允许推进到下一配置。
			kv.pendingInSince = map[int]time.Time{}
			kv.pendingOutSince = map[int]time.Time{}
		} else {
			// 维护每个 pending 分片首次进入未决态的时间戳，并清理已不再 pending 的记录。
			for s := range kv.pendingIn {
				if _, ok := kv.pendingInSince[s]; !ok {
					kv.pendingInSince[s] = now
				}
			}
			for s := range kv.pendingOut {
				if _, ok := kv.pendingOutSince[s]; !ok {
					kv.pendingOutSince[s] = now
				}
			}
			for s := range kv.pendingInSince {
				if _, ok := kv.pendingIn[s]; !ok {
					delete(kv.pendingInSince, s)
				}
			}
			for s := range kv.pendingOutSince {
				if _, ok := kv.pendingOut[s]; !ok {
					delete(kv.pendingOutSince, s)
				}
			}
			// 卡死检测：任一 pending 分片卡 >3s 即触发看门狗。
			stalled := false
			for s := range kv.pendingIn {
				if t, ok := kv.pendingInSince[s]; ok && now.Sub(t) > 3*time.Second {
					stalled = true
					break
				}
			}
			if !stalled {
				for s := range kv.pendingOut {
					if t, ok := kv.pendingOutSince[s]; ok && now.Sub(t) > 3*time.Second {
						stalled = true
						break
					}
				}
			}
			if stalled {
				// 卡死看门狗：迁移长时间（>3s）未完成，pendingIn/pendingOut 残留会冻结
				// 配置推进、令客户端读挂死（3+ group 快速 churn / ReMigration 漂移的根因）。
				// 对每个卡滞分片以最新 ShardMaster 配置重算 owner 并重拉取/重推送，bump
				// fetchEpoch 让陈旧 fetcher/sender 自退。仅卡死时触发，正常快路径零开销。
				kv.rekickStuckMigrations(latest)
				// 重置计时，限频：每 3s 最多重踢一次。
				for s := range kv.pendingInSince {
					kv.pendingInSince[s] = now
				}
				for s := range kv.pendingOutSince {
					kv.pendingOutSince[s] = now
				}
			}
		}
		kv.mu.Unlock()
		if latest.Num <= cur {
			continue
		}
		if blockAdvance {
			continue // 本组仍有待收分片，先完成迁移再推进配置
		}
		next := kv.mck.Query(cur + 1)
		if next.Num != cur+1 {
			continue
		}
		// I8：去重——同一配置号只向 Raft 提议一次；提议成功（已提交应用）后
		// applyNewConfig 会更新 proposedConfigNum，否则保持原值以便下次重试。
		if next.Num <= kv.proposedConfigNum {
			continue
		}
		res := kv.propose(Op{Kind: "NewConfig", Config: next})
		if res.err == OK {
			kv.proposedConfigNum = next.Num
		}
	}
}

// migratePump 是迁移的「保活泵」：周期性扫描 pendingIn/pendingOut，对长时间无进展的
// 分片重新拉起 fetchShard/sendShard（bump fetchEpoch 让陈旧协程自退），与 applyNewConfig
// 在配置推进时的一次性拉起互补——泵兜底处理「拉取协程意外退出 / 源瞬时不可达 /
// 配置推进快于单跳迁移」导致的残留 pending，避免依赖单一触发点。限频（每分片每 500ms
// 最多重拉一次）以避免 goroutine 风暴（cycle 48 健壮性增强）。
func (kv *ShardKV) migratePump() {
	for !kv.killed() {
		time.Sleep(150 * time.Millisecond)
		kv.mu.Lock()
		// 收方已拥有分片 s、但数据迟到在 incoming（applyNewConfig 已先行跑过、未消费到）
		// 时，直接消费 incoming → 清 pendingIn，解除「配置推进被 pendingIn 阻塞」的冻结。
		// 仅作用于「本组拥有 s」的情形、不前向转发——避免与双向漂移的 pendingIn 状态冲突
		// （cycle 55 教训：独立 goroutine 改写 shards/pendingIn 会清掉合法待拉取标记）。
		// 数据本就由旧 owner 通过 InstallShard 推到本组 incoming（旧 owner 在收到 ACK 后
		// 才 GC，故数据不会丢），只是 applyNewConfig 的临界区没赶上，泵在此补消费即可。
		for s := range kv.pendingIn {
			if kv.config.Shards[s] == kv.gid {
				if data, ok := kv.incoming[s]; ok {
					if _, exists := kv.shards[s]; exists {
						kv.mergeShardData(s, data)
					} else {
						kv.shards[s] = data
					}
					delete(kv.incoming, s)
					delete(kv.pendingIn, s)
				}
			}
		}
		now := time.Now()
		for s := range kv.pendingIn {
			if t, ok := kv.pendingInSince[s]; !ok || now.Sub(t) > 500*time.Millisecond {
				kv.pendingInSince[s] = now
				oldG := kv.prevConfig.Shards[s]
				if oldG == kv.gid || oldG == 0 {
					// 上一版配置已无旧 owner：尝试最新配置 owner 兜底（多跳迁移链找回）。
					if cur := kv.mck.Query(-1); cur.Num > 0 {
						if co := cur.Shards[s]; co != kv.gid && co != 0 {
							kv.fetchEpoch[s]++
							epoch := kv.fetchEpoch[s]
							servers := append([]string{}, cur.Groups[co]...)
							kv.mu.Unlock()
							go kv.fetchShard(s, servers, epoch)
							kv.mu.Lock()
						}
					}
					continue
				}
				kv.fetchEpoch[s]++
				epoch := kv.fetchEpoch[s]
				servers := append([]string{}, kv.prevConfig.Groups[oldG]...)
				kv.mu.Unlock()
				go kv.fetchShard(s, servers, epoch)
				kv.mu.Lock()
			}
		}
		for s := range kv.pendingOut {
			newG := kv.config.Shards[s]
			if newG == kv.gid || newG == 0 {
				// 本组当前仍拥有该分片（配置回摆 A→B→A）或无主：残留 pendingOut 无意义，
				// 直接清除，避免向本组自身发 SendShard 触发自 GC（即便有 GC 守卫也不丢数据，
				// 但省掉无谓自环 RPC）。与 rekickStuckMigrations 的 pendingOut 分支一致。
				delete(kv.pendingOut, s)
				delete(kv.pendingOutSince, s)
				continue
			}
			if t, ok := kv.pendingOutSince[s]; !ok || now.Sub(t) > 500*time.Millisecond {
				kv.pendingOutSince[s] = now
				kv.mu.Unlock()
				go kv.sendShard(s, newG)
				kv.mu.Lock()
			}
		}
		kv.mu.Unlock()
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
			delete(kv.pendingInSince, s)
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
			delete(kv.pendingOutSince, s)
			continue
		}
		go kv.sendShard(s, newG)
	}
	Metrics.Counter("config_stalls").Inc()
}

// ============================== 读取 RPC ==============================

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

	// 线性一致读优化（ReadIndex 快路径）：仅当本副本是 leader 且持有合法 leader
	// 租约（HasLeaderLease，多数派近期有心跳接触）时，才跳过日志追加、直接读本地
	// 状态机。租约保证该 leader 期间无更新任期产生，commitIndex 不会被回滚，故等待
	// 本组 apply 到 commitIndex 后本地读是线性一致的。租约失效（分区/刚上任）或分片
	// 尚未生效时，一律回退 propose 路径，绝不牺牲正确性。
	if kv.rf.HasLeaderLease() {
		if commitIdx, isLeader := kv.rf.ReadIndex(); isLeader {
			if kv.waitApplied(commitIdx, 3*time.Second) {
				kv.mu.Lock()
				if sd, ok := kv.shards[shard]; ok {
					v := sd.Data[args.Key]
					kv.mu.Unlock()
					Metrics.Counter("read_leases").Inc()
					Metrics.Counter("ops_total").Inc()
					reply.Err = OK
					reply.Value = v
					return
				}
				kv.mu.Unlock()
			}
		}
	}

	// 回退路径：始终走 propose（经 Raft 共识，绝对线性一致）。
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

// waitApplied 轮询等待本组状态机已应用到索引 idx（ReadIndex 快路径用），
// 超时返回 false。仅用于租约有效的 leader 本地读前，确保读到 commitIndex 对应的
// 已提交状态。appliedIndex 的读写均在 kv.mu 保护下进行（applier 在持锁时写入）。
func (kv *ShardKV) waitApplied(idx int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		kv.mu.Lock()
		applied := kv.appliedIndex
		kv.mu.Unlock()
		if applied >= idx {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(2 * time.Millisecond)
	}
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
//
// 关键正确性守卫（选举抖动窗口丢写修复）：新当选 leader 在「重新提交本任期
// no-op」之前，其 commitIndex 可能仍落后上一任 leader 已提交的位置，旧任期已提交
// 写尚未 apply。若此时直接传出 kv.shards，快照会缺失该写、被新 group 装入后即静
// 默丢写（TestChaosSwingWriteDataLoss 在杀主+迁移重叠窗口偶发复现）。因此必须：
//  1) 持有 leader 租约（多数派近期有心跳接触，非分区旧主）；
//  2) 已在当前任期提交过条目（HasCommittedCurrentTerm：no-op 已提交，旧任期写
//     现已被 commitIndex 覆盖）；
//  3) 等待本组状态机 apply 到 commitIndex（waitApplied），确保传出的快照包含
//     全部已提交写。任一不满足即返回 ErrWrongLeader，fetchShard 会重试。
func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 守卫 1+2：持有租约且已提交当前任期条目。
	if !kv.rf.HasLeaderLease() || !kv.rf.HasCommittedCurrentTerm() {
		reply.Err = ErrWrongLeader
		return
	}
	// 守卫 3：读到当前 commitIndex，并等待状态机 apply 到该点，避免传出
	// 尚未应用旧任期写的陈旧快照。
	commitIdx, stillLeader := kv.rf.ReadIndex()
	if !stillLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if !kv.waitApplied(commitIdx, 3*time.Second) {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	data, err := kv.resolveShardForTransfer(args.Shard)
	if err != OK {
		kv.mu.Unlock()
		reply.Err = err
		return
	}
	reply.Data = data.copy()
	reply.Config = kv.config.Num
	kv.mu.Unlock()
	reply.Err = OK
}

// resolveShardForTransfer 返回某分片供迁移传输的数据：优先本组已生效的 shards[s]，
// 否则回退到缓冲的 incoming[s]（多跳 rebalance 下孤儿 incoming 仍含有效数据，新 owner
// 拉走后即安装）。本组既不拥有也不缓冲该分片则 ErrWrongGroup。调用方必须持有 kv.mu。
// 这是 3-group / 多跳 churn 冻结根因的修复核心：若只服务 shards，真正主人 C 向中间组
// B 拉取迟到推送时永远 ErrWrongGroup，pendingIn[s] 清不掉、配置冻结。
func (kv *ShardKV) resolveShardForTransfer(s int) (*ShardData, Err) {
	if sd, ok := kv.shards[s]; ok {
		return sd, OK
	}
	if data, ok2 := kv.incoming[s]; ok2 {
		return data, OK
	}
	return nil, ErrWrongGroup
}

// debugState 返回一份加锁拷贝的迁移状态快照，仅供测试诊断（避免无锁读 map 触发
// 并发读写 panic）。打印 pendingIn/pendingOut/shards/incoming 与配置号，定位迁移冻结。
// orphanCounts 在持锁下返回当前残留的迁移中间态数量：
// pendingIn（待收未收）/ pendingOut（待迁未 GC）/ incoming（收到未生效）。
// 配置稳定后应全部为 0；非 0 表示迁移状态机有泄漏（即便未冻结也已破坏一致性）。
func (kv *ShardKV) orphanCounts() (pendingIn, pendingOut, incoming int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return len(kv.pendingIn), len(kv.pendingOut), len(kv.incoming)
}

func (kv *ShardKV) debugState() string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	pin := []int{}
	for s := range kv.pendingIn {
		pin = append(pin, s)
	}
	pout := []int{}
	for s := range kv.pendingOut {
		pout = append(pout, s)
	}
	sh := []int{}
	for s := range kv.shards {
		sh = append(sh, s)
	}
	inc := []int{}
	for s := range kv.incoming {
		inc = append(inc, s)
	}
	return fmt.Sprintf("config=%d prev=%d proposed=%d pendingIn=%v pendingOut=%v shards=%v incoming=%v | raft[%s]",
		kv.config.Num, kv.prevConfig.Num, kv.proposedConfigNum, pin, pout, sh, inc, kv.rf.String())
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
			kv.applyGC(op.GCShard)
		}
		nid := op.NotifyId
		ch := kv.notified[nid]
		delete(kv.notified, nid)
		kv.appliedIndex = idx
		// I9：apply 滞后画像 = commitIndex - appliedIndex（提交但未应用条目数）。
		if ci, _ := kv.rf.ReadIndex(); ci > idx {
			Metrics.Gauge("apply_lag").Set(float64(ci - idx))
		} else {
			Metrics.Gauge("apply_lag").Set(0)
		}
		kv.mu.Unlock()

		if ch != nil {
			ch <- res
		}
		// 快照压缩（可选）：日志超过阈值时把 KV 状态压进 Raft 快照；另外每次配置
		// 变更（NewConfig）也强制快照一次，及时固化最新配置 + 迁移状态（I5）。
		if kv.maxraftstate > 0 && (kv.rf.RaftStateSize() > kv.maxraftstate || op.Kind == "NewConfig") {
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
	// 关键正确性守卫（修复 A→B→A 高频再平衡下的丢写）：命令被 apply 时，若本组
	// 已因配置变迁失去该分片所有权，即使本地仍残留旧副本（applyNewConfig 在"失去
	// 所有权"分支不会清除 kv.shards[s]，以便 GetShard 向新 owner 传输），也绝不可把
	// 写入落到陈舊本地副本并返回 OK——那样数据无法到达新 owner，而客户端已收到 OK
	// 不再重试，造成静默丢写（TestSKVClientLiveDuringMigration 复现：append 在"提交晚
	// 于迁移配置"窗口内被写入旧 owner 陈舊副本）。必须返回 ErrWrongGroup，由客户端
	// 重读配置后把请求重投到当前 owner。此检查与 applyNewConfig 的"重新夺回时清除
	// 本地陈舊副本"互为补充：前者防"提交晚于迁移"的写入落空，后者防"重新夺回后窗口
	// 内本地写被权威迁移整体替换"。
	if kv.config.Shards[op.Shard] != kv.gid {
		res.err = ErrWrongGroup
		return
	}
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
	if kv.installedCfgNum == nil {
		kv.installedCfgNum = map[int]int{}
	}
	// I5：配置变更计数（可观测配置推进频率）。
	Metrics.Counter("config_changes").Inc()
	// I9：当前生效配置号画像。
	Metrics.Gauge("config_num").Set(float64(cfg.Num))
	// I8：配置已应用，更新已提议水位，避免 pollConfig 重复提议。
	kv.proposedConfigNum = cfg.Num
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
					// 本组成为 owner 且 incoming 已缓冲该分片的权威副本（来自上一版
					// 配置 owner 的拉取）：整体替换为权威数据。注意此处必须「替换」
					// 而非「合并保留本地值」——在 A→B→A 高频来回再平衡下，本地
					// kv.shards[s] 是上一轮所有权的陈旧残留，mergeShardData「仅补缺失
					// key、不覆盖已有值」会用陈旧值覆盖权威数据，导致 LastSeq 被推高而
					// Data 停留在旧值、后续 Append 被去重丢弃（TestSKVClientLiveDuring
					// Migration 复现丢写）。本函数整体在 kv.mu 下执行（applier 持锁），
					// 不存在与并发客户端写的竞争，故直接替换安全。
					kv.shards[s] = data
					delete(kv.incoming, s)
					// 关键修复（cycle 48 根因）：数据已从 incoming 装入本组分片，
					// 必须清除 pendingIn[s]，否则 pollConfig 被 pendingIn 门控而
					// 无法推进配置——形成"收方等配置推进清 pendingIn / 配置推进
					// 又被 pendingIn 阻塞"的死锁，3-group / ReMigration churn 冻结。
					delete(kv.pendingIn, s)
					kv.recordMigrationLatency(s)
				} else if !have {
					// 既未持有也未在 incoming 中缓冲：从上一版配置记录的旧 owner 拉取。
					kv.pendingIn[s] = true
					oldServers := append([]string{}, kv.prevConfig.Groups[oldG]...)
					kv.fetchEpoch[s]++
					epoch := kv.fetchEpoch[s]
					go kv.fetchShard(s, oldServers, epoch)
				} else {
					// have==true：本组仍持有上一轮所有权的陈舊副本（A→B→A 来回再平衡下，
					// 分组失去分片后并未丢弃本地副本）。重新夺回所有权时该副本已非权威——
					// 权威数据在旧 owner 处，且可能含本组从未见过的新写入。若保留它，窗口内
					// 客户端写会被追加到陈舊副本，随后到达的权威迁移（按迁移配置号整体替换）
					// 会把这段本地写冲掉，造成丢写（TestSKVClientLiveDuringMigration 在套件
					// 上下文复现 18/20）。故此处清除本地陈舊副本：① shards[s] 缺席使窗口内
					// 写入返回 ErrWrongGroup、由客户端重试，直到权威迁移装入；② 随后
					// applyInstallShard 以「空分片 + 迁移配置号 LWW」干净替换，绝不残留陈舊值。
					// 连续持有（prevConfig.Shards[s]==gid）不会进入本分支，故不会误清有效数据。
					delete(kv.shards, s)
					kv.pendingIn[s] = true
					oldServers := append([]string{}, kv.prevConfig.Groups[oldG]...)
					kv.fetchEpoch[s]++
					epoch := kv.fetchEpoch[s]
					go kv.fetchShard(s, oldServers, epoch)
				}
			}
			delete(kv.pendingOut, s) // 本组现在拥有 s，不再迁出它
			// I2：记录安装配置号（幂等去重依据）。
			if _, ok := kv.shards[s]; ok {
				kv.installedCfgNum[s] = cfg.Num
			}
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
				// 同样清除 pendingIn：该分片数据已装入，无论后续归属如何都不再"待收"。
				delete(kv.pendingIn, s)
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
	if kv.installedCfgNum == nil {
		kv.installedCfgNum = map[int]int{}
	}
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
	// I3（安全版）：仅当本组「当前不拥有」该分片且迁移配置号已落后于本组配置号时，
	// 才丢弃陈旧传输——此时数据已无用（本组不持有该分片，配置也已推进过去）。
	// 若本组「拥有」该分片（kv.config.Shards[s]==gid），则这是因"配置推进快于单跳迁移"
	// 而迟到的合法传输，必须安装，否则 pendingIn 永不清除、配置冻结（ReMigration 快速
	// churn 回归）。旧的"无条件 op.MigrateConfigNum < config.Num 即丢弃"会把这种合法
	// 迟到传输也丢掉，正是本次冻结的根因。
	if op.MigrateConfigNum < kv.config.Num && kv.config.Shards[s] != kv.gid {
		if res != nil {
			res.err = OK
		}
		return
	}
	if kv.config.Shards[s] == kv.gid {
		// 本组拥有该分片：以「迁移配置号」做 LWW（后写胜出）决定如何并入，杜绝
		// 迟到/陈旧传输把已提交的更新数据冲掉（A→B→A 高频来回再平衡下，旧实现用
		// mergeShardData「仅补缺失 key、不覆盖已有值」会用陈旧快照覆盖新写入，导致
		// LastSeq 被推高而 Data 停留在旧值，后续 Append 被去重丢弃——TestSKVClientLive
		// DuringMigration 复现丢写）。
		installed := kv.installedCfgNum[s]
		if op.MigrateConfigNum < installed {
			// 迟到/陈旧传输：比本组已装入的配置号更旧，直接丢弃，保留当前（更新）数据。
			if res != nil {
				res.err = OK
			}
			return
		}
		if op.MigrateConfigNum == installed && kv.shards[s] != nil {
			// 同一配置号的重复/迟到传输：正常情况下源端在该配置下失去所有权后不再
			// 服务写，两次传输应完全一致；但迁移竞态下源端可能在两次发送间又应用了
			// 本组从未见过的写（极窄窗口），故以「版本」(各 client LastSeq 最大值)
			// 做二次判据：传入副本版本更高 → 整体替换为更完整的快照，避免「陈旧传输
			// 先到、完整传输后被盲目跳过」造成丢写；版本不更高 → 跳过，保留本地
			// （可能含窗口内本地写，且不会覆盖更高版本数据）。
			if shardVersion(incoming) > shardVersion(kv.shards[s]) {
				kv.shards[s] = incoming
				kv.installedCfgNum[s] = op.MigrateConfigNum
				kv.recordShardBytes(s)
			}
			delete(kv.pendingIn, s)
			kv.recordMigrationLatency(s)
			if res != nil {
				res.err = OK
			}
			return
		}
		// 更新（或更早未持有）：以迁移来的权威副本整体替换，并记录其配置号。
		kv.shards[s] = incoming
		kv.installedCfgNum[s] = op.MigrateConfigNum
		delete(kv.pendingIn, s)
		kv.recordMigrationLatency(s)
		kv.recordShardBytes(s) // I15：记录分片字节大小 + 超大分片告警
		if res != nil {
			res.err = OK
		}
		return
	}
	// 本组当前不拥有该分片：缓冲到 incoming，待 applyNewConfig 推进配置后消费安装。
	kv.incoming[s] = incoming
	if res != nil {
		res.err = OK
	}
}

// applyGC 处理 GCShard：丢弃已迁出的分片副本，但仅当本组当前配置不再拥有该分片时。
// GC 语义是"新 owner 已持有，本组可删副本"；A→B→A 快速 churn 下分片可能在 sendShard
// 尚未完成 GC 时就回摆回本组，残留 pendingOut 会被 migratePump / 旧 sendShard 协程触发
// 自 GC——若无此守卫会删掉本组仍是权威 owner 的分片数据（丢数据）。pendingOut 标记无论
// 是否拥有都清除。调用方必须持有 kv.mu。
func (kv *ShardKV) applyGC(s int) {
	if kv.config.Shards[s] != kv.gid {
		delete(kv.shards, s)
	}
	delete(kv.pendingOut, s)
}

// recordMigrationLatency 记录分片 s 从"进入待接收(pendingIn)"到"成功装入本组"的
// 耗时（毫秒），供 /metrics 的 shard_migration_ms 直方图观测迁移性能。
func (kv *ShardKV) recordMigrationLatency(s int) {
	if t, ok := kv.pendingInSince[s]; ok && !t.IsZero() {
		Metrics.Histogram("shard_migration_ms").Record(time.Since(t).Seconds() * 1000)
	}
}

// recordShardBytes 记录分片 s 经迁移装入时的序列化字节大小（直方图 shard_bytes），
// 并对超过阈值（4MiB）的超大分片告警（counter shard_bytes_overflow），便于及时发现
// 异常大的分片数据拖慢快照/迁移。
func (kv *ShardKV) recordShardBytes(s int) {
	sd := kv.shards[s]
	if sd == nil {
		return
	}
	n := shardByteSize(sd)
	Metrics.Histogram("shard_bytes").Record(float64(n))
	const overflowThreshold = 4 * 1024 * 1024
	if n > overflowThreshold {
		Metrics.Counter("shard_bytes_overflow").Inc()
	}
}

// shardByteSize 返回 ShardData 经 gob 编码后的字节数（用于迁移分片大小观测）。
func shardByteSize(sd *ShardData) int {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(sd); err != nil {
		return 0
	}
	return buf.Len()
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

// migrateBackoff 返回第 attempt 次迁移重试的退避时长（指数退避，上限 1s）。
// 用于 fetchShard/sendShard 在来源不可达时降低 RPC 风暴、缓解 churn 下的网络争用；
// 首跳（attempt=0）仍为 50ms，保持正常路径延迟不变，仅在持续失败时退避。
func migrateBackoff(attempt int) time.Duration {
	const base = 50 * time.Millisecond
	const max = 1 * time.Second
	d := base
	for i := 0; i < attempt; i++ {
		d *= 2
		if d >= max {
			return max
		}
	}
	return d
}

func (kv *ShardKV) fetchShard(s int, oldServers []string, epoch int64) {
	attempt := 0
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
		// 更新，本组需要的数据始终由「当前 prevConfig 的 owner」提供。同时记录
		// 当前配置号，作为本次拉取安装的配置版本（LWW 依据，见 applyInstallShard）。
		oldG := kv.prevConfig.Shards[s]
		curNum := kv.config.Num
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
			if end.Call("ShardKV.GetShard", &GetShardArgs{Shard: s}, reply) && reply.Err == OK && reply.Data != nil && reply.Config >= kv.config.Num {
				kv.propose(Op{Kind: "InstallShard", MigrateShard: s, MigrateData: reply.Data, MigrateConfigNum: curNum})
				got = true
				break
			}
		}
		if !got {
			// 自愈回源（cycle 27/33 + n=33 加固）：记录的旧 owner 不可达时，先尝试用
			// ShardMaster「最新」配置 owner 重新拉取，跨多跳迁移链找回数据。但若最新
			// 配置下本组自己就是 owner（curOwner==kv.gid）却仍无数据，说明数据其实还
			// 在「上一版配置」的 owner 手中——绝不能就此放弃（旧实现在此直接 return，
			// 导致 pendingIn 永久残留、配置冻结，TestSKVClientLiveDuringMigration 在
			// 高频 A→B→A 来回再平衡下复现）。一律回退到 prevConfig owner 作为权威来源
			// 拉取；仅当 prevConfig 也无 owner（全新分配 oldG==0）才清 pendingIn 让配置
			// 推进，避免残留冻结。
			if cur := kv.mck.Query(-1); cur.Num > 0 {
				curOwner := cur.Shards[s]
				if curOwner != kv.gid && curOwner != 0 {
					for _, srv := range cur.Groups[curOwner] {
						end := kv.make_end(srv)
						reply := &GetShardReply{}
						if end.Call("ShardKV.GetShard", &GetShardArgs{Shard: s}, reply) && reply.Err == OK && reply.Data != nil && reply.Config >= kv.config.Num {
							kv.propose(Op{Kind: "InstallShard", MigrateShard: s, MigrateData: reply.Data, MigrateConfigNum: curNum})
							got = true
							break
						}
					}
				}
			}
			if !got {
				kv.mu.Lock()
				pg := kv.prevConfig.Shards[s]
				kv.mu.Unlock()
				if pg != kv.gid && pg != 0 {
					for _, srv := range kv.prevConfig.Groups[pg] {
						end := kv.make_end(srv)
						reply := &GetShardReply{}
						if end.Call("ShardKV.GetShard", &GetShardArgs{Shard: s}, reply) && reply.Err == OK && reply.Data != nil && reply.Config >= kv.config.Num {
							kv.propose(Op{Kind: "InstallShard", MigrateShard: s, MigrateData: reply.Data, MigrateConfigNum: curNum})
							got = true
							break
						}
					}
				} else {
					// prevConfig 无旧 owner：全新分配或已回流到本组但无来源，清
					// pendingIn（数据由 applyNewConfig 的 oldG==0 路径 / 后续 incoming
					// 负责），避免残留冻结。
					kv.mu.Lock()
					delete(kv.pendingIn, s)
					kv.mu.Unlock()
				}
			}
		}
		if got {
			return
		}
		time.Sleep(migrateBackoff(attempt))
		attempt++
	}
}

func (kv *ShardKV) sendShard(s, newG int) {
	attempt := 0
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
			time.Sleep(migrateBackoff(attempt))
			attempt++
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
			time.Sleep(migrateBackoff(attempt))
			attempt++
			continue
		}
		sent := false
		for _, srv := range servers {
			end := kv.make_end(srv)
			reply := &SendShardReply{}
			t0 := time.Now()
			ok := end.Call("ShardKV.SendShard", &SendShardArgs{Shard: s, Data: data, ConfigNum: cfg.Num}, reply) && reply.Err == OK
			// I10：记录 SendShard RPC 延迟（成功/失败均记录），观测迁移推送性能。
			Metrics.Histogram("send_shard_latency").Record(float64(time.Since(t0).Microseconds()) / 1000.0)
			if ok {
				sent = true
				break
			}
		}
		if sent {
			// 对方已提交该分片数据，本组可安全回收。GCShard 的 propose 可能因
			// 瞬时负载触发内置 3s 超时（ErrTimeout），必须重试直至提交成功，
			// 否则 pendingOut[s] 残留会留下脏状态（cycle 48 加固）。循环至
			// pendingOut[s] 被 GC 处理器清除为止。GC 重试保持紧密（50ms），
			// 以便尽快清除 pendingOut、释放配置推进。
			for !kv.killed() {
				kv.mu.Lock()
				if !kv.pendingOut[s] {
					kv.mu.Unlock()
					return
				}
				kv.mu.Unlock()
				r := kv.propose(Op{Kind: "GCShard", GCShard: s})
				if r.err == OK {
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
			return
		}
		time.Sleep(migrateBackoff(attempt))
		attempt++
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
	snap.Installed = map[int]int{}
	for k, v := range kv.installedCfgNum {
		snap.Installed[k] = v
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
	kv.installedCfgNum = map[int]int{}
	for k, v := range snap.Installed {
		kv.installedCfgNum[k] = v
	}
	kv.config = snap.Config
	kv.prevConfig = snap.PrevConfig

	// 重置看门狗时间戳：快照恢复后从「现在」重新计时，避免沿用崩溃/安装前的
	// 陈旧时间戳，导致看门狗误判某分片「已卡死很久」而狂触发 rekick（活锁热点），
	// 同时防止 /debug/shards 的 StallSeconds 显示虚假大值。fetchEpoch 只增不减，
	// 用于作废陈旧 fetcher，无需重置。
	now := time.Now()
	kv.pendingInSince = map[int]time.Time{}
	for s := range kv.pendingIn {
		kv.pendingInSince[s] = now
	}
	kv.pendingOutSince = map[int]time.Time{}
	for s := range kv.pendingOut {
		kv.pendingOutSince[s] = now
	}
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

func (ck *Clerk) Put(key, value string)    { ck.PutAppend(key, value, "Put") }
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
	Lease      bool // leader 是否持有合法租约（多数派近期有心跳接触）；分区/刚上任为 false
	ConfigNum  int
	Owned      []int
	Incoming   []int
	PendingIn  []int
	PendingOut []int
	// 卡滞观测：每个 pending 分片首次进入未决态的 RFC3339 时间戳，以及当前最大卡滞秒数。
	// 复现 3-group / ReMigration 冻结时，curl /debug/shards 即可看到哪些分片卡了多久。
	PendingInSince  map[int]string `json:",omitempty"`
	PendingOutSince map[int]string `json:",omitempty"`
	StallSeconds    float64        `json:",omitempty"`
}

func (kv *ShardKV) ShardDebug() ShardDebug {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	hasLease := isLeader && kv.rf.HasLeaderLease()
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
	inSince := map[int]string{}
	var maxStall float64
	for s, t := range kv.pendingInSince {
		inSince[s] = t.Format(time.RFC3339)
		if d := time.Since(t).Seconds(); d > maxStall {
			maxStall = d
		}
	}
	outSince := map[int]string{}
	for s, t := range kv.pendingOutSince {
		outSince[s] = t.Format(time.RFC3339)
		if d := time.Since(t).Seconds(); d > maxStall {
			maxStall = d
		}
	}
	return ShardDebug{
		GID:             kv.gid,
		Leader:          isLeader,
		Lease:           hasLease,
		ConfigNum:       kv.config.Num,
		Owned:           owned,
		Incoming:        incoming,
		PendingIn:       pendIn,
		PendingOut:      pendOut,
		PendingInSince:  inSince,
		PendingOutSince: outSince,
		StallSeconds:    maxStall,
	}
}
