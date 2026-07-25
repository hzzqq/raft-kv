// kvraft.go —— Lab 3：基于 Raft 的容错 KV 存储（Get/Put/Append，线性一致）
// 复用 raft 包导出的 Network/ClientEnd/Raft/Make/ApplyMsg。
package kvraft

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/src/metrics"
	"raftkv/src/raft"
)

// Metrics 是 KV 组件的可观测性指标（best-effort 进程级聚合）。
var Metrics = metrics.NewRegistry()

// client 会话 GC 的默认参数（生产环境）。
// 测试可临时覆盖 kv 实例上的 gcTTL / gcInterval 字段，以缩短时长、快速验证回收逻辑。
var (
	defaultGCTTL      = time.Hour
	defaultGCInterval = 10 * time.Minute
)

// Op 需要向 gob 注册：Raft 把日志以 interface{} 形式持久化，
// 反序列化时必须知道具体类型，否则重启后日志变空（命令丢失）。
func init() {
	gob.Register(Op{})
	// 进程级瞬时量 HELP：数据规模与去重会话表规模，供 kvraft.Metrics 直接 scrape/JSON 输出。
	Metrics.GaugeWithHelp("kv_data_keys", "当前 KV 状态机数据键数量(近似,多实例共享注册表时取最后写入者)")
	Metrics.GaugeWithHelp("kv_sessions", "当前去重会话表大小(已注册 client 数,近似)")
	Metrics.CounterWithHelp("gc_sweeps_total", "client 会话 GC 扫描累计次数")
	Metrics.CounterWithHelp("gc_sessions_evicted_total", "GC 累计回收的空闲会话数")
}

// Op 是提交到 Raft 状态机的操作。ClientId+Seq 用于幂等去重。
type Op struct {
	Key      string
	Value    string
	OpType   string // "Get" / "Put" / "Append"
	ClientId int64
	Seq      int64
}

// 应用结果
type OpResult struct {
	Err   string
	Value string
}

// clientSession 保存单个 Clerk 的去重状态与最近访问时间，供 GC 回收。
// 结构体本身未导出；仅 lastAccess（新增字段）保持未导出，满足"不改动对外契约"。
// LastSeq / LastResult 需导出以便 gob 序列化进快照（time.Time 字段 lastAccess 会被 gob 自动忽略）。
type clientSession struct {
	LastSeq    int64     // 该 client 已应用的最大 Seq
	LastResult string    // 最近一次 Get 的返回值（去重用）
	lastAccess time.Time // 最近一次被访问（去重命中或新命令）的时间，GC 依据其空闲时长回收（未导出）
}

// 应用层通知：applier 把"实际被提交的操作 + 结果"发给等待者，
// 等待者据此判断是否真的是自己提交的那条（防止 leader 切换导致错位）。
type applyResult struct {
	op     Op
	result OpResult
}

// KVPersistState 是压缩进快照的状态机内容（不含 interface{}，gob 可直接编解码）。
type KVPersistState struct {
	Data     map[string]string
	Sessions map[int64]*clientSession
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	data     map[string]string
	sessions map[int64]*clientSession // 每个 client 的去重状态 + 最近访问时间（GC 回收）
	notify   map[int]chan applyResult

	// appliedIndex 是本 KV 状态机已应用的最后一条日志绝对索引（与 raft 的
	// lastApplied 对齐）。ReadIndex 快路径用它判断「本地是否已达 commitIndex 对应的
	// 已提交状态」，从而安全地跳过一次 Raft 日志追加直接本地读（见 Get）。
	appliedIndex int

	gcTTL      time.Duration // 空闲超过该时长的 client 会话被 GC 回收
	gcInterval time.Duration // GC 扫描周期
	killCh     chan struct{} // GC goroutine 退出信号（仅由 Kill 关闭一次）
	killOnce   sync.Once

	maxraftstate int // >0 时超过该字节数触发快照（本层可选）
}

func (kv *KVServer) Kill() {
	kv.killOnce.Do(func() { close(kv.killCh) })
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *KVServer) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// ============================== RPC 参数 ==============================

type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" 或 "Append"
	ClientId int64
	Seq      int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err        string
}

type GetArgs struct {
	Key      string
	ClientId int64
	Seq      int64
}

type GetReply struct {
	WrongLeader bool
	Err        string
	Value      string
}

// ============================== 应用循环 ==============================

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg, ok := <-kv.applyCh
		if !ok {
			return
		}
		if msg.SnapshotValid {
			// 落后 follower 或重启节点通过快照追平状态机。
			// installSnapshot 内部持锁完成「陈旧快照拒绝 + 状态覆盖 + appliedIndex 推进」，
			// 修复此前 appliedIndex 无锁写的数据竞态与陈旧快照回滚状态机的正确性隐患。
			if kv.installSnapshot(msg.Snapshot, msg.SnapshotIndex) {
				Metrics.Counter("snapshots_installed").Inc()
				// 快照追平后同步刷新规模 gauge（installSnapshot 内部持锁，此处另行加锁读取）。
				kv.mu.Lock()
				Metrics.Gauge("kv_data_keys").Set(float64(len(kv.data)))
				Metrics.Gauge("kv_sessions").Set(float64(len(kv.sessions)))
				kv.mu.Unlock()
			}
			continue
		}
		if !msg.CommandValid {
			continue
		}
		Metrics.Counter("entries_applied").Inc()
		op, ok := msg.Command.(Op)
		if !ok {
			// no-op（leader 任期开始追加的空命令）：不更新状态机，直接跳过。
			continue
		}
		kv.mu.Lock()
		var res OpResult
		s, exists := kv.sessions[op.ClientId]
		if !exists || op.Seq > s.LastSeq {
			switch op.OpType {
			case "Get":
				res.Value = kv.data[op.Key]
			case "Put":
				kv.data[op.Key] = op.Value
			case "Append":
				kv.data[op.Key] += op.Value
			}
			if !exists {
				s = &clientSession{}
				kv.sessions[op.ClientId] = s
			}
			s.LastSeq = op.Seq
			s.LastResult = res.Value
		} else {
			// 重复命令：直接复用上次结果（幂等）
			res.Value = s.LastResult
		}
		s.lastAccess = time.Now()
		idx := msg.CommandIndex
		kv.appliedIndex = idx
		ch := kv.notify[idx]
		delete(kv.notify, idx)
		// maxraftstate > 0 且 Raft 状态超过阈值时主动压缩快照（Lab 2D ↔ KV 集成）。
		if kv.maxraftstate > 0 && kv.rf.RaftStateSize() > kv.maxraftstate {
			if snap := kv.encodeSnapshot(); snap != nil {
				kv.rf.Snapshot(idx, snap)
			}
		}
		// 每次应用后刷新规模 gauge（仍持 kv.mu，无额外锁开销）。
		Metrics.Gauge("kv_data_keys").Set(float64(len(kv.data)))
		Metrics.Gauge("kv_sessions").Set(float64(len(kv.sessions)))
		kv.mu.Unlock()
		if ch != nil {
			ch <- applyResult{op: op, result: res}
		}
	}
}

// encodeSnapshot 把当前 KV 状态机（data/lastSeq/lastResult）压缩成字节，供 Raft 持久化。
func (kv *KVServer) encodeSnapshot() []byte {
	st := KVPersistState{
		Data:     kv.data,
		Sessions: kv.sessions,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(st); err != nil {
		return nil
	}
	return buf.Bytes()
}

// installSnapshot 从 Raft 快照恢复 KV 状态机（重启或落后 follower 追平时调用）。
// snapIndex 为快照对应的日志绝对索引：若不高于当前 appliedIndex（陈旧/重复快照），
// 拒绝安装并返回 false——否则会把状态机倒回旧状态、破坏线性一致性。
// appliedIndex 的推进与状态覆盖在同一临界区内完成（消除无锁写竞态）。
// snapIndex 传 0 表示无索引信息的恢复路径：跳过陈旧检查、安装但不推进 appliedIndex。
func (kv *KVServer) installSnapshot(data []byte, snapIndex int) bool {
	if len(data) == 0 {
		return false
	}
	var st KVPersistState
	if err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&st); err != nil {
		return false
	}
	// gob 解码空 map 可能得到 nil：归一为空 map，防后续写入 panic。
	if st.Data == nil {
		st.Data = make(map[string]string)
	}
	if st.Sessions == nil {
		st.Sessions = make(map[int64]*clientSession)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapIndex > 0 && snapIndex <= kv.appliedIndex {
		return false // 陈旧快照：状态机已应用得更远，安装会回滚
	}
	kv.data = st.Data
	kv.sessions = st.Sessions
	if snapIndex > 0 {
		kv.appliedIndex = snapIndex
	}
	return true
}

// waitAppliedIndex 轮询等待本服务器状态机已应用到索引 idx（ReadIndex 快路径用），
// 超时返回 false。本地读前确保读到 commitIndex 对应的已提交状态；appliedIndex 的
// 读写均在 kv.mu 保护下进行（applier 在持锁时写入）。
func (kv *KVServer) waitAppliedIndex(idx int) bool {
	deadline := time.Now().Add(3 * time.Second)
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

// waitApplied 等待本服务器把 op 提交到状态机并返回结果。
// 若超时、或该位置被别的命令占据（leader 切换），返回 WrongLeader 让客户端重试。
func (kv *KVServer) waitApplied(op Op, index int) OpResult {
	ch := make(chan applyResult, 1)
	kv.mu.Lock()
	kv.notify[index] = ch
	kv.mu.Unlock()

	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()
	select {
	case ar := <-ch:
		if ar.op.ClientId == op.ClientId && ar.op.Seq == op.Seq {
			return ar.result
		}
		return OpResult{Err: "mismatch"}
	case <-timer.C:
		kv.mu.Lock()
		delete(kv.notify, index)
		kv.mu.Unlock()
		return OpResult{Err: "timeout"}
	}
}

// KVStatus 是 KV 状态机的只读健康快照（运维/诊断用），与 raft.RaftStatus 互补：
// 共识层看角色/任期/leader，数据层看 apply 进度、数据规模、去重会话表与 GC 配置。
// 此前 KV 状态机对运维完全不透明（只能 dump 内存），与同仓 raft/shardkv 已暴露的
// 健康快照不对齐；此处补齐数据面一手信号。
type KVStatus struct {
	Me            int    `json:"me"`
	Role          string `json:"role"`
	Term          int    `json:"term"`
	LeaderID      int    `json:"leader_id"`
	AppliedIndex  int    `json:"applied_index"`
	DataKeys      int    `json:"data_keys"`
	Sessions      int    `json:"sessions"`
	GCTTLSec      int64  `json:"gc_ttl_sec"`
	GCIntervalSec int64  `json:"gc_interval_sec"`
}

// Status 返回 KV 状态机当前只读快照。所有字段均在 kv.mu 保护下读取，并发安全；
// rf 为 nil（簇无关测试或部分初始化）时不 panic，仅缺失共识字段。
func (kv *KVServer) Status() KVStatus {
	st := KVStatus{
		Me:            kv.me,
		AppliedIndex:  kv.appliedIndex,
		DataKeys:      len(kv.data),
		Sessions:      len(kv.sessions),
		GCTTLSec:      int64(kv.gcTTL / time.Second),
		GCIntervalSec: int64(kv.gcInterval / time.Second),
	}
	if kv.rf != nil {
		rs := kv.rf.Status()
		st.Me = rs.Me
		st.Role = rs.Role.String()
		st.Term = rs.Term
		st.LeaderID = rs.LeaderID
	}
	return st
}

func (kv *KVServer) startOp(op Op) (int, bool) {
	index, _, ok := kv.rf.Start(op)
	if !ok {
		return 0, false
	}
	return index, true
}

// ============================== 对外 RPC ==============================

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	start := time.Now()
	// 线性一致读优化（ReadIndex 快路径）：仅当本副本是 leader 且持有合法 leader
	// 租约（HasLeaderLease，多数派近期有心跳接触）时，才跳过日志追加、直接读本地
	// 状态机。租约保证该 leader 期间无更新任期产生、commitIndex 不会被回滚，故等待
	// 本服务器 apply 到 commitIndex 后本地读是线性一致的。租约失效（分区/刚上任）
	// 时一律回退 propose 路径，绝不牺牲正确性。mirror shardkv 的同类优化。
	if kv.rf.HasLeaderLease() {
		if commitIdx, isLeader := kv.rf.ReadIndex(); isLeader {
			if kv.waitAppliedIndex(commitIdx) {
				kv.mu.Lock()
				v := kv.data[args.Key]
				kv.mu.Unlock()
				Metrics.Counter("read_leases").Inc()
				reply.Value = v
				Metrics.Counter("ops_total").Inc()
				Metrics.Histogram("op_latency_ms").Record(float64(time.Since(start).Microseconds()) / 1000.0)
				return
			}
		}
	}
	// 回退路径：始终走 propose（经 Raft 共识，绝对线性一致）。
	op := Op{Key: args.Key, OpType: "Get", ClientId: args.ClientId, Seq: args.Seq}
	index, ok := kv.startOp(op)
	if !ok {
		reply.WrongLeader = true
		Metrics.Counter("ops_total").Inc()
		Metrics.Counter("ops_errors").Inc()
		return
	}
	res := kv.waitApplied(op, index)
	if res.Err != "" {
		reply.WrongLeader = true
		Metrics.Counter("ops_total").Inc()
		Metrics.Counter("ops_errors").Inc()
		return
	}
	reply.Value = res.Value
	Metrics.Counter("ops_total").Inc()
	Metrics.Histogram("op_latency_ms").Record(float64(time.Since(start).Microseconds()) / 1000.0)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	start := time.Now()
	op := Op{Key: args.Key, Value: args.Value, OpType: args.Op, ClientId: args.ClientId, Seq: args.Seq}
	index, ok := kv.startOp(op)
	if !ok {
		reply.WrongLeader = true
		Metrics.Counter("ops_total").Inc()
		Metrics.Counter("ops_errors").Inc()
		return
	}
	res := kv.waitApplied(op, index)
	if res.Err != "" {
		reply.WrongLeader = true
		Metrics.Counter("ops_total").Inc()
		Metrics.Counter("ops_errors").Inc()
		return
	}
	reply.Err = res.Err
	Metrics.Counter("ops_total").Inc()
	Metrics.Histogram("op_latency_ms").Record(float64(time.Since(start).Microseconds()) / 1000.0)
}

// ============================== 构造 ==============================

func MakeKVServer(me int, rf *raft.Raft, applyCh chan raft.ApplyMsg, maxraftstate int) *KVServer {
	kv := &KVServer{
		me:           me,
		rf:           rf,
		applyCh:      applyCh,
		data:         make(map[string]string),
		sessions:     make(map[int64]*clientSession),
		notify:       make(map[int]chan applyResult),
		maxraftstate: maxraftstate,
		gcTTL:        defaultGCTTL,
		gcInterval:   defaultGCInterval,
		killCh:       make(chan struct{}),
	}
	go kv.applier()
	go kv.gc()
	return kv
}

// gc 周期性扫描并回收空闲超过 gcTTL 的 client 会话；
// 收到 killCh 信号时退出，确保 Kill 后能干净关闭 goroutine。
func (kv *KVServer) gc() {
	ticker := time.NewTicker(kv.gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-kv.killCh:
			return
		case <-ticker.C:
			kv.gcSweep(time.Now())
		}
	}
}

// gcSweep 在持锁状态下回收所有空闲超时的会话，避免与请求处理产生竞态。
// 同时累加可观测计数：扫描次数与本次实际回收的会话数，使 GC 行为不再对运维不可见。
func (kv *KVServer) gcSweep(now time.Time) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var evicted int
	for cid, s := range kv.sessions {
		if now.Sub(s.lastAccess) > kv.gcTTL {
			delete(kv.sessions, cid)
			evicted++
		}
	}
	Metrics.Counter("gc_sweeps_total").Inc()
	if evicted > 0 {
		Metrics.Counter("gc_sessions_evicted_total").Add(int64(evicted))
	}
	// 回收后同步会话规模 gauge（data 不变，一并刷新保持两个规模 gauge 同源更新）。
	Metrics.Gauge("kv_sessions").Set(float64(len(kv.sessions)))
	Metrics.Gauge("kv_data_keys").Set(float64(len(kv.data)))
}

// ============================== 客户端 Clerk ==============================

var clientSeq int64 // 全局原子计数器，保证每个 Clerk 的 clientId 全局唯一

type Clerk struct {
	servers   []*raft.ClientEnd
	clientId  int64
	seq       int64
	leaderHint int
}

func MakeClerk(servers []*raft.ClientEnd) *Clerk {
	return &Clerk{
		servers:  servers,
		clientId: atomic.AddInt64(&clientSeq, 1),
		seq:     0,
	}
}

func (ck *Clerk) Get(key string) string {
	ck.seq++
	op := GetArgs{Key: key, ClientId: ck.clientId, Seq: ck.seq}
	backoff := 10 * time.Millisecond
	for {
		srv := ck.servers[ck.leaderHint]
		reply := &GetReply{}
		ok := srv.Call("Get", &op, reply)
		if ok && !reply.WrongLeader {
			return reply.Value
		}
		// 轮换 leader 候选 + 指数退避，避免 leader 切换期对副本的惊群重试
		ck.leaderHint = (ck.leaderHint + 1) % len(ck.servers)
		time.Sleep(backoff)
		if backoff < 500*time.Millisecond {
			backoff *= 2
		}
	}
}

func (ck *Clerk) PutAppend(key, value, op string) {
	ck.seq++
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, Seq: ck.seq}
	backoff := 10 * time.Millisecond
	for {
		srv := ck.servers[ck.leaderHint]
		reply := &PutAppendReply{}
		ok := srv.Call("PutAppend", &args, reply)
		if ok && !reply.WrongLeader {
			return
		}
		ck.leaderHint = (ck.leaderHint + 1) % len(ck.servers)
		time.Sleep(backoff)
		if backoff < 500*time.Millisecond {
			backoff *= 2
		}
	}
}

func (ck *Clerk) Put(key, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key, value string) {
	ck.PutAppend(key, value, "Append")
}

func nrand() int64 {
	return time.Now().UnixNano()
}
