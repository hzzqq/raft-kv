// shardmaster.go —— Lab 4 配置服务（分片控制器）
// 维护 NShards 个分片到 replica group 的映射，并通过 Raft 复制所有变更，
// 保证配置的线性一致。客户端用 Query 读取最新（或历史）配置。
package shardmaster

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/src/raft"
)

// ============================== 常量与类型 ==============================

const NShards = 10

type Err string

const (
	OK             Err = "OK"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
	ErrInvalid     Err = "ErrInvalid"
)

// Config 是一份分片配置。Num 单调递增；Shards[i] 表示分片 i 由哪个 gid 负责；
// Groups 记录每个 gid 由哪些 server 名组成。
type Config struct {
	Num    int
	Shards [NShards]int
	Groups map[int][]string
}

// ---------- RPC 参数 ----------

type JoinArgs struct {
	Servers map[int][]string
	CkId    int64
	Seq     int64
}
type JoinReply struct{ Err Err }

type LeaveArgs struct {
	Gids []int
	CkId int64
	Seq  int64
}
type LeaveReply struct{ Err Err }

type MoveArgs struct {
	Shard int
	Gid   int
	CkId  int64
	Seq   int64
}
type MoveReply struct{ Err Err }

type QueryArgs struct{ Num int }
type QueryReply struct {
	Err    Err
	Config Config
}

// Op 是写入 Raft 日志的操作（配置变更）。CkId/Seq 用于客户端幂等去重；
// NotifyId 是调用方在 Start 之前就分配好的唯一 id，applier 据此唤醒等待者，
// 避免"先 Start 拿到 index 再注册 channel"造成的丢失唤醒竞态。
type Op struct {
	Kind     string // "Join" / "Leave" / "Move"
	CkId     int64
	Seq      int64
	NotifyId int64
	Servers  map[int][]string
	Gids     []int
	Shard    int
	Gid      int
}

func init() {
	gob.Register(Op{})
}

// ============================== ShardMaster 结构体 ==============================

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	configs  []Config // 历史配置；configs[0] 为初始空配置
	lastSeq  map[int64]int64
	notified map[int64]chan struct{}
	notifyId int64
	killCh   chan struct{} // 关闭即通知 applier 退出
}

// ============================== 构造 ==============================

func Make(peers []*raft.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := &ShardMaster{
		me:       me,
		applyCh:  make(chan raft.ApplyMsg, 100),
		configs:  make([]Config, 1),
		lastSeq:  make(map[int64]int64),
		notified: make(map[int64]chan struct{}),
		killCh:   make(chan struct{}),
	}
	sm.configs[0] = Config{Num: 0, Groups: map[int][]string{}}
	sm.rf = raft.Make(peers, me, persister, sm.applyCh)
	go sm.applier()
	return sm
}

func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// 关闭 killCh 唤醒阻塞在 <-applyCh 的 applier，避免实例 cleanup 后泄漏 goroutine。
	select {
	case <-sm.killCh:
	default:
		close(sm.killCh)
	}
}

// RaftRPC 把网络层转发的 Raft 内部 RPC（RequestVote/AppendEntries/InstallSnapshot）
// 派发给底层 rf。测试框架用同一个 server id 同时承载"配置服务 RPC"与"Raft RPC"，
// 因此 handler 需要把两者都分发到正确接收方。
func (sm *ShardMaster) RaftRPC(method string, args, reply interface{}) {
	switch method {
	case "RequestVote":
		sm.rf.RequestVote(args.(*raft.RequestVoteArgs), reply.(*raft.RequestVoteReply))
	case "RequestPreVote":
		sm.rf.RequestPreVote(args.(*raft.RequestPreVoteArgs), reply.(*raft.RequestPreVoteReply))
	case "AppendEntries":
		sm.rf.AppendEntries(args.(*raft.AppendEntriesArgs), reply.(*raft.AppendEntriesReply))
	case "InstallSnapshot":
		sm.rf.InstallSnapshot(args.(*raft.InstallSnapshotArgs), reply.(*raft.InstallSnapshotReply))
	case "TimeoutNow":
		sm.rf.TimeoutNow(args.(*raft.TimeoutNowArgs), reply.(*raft.TimeoutNowReply))
	}
}
func (sm *ShardMaster) killed() bool { return atomic.LoadInt32(&sm.dead) == 1 }

// ============================== applier ==============================

func (sm *ShardMaster) applier() {
	for !sm.killed() {
		// 经 killCh 退出：raft 不会关闭 applyCh，故自带 killCh 避免 cleanup 后泄漏。
		var msg raft.ApplyMsg
		select {
		case m, ok := <-sm.applyCh:
			if !ok {
				return
			}
			msg = m
		case <-sm.killCh:
			return
		}
		if !msg.CommandValid {
			continue
		}
		// leader 上任会追加一条 no-op（Command 为 nil）；非 Op 的命令直接跳过，
		// 但仍要正确地加/解锁，避免对未加锁的 mutex 执行 Unlock。
		op, isOp := msg.Command.(Op)
		sm.mu.Lock()
		if isOp {
			if last, exists := sm.lastSeq[op.CkId]; !exists || op.Seq > last {
				sm.applyOp(op)
				sm.lastSeq[op.CkId] = op.Seq
			}
			nid := op.NotifyId
			ch := sm.notified[nid]
			delete(sm.notified, nid)
			sm.mu.Unlock()
			if ch != nil {
				close(ch)
			}
		} else {
			sm.mu.Unlock()
		}
	}
}

// applyOp 把一条配置变更应用到内存中的 configs 历史（已加锁上下文）。
func (sm *ShardMaster) applyOp(op Op) {
	last := sm.configs[len(sm.configs)-1]
	// 从上一版配置继承分片映射：Join/Leave 随后会用 rebalance 整体重写，
	// 而 Move 只改一个分片、其余必须保留——否则 Move 后的新配置里其余分片
	// 会被清零成"未分配(0)"，导致所有 replica group 丢失分片所有权而卡死。
	newCfg := Config{Num: last.Num + 1, Groups: copyGroups(last.Groups), Shards: last.Shards}
	switch op.Kind {
	case "Join":
		for gid, srvs := range op.Servers {
			cp := append([]string{}, srvs...)
			newCfg.Groups[gid] = cp
		}
		rebalance(&newCfg)
	case "Leave":
		for _, gid := range op.Gids {
			delete(newCfg.Groups, gid)
		}
		rebalance(&newCfg)
	case "Move":
		newCfg.Shards[op.Shard] = op.Gid
	}
	sm.configs = append(sm.configs, newCfg)
}

// rebalance 在保持尽可能多已有 shard→gid 分配的前提下，把 NShards 个分片
// 尽量均匀地分给当前所有 group（负载差不超过 ±1）。完全确定性：gids 按升序
// 处理，分片按下标顺序扫描。Move 的单分片覆盖在 applyOp 中完成，不调用本函数。
// 当 group 数量变化时，仅搬动"必须搬动"的分片（被移除 group 的碎片与超出目标
// 负载的碎片），从而把配置变更的扰动降到最小。
func rebalance(cfg *Config) {
	gids := make([]int, 0, len(cfg.Groups))
	for g := range cfg.Groups {
		gids = append(gids, g)
	}
	sort.Ints(gids)
	ng := len(gids)
	if ng == 0 {
		for i := range cfg.Shards {
			cfg.Shards[i] = 0
		}
		return
	}
	idxOf := make(map[int]int, ng)
	for i, g := range gids {
		idxOf[g] = i
	}
	// 目标负载：前 extra 个 group 各得 base+1，其余各得 base（差不超过 ±1）。
	load := make([]int, ng)
	target := make([]int, ng)
	base := NShards / ng
	extra := NShards % ng
	for i := 0; i < ng; i++ {
		if i < extra {
			target[i] = base + 1
		} else {
			target[i] = base
		}
	}
	// 统计当前仍有效的分配负载；gid 已不在 Groups 的碎片标记为待重分配(-1)。
	for i := 0; i < NShards; i++ {
		g := cfg.Shards[i]
		if gi, ok := idxOf[g]; ok {
			load[gi]++
		} else {
			cfg.Shards[i] = -1
		}
	}
	// 把超额 group 上多出的碎片释放出来（按下标顺序释放其名下分片）。
	for gi := 0; gi < ng; gi++ {
		for load[gi] > target[gi] {
			for i := 0; i < NShards; i++ {
				if cfg.Shards[i] == gids[gi] {
					cfg.Shards[i] = -1
					load[gi]--
					break
				}
			}
		}
	}
	// 把每个待重分配碎片交给"当前负载最低且尚未达标"的 group。
	for i := 0; i < NShards; i++ {
		if cfg.Shards[i] != -1 {
			continue
		}
		best := -1
		for gi := 0; gi < ng; gi++ {
			if load[gi] >= target[gi] {
				continue
			}
			if best == -1 || load[gi] < load[best] {
				best = gi
			}
		}
		if best == -1 {
			best = 0 // 理论上不会触发（总有未达标 group）
		}
		cfg.Shards[i] = gids[best]
		load[best]++
	}
}

func copyGroups(src map[int][]string) map[int][]string {
	dst := make(map[int][]string, len(src))
	for g, s := range src {
		dst[g] = append([]string{}, s...)
	}
	return dst
}

// ============================== 输入校验（I6） ==============================

// validateJoin：gid 必须 > 0；每个 servers 条目必须非空；且不可重复加入已存在的 gid。
func validateJoin(groups map[int][]string, servers map[int][]string) bool {
	if len(servers) == 0 {
		return false
	}
	for gid, srvs := range servers {
		if gid <= 0 {
			return false
		}
		if len(srvs) == 0 {
			return false
		}
		if _, exists := groups[gid]; exists {
			return false
		}
	}
	return true
}

// validateLeave：每个待移除 gid 必须当前存在于 Groups 中（空列表视为非法）。
func validateLeave(groups map[int][]string, gids []int) bool {
	if len(gids) == 0 {
		return false
	}
	seen := map[int]bool{}
	for _, g := range gids {
		if g <= 0 {
			return false
		}
		if seen[g] {
			return false
		}
		seen[g] = true
		if _, exists := groups[g]; !exists {
			return false
		}
	}
	return true
}

// validateMove：分片下标必须在 [0, NShards)；目标 gid 必须存在于当前 Groups。
func validateMove(groups map[int][]string, shard, gid int) bool {
	if shard < 0 || shard >= NShards {
		return false
	}
	if _, exists := groups[gid]; !exists {
		return false
	}
	return true
}

// ============================== RPC：Join / Leave / Move ==============================

// propose 把 op 提交到 Raft 并等待其被应用（或超时）。返回最终错误码。
func (sm *ShardMaster) propose(op Op) Err {
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		return ErrWrongLeader
	}
	// 在 Start 之前分配并注册 NotifyId，彻底消除"丢失唤醒"竞态。
	nid := atomic.AddInt64(&sm.notifyId, 1)
	op.NotifyId = nid
	ch := make(chan struct{}, 1)
	sm.mu.Lock()
	sm.notified[nid] = ch
	sm.mu.Unlock()

	_, _, ok := sm.rf.Start(op)
	if !ok {
		sm.mu.Lock()
		delete(sm.notified, nid)
		sm.mu.Unlock()
		return ErrWrongLeader
	}

	select {
	case <-ch:
		return OK
	case <-time.After(3 * time.Second):
		sm.mu.Lock()
		delete(sm.notified, nid)
		sm.mu.Unlock()
		return ErrTimeout
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	sm.mu.Lock()
	ok := validateJoin(sm.configs[len(sm.configs)-1].Groups, args.Servers)
	sm.mu.Unlock()
	if !ok {
		reply.Err = ErrInvalid
		return
	}
	reply.Err = sm.propose(Op{Kind: "Join", CkId: args.CkId, Seq: args.Seq, Servers: args.Servers})
}
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	sm.mu.Lock()
	ok := validateLeave(sm.configs[len(sm.configs)-1].Groups, args.Gids)
	sm.mu.Unlock()
	if !ok {
		reply.Err = ErrInvalid
		return
	}
	reply.Err = sm.propose(Op{Kind: "Leave", CkId: args.CkId, Seq: args.Seq, Gids: args.Gids})
}
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	sm.mu.Lock()
	ok := validateMove(sm.configs[len(sm.configs)-1].Groups, args.Shard, args.Gid)
	sm.mu.Unlock()
	if !ok {
		reply.Err = ErrInvalid
		return
	}
	reply.Err = sm.propose(Op{Kind: "Move", CkId: args.CkId, Seq: args.Seq, Shard: args.Shard, Gid: args.Gid})
}

// Query 直接返回已提交的内存配置（不进 Raft，只读最新提交态）。
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	sm.mu.Lock()
	n := len(sm.configs)
	if args.Num < 0 || args.Num >= n {
		reply.Config = sm.configs[n-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
	sm.mu.Unlock()
	reply.Err = OK
}

// ============================== Clerk（客户端） ==============================

type Clerk struct {
	mu       sync.Mutex
	sm       []string // 配置服务各 server 的名字
	make_end func(string) *raft.ClientEnd
	clientId int64
	seq      int64
}

func MakeClerk(sm []string, make_end func(string) *raft.ClientEnd) *Clerk {
	return &Clerk{sm: sm, make_end: make_end, clientId: nrand(), seq: 0}
}

func (ck *Clerk) Query(num int) Config {
	for {
		for _, name := range ck.sm {
			end := ck.make_end(name)
			args := &QueryArgs{Num: num}
			reply := &QueryReply{}
			if end.Call("ShardMaster.Query", args, reply) && reply.Err == OK {
				return reply.Config
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	ck.seq++
	seq := ck.seq
	ck.mu.Unlock()
	for {
		for _, name := range ck.sm {
			end := ck.make_end(name)
			args := &JoinArgs{Servers: servers, CkId: ck.clientId, Seq: seq}
			reply := &JoinReply{}
			if end.Call("ShardMaster.Join", args, reply) && reply.Err == OK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	ck.seq++
	seq := ck.seq
	ck.mu.Unlock()
	for {
		for _, name := range ck.sm {
			end := ck.make_end(name)
			args := &LeaveArgs{Gids: gids, CkId: ck.clientId, Seq: seq}
			reply := &LeaveReply{}
			if end.Call("ShardMaster.Leave", args, reply) && reply.Err == OK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	ck.seq++
	seq := ck.seq
	ck.mu.Unlock()
	for {
		for _, name := range ck.sm {
			end := ck.make_end(name)
			args := &MoveArgs{Shard: shard, Gid: gid, CkId: ck.clientId, Seq: seq}
			reply := &MoveReply{}
			if end.Call("ShardMaster.Move", args, reply) && reply.Err == OK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ============================== 小工具 ==============================

func nrand() int64 {
	return rand.Int63()
}

func (sm *ShardMaster) String() string {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var b bytes.Buffer
	fmt.Fprintf(&b, "sm[%d] configs=%d latestNum=%d\n", sm.me, len(sm.configs), sm.configs[len(sm.configs)-1].Num)
	return b.String()
}
