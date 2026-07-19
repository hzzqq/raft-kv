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
}

// ============================== 构造 ==============================

func Make(peers []*raft.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := &ShardMaster{
		me:       me,
		applyCh:  make(chan raft.ApplyMsg, 100),
		configs:  make([]Config, 1),
		lastSeq:  make(map[int64]int64),
		notified: make(map[int64]chan struct{}),
	}
	sm.configs[0] = Config{Num: 0, Groups: map[int][]string{}}
	sm.rf = raft.Make(peers, me, persister, sm.applyCh)
	go sm.applier()
	return sm
}

func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
}

// RaftRPC 把网络层转发的 Raft 内部 RPC（RequestVote/AppendEntries/InstallSnapshot）
// 派发给底层 rf。测试框架用同一个 server id 同时承载"配置服务 RPC"与"Raft RPC"，
// 因此 handler 需要把两者都分发到正确接收方。
func (sm *ShardMaster) RaftRPC(method string, args, reply interface{}) {
	switch method {
	case "RequestVote":
		sm.rf.RequestVote(args.(*raft.RequestVoteArgs), reply.(*raft.RequestVoteReply))
	case "AppendEntries":
		sm.rf.AppendEntries(args.(*raft.AppendEntriesArgs), reply.(*raft.AppendEntriesReply))
	case "InstallSnapshot":
		sm.rf.InstallSnapshot(args.(*raft.InstallSnapshotArgs), reply.(*raft.InstallSnapshotReply))
	}
}
func (sm *ShardMaster) killed() bool { return atomic.LoadInt32(&sm.dead) == 1 }

// ============================== applier ==============================

func (sm *ShardMaster) applier() {
	for !sm.killed() {
		msg, ok := <-sm.applyCh
		if !ok {
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
	newCfg := Config{Num: last.Num + 1, Groups: copyGroups(last.Groups)}
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

// rebalance 把 NShards 个分片尽量均匀地分给当前所有 group（确定性轮转）。
func rebalance(cfg *Config) {
	gids := make([]int, 0, len(cfg.Groups))
	for g := range cfg.Groups {
		gids = append(gids, g)
	}
	sort.Ints(gids)
	if len(gids) == 0 {
		for i := range cfg.Shards {
			cfg.Shards[i] = 0
		}
		return
	}
	for i := range cfg.Shards {
		cfg.Shards[i] = gids[i%len(gids)]
	}
}

func copyGroups(src map[int][]string) map[int][]string {
	dst := make(map[int][]string, len(src))
	for g, s := range src {
		dst[g] = append([]string{}, s...)
	}
	return dst
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
	reply.Err = sm.propose(Op{Kind: "Join", CkId: args.CkId, Seq: args.Seq, Servers: args.Servers})
}
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	reply.Err = sm.propose(Op{Kind: "Leave", CkId: args.CkId, Seq: args.Seq, Gids: args.Gids})
}
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
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
