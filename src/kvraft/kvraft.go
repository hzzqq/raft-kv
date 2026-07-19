// kvraft.go —— Lab 3：基于 Raft 的容错 KV 存储（Get/Put/Append，线性一致）
// 复用 raft 包导出的 Network/ClientEnd/Raft/Make/ApplyMsg。
package kvraft

import (
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/src/raft"
)

// Op 需要向 gob 注册：Raft 把日志以 interface{} 形式持久化，
// 反序列化时必须知道具体类型，否则重启后日志变空（命令丢失）。
func init() {
	gob.Register(Op{})
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

// 应用层通知：applier 把"实际被提交的操作 + 结果"发给等待者，
// 等待者据此判断是否真的是自己提交的那条（防止 leader 切换导致错位）。
type applyResult struct {
	op     Op
	result OpResult
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	data       map[string]string
	lastSeq    map[int64]int64 // 每个 client 已应用的最大 Seq
	lastResult map[int64]string  // 每个 client 最近一次 Get 的返回值（去重用）
	notify     map[int]chan applyResult

	maxraftstate int // >0 时超过该字节数触发快照（本层可选）
}

func (kv *KVServer) Kill() {
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
		if !msg.CommandValid {
			continue
		}
		op, ok := msg.Command.(Op)
		if !ok {
			// no-op（leader 任期开始追加的空命令）：不更新状态机，直接跳过。
			continue
		}
		kv.mu.Lock()
		var res OpResult
		if lastSeq, exists := kv.lastSeq[op.ClientId]; !exists || op.Seq > lastSeq {
			switch op.OpType {
			case "Get":
				res.Value = kv.data[op.Key]
			case "Put":
				kv.data[op.Key] = op.Value
			case "Append":
				kv.data[op.Key] += op.Value
			}
			kv.lastSeq[op.ClientId] = op.Seq
			kv.lastResult[op.ClientId] = res.Value
		} else {
			// 重复命令：直接复用上次结果（幂等）
			res.Value = kv.lastResult[op.ClientId]
		}
		idx := msg.CommandIndex
		ch := kv.notify[idx]
		delete(kv.notify, idx)
		kv.mu.Unlock()
		if ch != nil {
			ch <- applyResult{op: op, result: res}
		}
	}
}

// waitApplied 等待本服务器把 op 提交到状态机并返回结果。
// 若超时、或该位置被别的命令占据（leader 切换），返回 WrongLeader 让客户端重试。
func (kv *KVServer) waitApplied(op Op, index int) OpResult {
	ch := make(chan applyResult, 1)
	kv.mu.Lock()
	kv.notify[index] = ch
	kv.mu.Unlock()

	select {
	case ar := <-ch:
		if ar.op.ClientId == op.ClientId && ar.op.Seq == op.Seq {
			return ar.result
		}
		return OpResult{Err: "mismatch"}
	case <-time.After(1 * time.Second):
		kv.mu.Lock()
		delete(kv.notify, index)
		kv.mu.Unlock()
		return OpResult{Err: "timeout"}
	}
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
	op := Op{Key: args.Key, OpType: "Get", ClientId: args.ClientId, Seq: args.Seq}
	index, ok := kv.startOp(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	res := kv.waitApplied(op, index)
	if res.Err != "" {
		reply.WrongLeader = true
		return
	}
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{Key: args.Key, Value: args.Value, OpType: args.Op, ClientId: args.ClientId, Seq: args.Seq}
	index, ok := kv.startOp(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	res := kv.waitApplied(op, index)
	if res.Err != "" {
		reply.WrongLeader = true
		return
	}
	reply.Err = res.Err
}

// ============================== 构造 ==============================

func MakeKVServer(me int, rf *raft.Raft, applyCh chan raft.ApplyMsg, maxraftstate int) *KVServer {
	kv := &KVServer{
		me:          me,
		rf:          rf,
		applyCh:     applyCh,
		data:        make(map[string]string),
		lastSeq:     make(map[int64]int64),
		lastResult:  make(map[int64]string),
		notify:      make(map[int]chan applyResult),
		maxraftstate: maxraftstate,
	}
	go kv.applier()
	return kv
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
	for {
		srv := ck.servers[ck.leaderHint]
		reply := &GetReply{}
		ok := srv.Call("Get", &op, reply)
		if ok && !reply.WrongLeader {
			return reply.Value
		}
		ck.leaderHint = (ck.leaderHint + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) PutAppend(key, value, op string) {
	ck.seq++
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, Seq: ck.seq}
	for {
		srv := ck.servers[ck.leaderHint]
		reply := &PutAppendReply{}
		ok := srv.Call("PutAppend", &args, reply)
		if ok && !reply.WrongLeader {
			return
		}
		ck.leaderHint = (ck.leaderHint + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
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
