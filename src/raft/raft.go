// raft.go —— 从零实现 Raft 共识算法（对照 Raft 论文 Figure 2）
// 涵盖 Lab2A 选举 / 2B 日志复制 / 2C 持久化 / 2D 日志压缩(Snapshot)。
package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/src/metrics"
)

// Metrics 是 Raft 组件的可观测性指标（best-effort 进程级聚合）。
// 网关 / 演示程序可读取 raft.Metrics.Snapshot() 查看领导者变更、日志应用等。
var Metrics = metrics.NewRegistry()

// ============================== 常量与类型 ==============================

const (
	ElectionTimeoutMin = 260 * time.Millisecond
	ElectionTimeoutMax = 480 * time.Millisecond
	HeartbeatInterval  = 110 * time.Millisecond
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Unknown"
}

// LogEntry 是一条日志项。Command 为 nil 时代表占位（无客户端命令）。
type LogEntry struct {
	Term    int
	Command interface{}
}

// ApplyMsg 是提交后送给状态机的消息。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// ============================== RPC 参数 ==============================

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestPreVoteArgs / RequestPreVoteReply 是 Pre-Vote（预投票）扩展的 RPC
// 参数（Diego Ongaro 的 Raft 扩展）。候选人在正式自增任期、广播 RequestVote 之前，
// 先以"意向任期" currentTerm+1 征求多数派意向，从而避免抬升任期去扰动稳定 leader。
type RequestPreVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestPreVoteReply struct {
	Term        int
	VoteGranted bool
}

// TimeoutNowArgs / TimeoutNowReply 是领导权转移（Leadership Transfer）扩展的 RPC：
// leader 在让位前给目标节点发送 TimeoutNow，目标据此立即越过选举超时发起选举，
// 从而平滑地把领导权交给日志最新、最适合的节点（如用于负载再平衡）。
type TimeoutNowArgs struct {
	Term int
}

type TimeoutNowReply struct {
	Term int
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// 冲突回退信息（仿 6.824），让 leader 快速调整 nextIndex。
	ConflictTerm  int
	ConflictIndex int
}

// InstallSnapshot RPC（leader 把快照推给落后 follower）。
type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// ============================== Raft 结构体 ==============================

type Raft struct {
	mu        sync.Mutex
	peers     []*ClientEnd
	persister *Persister
	me        int
	dead      int32
	applyCh   chan ApplyMsg

	applyCond *sync.Cond
	killCh    chan struct{}

	// ---- 持久化状态（论文 Figure 2 的 persistent state）----
	currentTerm int
	votedFor    int
	log         []LogEntry

	// ---- 易失状态 ----
	commitIndex int
	lastApplied int
	role        Role
	// committedCurrentTerm 标记本 leader 是否已在「当前任期」提交过条目（通常为
	// becomeLeader 时追加的 no-op）。Raft 提交安全性要求：leader 只能经由提交
	// 当前任期条目来间接提交旧任期条目。故该标记置位前，commitIndex 可能仍落后
	// 于上一任 leader 已提交的位置、旧任期已提交写尚未 apply——此时若对外服务
	// 读/迁移传输，会传出陈旧快照造成丢写。GetShard 据此守卫（详见 shardkv）。
	committedCurrentTerm bool

	// preVoteWon 标记本轮"预投票"是否已转化为正式选举，用于防止同一轮预投票的
	// 多个多数派回包并发触发两次正式选举（doRealElection 的 exactly-once 守卫）。
	preVoteWon bool

	nextIndex  []int
	matchIndex []int

	// ---- leader 租约（线性一致读 ReadIndex 快路径用）----
	// lastContact[i] 记录本节点最后一次「接触」peer i 的时间：follower 在收到合法
	// leader 的 AppendEntries/InstallSnapshot 时更新 lastContact[LeaderId]；leader
	// 在收到 peer i 的成功 AE/IS 应答及自身每次心跳时更新 lastContact[i/me]。
	// HasLeaderLease 据此判断 leader 是否在 ElectionTimeoutMin 内仍与多数派保持接触。
	lastContact []time.Time

	// ---- 选举/心跳计时 ----
	// timerMu 保护 electionTimer/heartbeatTimer 的 Reset/Stop：ticker 与选举/心跳
	// goroutine 都会改动这两个 Timer，而 time.Timer 并非并发安全；不加锁在 -race 下
	// 会被判为数据竞争。注意锁序始终 timerMu 在外、与 rf.mu 不形成环（见 reset 函数）。
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	timerMu        sync.Mutex

	// ---- 快照（2D）----
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
}

// ============================== 日志索引辅助 ==============================

// lastLogIndex 返回最后一条日志的索引。
func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log)
}

// lastLogTerm 返回最后一条日志的任期。
func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

// entryTerm 返回索引 idx 处日志项的任期（需 idx >= lastIncludedIndex）。
func (rf *Raft) entryTerm(idx int) int {
	if idx == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	if idx < rf.lastIncludedIndex {
		return -1 // 已不在内存日志中
	}
	return rf.log[idx-rf.lastIncludedIndex-1].Term
}

// ============================== 持久化 ==============================

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var term int
	var voted int
	var logs []LogEntry
	var lii, lit, commit int
	if d.Decode(&term) != nil || d.Decode(&voted) != nil ||
		d.Decode(&logs) != nil || d.Decode(&lii) != nil || d.Decode(&lit) != nil ||
		d.Decode(&commit) != nil {
		// 损坏的持久化数据，忽略
		return
	}
	rf.currentTerm = term
	rf.votedFor = voted
	rf.log = logs
	rf.lastIncludedIndex = lii
	rf.lastIncludedTerm = lit
	rf.commitIndex = commit
}

// ============================== 对外接口 ==============================

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// ReadIndex 返回 leader 当前的 commitIndex 与是否仍为主。
// 供上层（ShardKV）实现线性一致读优化：以 commitIndex 为一致性点，等待本组
// 状态机 apply 到该索引后直接读本地状态，省去一次日志追加。
func (rf *Raft) ReadIndex() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex, rf.role == Leader
}

// HasLeaderLease 返回 leader 是否仍持有多数派最近接触的心跳租约。
// 用于支持线性一致读（ReadIndex 快路径）：仅当租约有效时，leader 才能基于
// commitIndex 安全地本地读，否则可能返回落后/陈旧数据（分区下旧 leader 的
// stale read 问题）。租约时长取选举超时最小值 ElectionTimeoutMin。
func (rf *Raft) HasLeaderLease() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return false
	}
	lease := ElectionTimeoutMin
	contacted := 0
	for i := range rf.peers {
		if i == rf.me {
			contacted++
			continue
		}
		if !rf.lastContact[i].IsZero() && time.Since(rf.lastContact[i]) <= lease {
			contacted++
		}
	}
	return contacted > len(rf.peers)/2
}

// HasCommittedCurrentTerm 返回 leader 是否已在当前任期提交过条目（通常为 no-op）。
// 仅当该标记为 true 时，commitIndex 才已覆盖本任期 no-op，从而"拉动"所有先前
// 已提交的旧任期写——此时对外服务读/迁移传输才是安全的（不会传出旧 leader 已提交
// 但尚未 apply 的陈旧快照）。新 leader 在重新提交 no-op 前该标记恒为 false，
// 用于 GetShard 的传输守卫（见 shardkv.go）。
func (rf *Raft) HasCommittedCurrentTerm() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.committedCurrentTerm
}

// LastApplied 返回已应用到状态机的最后索引（测试用，用于断言未达多数时不提交）。
func (rf *Raft) LastApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied
}

// RaftStateSize 返回当前持久化 Raft 状态（日志等）的字节大小，
// 供 KV 层判断何时需要快照压缩（Lab 2D ↔ KV 集成）。
func (rf *Raft) RaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.persister.ReadRaftState())
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
	select {
	case <-rf.killCh:
	default:
		close(rf.killCh)
	}
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// Start 把一条客户端命令追加到本节点日志（仅 leader 生效）。
// 返回值：(命令的最终索引, 当前任期, 是否为 leader)。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}
	idx := rf.lastLogIndex() + 1
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist()
	// 复制由心跳计时器（~110ms）触发，避免持锁发 RPC 造成死锁。
	return idx, rf.currentTerm, true
}

// ============================== 选举 ==============================

func (rf *Raft) resetElectionTimer() {
	d := ElectionTimeoutMin + time.Duration(rand.Int63n(int64(ElectionTimeoutMax-ElectionTimeoutMin)))
	rf.timerMu.Lock()
	defer rf.timerMu.Unlock()
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(d)
}

// startElection 进入选举流程。先发起 Pre-Vote（预投票）：以"意向任期" currentTerm+1
// 征求多数派意向，不抬升自身任期、不持久化 votedFor。仅当拿到多数派预投票授权后，
// 才调用 doRealElection 真正自增任期并广播 RequestVote。这样，日志落后或处于少数派
// 分区的节点永远拿不到多数预投票，也就永远不会抬升任期去扰动稳定 leader。
func (rf *Raft) startElection() {
	rf.mu.Lock()
	// 预投票意向任期：当前任期 +1。整个预投票阶段不修改 currentTerm。
	preTerm := rf.currentTerm + 1
	rf.preVoteWon = false
	lastIdx := rf.lastLogIndex()
	lastTerm := rf.lastLogTerm()
	me := rf.me
	rf.mu.Unlock()

	rf.resetElectionTimer()

	preVotes := 1 // 自己默认算一票
	var pmu sync.Mutex
	for i := range rf.peers {
		if i == me {
			continue
		}
		args := &RequestPreVoteArgs{
			Term:         preTerm,
			CandidateId:  me,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		}
		go func(i int, args *RequestPreVoteArgs) {
			reply := &RequestPreVoteReply{}
			ok := rf.peers[i].Call("RequestPreVote", args, reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term)
			}
			rf.mu.Unlock()
			if !reply.VoteGranted {
				return
			}
			pmu.Lock()
			preVotes++
			got := preVotes
			pmu.Unlock()
			if got == len(rf.peers)/2+1 {
				rf.doRealElection(preTerm, lastIdx, lastTerm, me)
			}
		}(i, args)
	}
}

// doRealElection 仅在 Pre-Vote 获得多数派授权后才进入正式选举：真正抬升任期、
// 自投、广播 RequestVote。期间若出现更高任期（其他节点已成 leader）则放弃。
// preVoteWon 守卫保证同一轮预投票只转化一次正式选举。
func (rf *Raft) doRealElection(preTerm int, lastIdx, lastTerm, me int) {
	rf.mu.Lock()
	// 仅当本节点任期仍等于"预投票意向任期 - 1"时方可推进；否则说明期间出现了
	// 更高任期（其他节点已成 leader），放弃本次选举，避免重复/冲突的正式选举。
	if rf.currentTerm != preTerm-1 || rf.preVoteWon {
		rf.mu.Unlock()
		return
	}
	rf.preVoteWon = true
	rf.currentTerm = preTerm
	rf.role = Candidate
	rf.votedFor = me
	rf.persist()
	term := rf.currentTerm
	rf.mu.Unlock()

	rf.resetElectionTimer()

	votes := 1 // 自己投自己
	var mu sync.Mutex
	for i := range rf.peers {
		if i == me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         term,
			CandidateId:  me,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		}
		go func(i int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			ok := rf.peers[i].Call("RequestVote", args, reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term)
				return
			}
			if args.Term == rf.currentTerm && rf.role == Candidate && reply.VoteGranted {
				mu.Lock()
				votes++
				got := votes
				mu.Unlock()
				if got == len(rf.peers)/2+1 {
					rf.becomeLeader()
				}
			}
		}(i, args)
	}
}

// LeadershipTransfer 把本节点（须为 leader）的领导权平滑移交给 target 节点。
// 流程：先确保 target 已追上本任期已提交位置（必要时触发一次复制并短暂等待），
// 再发 TimeoutNow 让 target 立即选举，最后主动以更高任期退位让路。用于负载再平衡
// 或计划内维护（把 leader 挪到更合适的节点）。返回 false 表示无法/未执行转移。
func (rf *Raft) LeadershipTransfer(target int) bool {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return false
	}
	if target == rf.me || target < 0 || target >= len(rf.peers) {
		rf.mu.Unlock()
		return false
	}
	needSync := rf.matchIndex[target] < rf.commitIndex
	term := rf.currentTerm
	rf.mu.Unlock()

	if needSync {
		rf.broadcastAppendEntries()
		// 等待目标追上已提交位置（最多 500ms）。
		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			rf.mu.Lock()
			role := rf.role
			caughtUp := rf.matchIndex[target] >= rf.commitIndex
			curTerm := rf.currentTerm
			rf.mu.Unlock()
			if role != Leader || curTerm != term {
				return false
			}
			if caughtUp {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		rf.mu.Lock()
		caughtUp := rf.matchIndex[target] >= rf.commitIndex && rf.role == Leader && rf.currentTerm == term
		rf.mu.Unlock()
		if !caughtUp {
			return false
		}
	}

	reply := &TimeoutNowReply{}
	ok := rf.peers[target].Call("TimeoutNow", &TimeoutNowArgs{Term: term}, reply)
	if !ok {
		return false
	}
	// 退位并以更高任期让路，target 随即赢得选举。
	rf.mu.Lock()
	rf.stepDown(term + 1)
	rf.mu.Unlock()
	Metrics.Counter("leadership_transfers").Inc()
	return true
}

func (rf *Raft) becomeLeader() {
	if rf.role == Leader {
		return
	}
	rf.role = Leader
	// 新任期必须重新提交一条当前任期 no-op 才能提交旧任期条目；重置该标记，
	// 确保 GetShard 等传输守卫在重新提交 no-op 之前不会传出陈旧快照。
	rf.committedCurrentTerm = false
	rf.preVoteWon = false
	rf.lastContact[rf.me] = time.Now()
	Metrics.Counter("leader_changes").Inc()
	// 任期开始时追加一条 no-op（空命令）。按 Raft 提交规则，leader 只能
	// 通过提交"当前任期"的条目来间接提交旧任期的日志；no-op 作为当前任期的
	// 第一条条目，被多数派复制并提交后即可"拉动"先前未提交的旧条目。
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: nil})
	rf.persist()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	last := rf.lastLogIndex()
	for i := range rf.nextIndex {
		rf.nextIndex[i] = last + 1
		rf.matchIndex[i] = 0
	}
	rf.resetHeartbeatTimer()
	// 注意：becomeLeader 在持锁上下文中被调用，不能在此直接发 RPC，
	// 复制由心跳计时器（~110ms）触发，正确且无死锁。
}

// stepDown 发现更高任期时退位为 follower。
func (rf *Raft) stepDown(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
	rf.role = Follower
	rf.preVoteWon = false
	rf.resetElectionTimer()
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	// 日志至少和自己一样新，且尚未投给别人
	upToDate := (args.LastLogTerm > rf.lastLogTerm()) ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())
	grant := false
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		grant = true
		rf.resetElectionTimer() // 听到候选人，刷新选举计时
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = grant
}

// RequestPreVote 处理 Pre-Vote（预投票）请求。与 RequestVote 的关键区别：不持久化
// votedFor、不抬升 currentTerm。仅当候选人日志至少与自己一样新、且意向任期 >= 当前
// 任期时才授权。这样，日志落后或处于少数派分区的节点永远拿不到多数预投票，也就永远
// 不会抬升任期去扰动稳定 leader（避免无谓的 leader 翻腾与客户端请求被重定向）。
// 只有确实能与多数派通信且日志够新的节点才会获得预投票、进而进入正式选举。
func (rf *Raft) RequestPreVote(args *RequestPreVoteArgs, reply *RequestPreVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// 预投票不持久化任何状态、不抬升 currentTerm（区别于正式 RequestVote）。
	upToDate := (args.LastLogTerm > rf.lastLogTerm()) ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())
	reply.Term = rf.currentTerm
	reply.VoteGranted = upToDate
}

// TimeoutNow 处理领导权转移请求：接收方立即越过选举超时发起选举（以当前任期+1 参选），
// 从而平滑地从当前 leader 接管领导权。发送方（旧 leader）在调用后会主动退位让路。
func (rf *Raft) TimeoutNow(args *TimeoutNowArgs, reply *TimeoutNowReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	// 跨过选举超时，立即发起选举（Pre-Vote → 正式选举）。
	rf.startElection()
}

// ============================== 日志复制 ==============================

func (rf *Raft) resetHeartbeatTimer() {
	rf.timerMu.Lock()
	defer rf.timerMu.Unlock()
	if !rf.heartbeatTimer.Stop() {
		select {
		case <-rf.heartbeatTimer.C:
		default:
		}
	}
	rf.heartbeatTimer.Reset(HeartbeatInterval)
}

// advanceCommit 依据 matchIndex 推进 commitIndex（多数派复制到的位置才提交）。
func (rf *Raft) advanceCommit() {
	if rf.role != Leader {
		return
	}
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 && rf.entryTerm(n) == rf.currentTerm {
			rf.commitIndex = n
			rf.committedCurrentTerm = true // 当前任期条目已提交，旧任期写现可安全服务
			rf.persist() // 持久化提交点：崩溃重启后据 commitIndex 重放已提交条目
			rf.applyCond.Broadcast()
			break
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	me := rf.me
	term := rf.currentTerm
	commitIdx := rf.commitIndex
	lastIncludedIndex := rf.lastIncludedIndex
	snap := rf.snapshot
	snapTerm := rf.lastIncludedTerm

	for i := range rf.peers {
		if i == me {
			continue
		}
		nextIdx := rf.nextIndex[i]
		if nextIdx <= lastIncludedIndex {
			// follower 落后到快照之前，发快照
			args := &InstallSnapshotArgs{
				Term:              term,
				LeaderId:          me,
				LastIncludedIndex: lastIncludedIndex,
				LastIncludedTerm:  snapTerm,
				Data:              snap,
			}
			go func(i int, args *InstallSnapshotArgs) {
				reply := &InstallSnapshotReply{}
				ok := rf.peers[i].Call("InstallSnapshot", args, reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.stepDown(reply.Term)
					return
				}
				if rf.role == Leader && args.Term == rf.currentTerm {
					rf.lastContact[i] = time.Now()
					rf.matchIndex[i] = args.LastIncludedIndex
					rf.nextIndex[i] = args.LastIncludedIndex + 1
				}
			}(i, args)
			continue
		}

		prevIdx := nextIdx - 1
		prevTerm := rf.entryTerm(prevIdx)
		var entries []LogEntry
		if nextIdx <= rf.lastLogIndex() {
			entries = append(entries, rf.log[nextIdx-lastIncludedIndex-1:]...)
		}
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: commitIdx,
		}
		go func(i int, args *AppendEntriesArgs) {
			reply := &AppendEntriesReply{}
			ok := rf.peers[i].Call("AppendEntries", args, reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term)
				return
			}
			if rf.role == Leader && args.Term == rf.currentTerm {
				rf.lastContact[i] = time.Now()
				if reply.Success {
					rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					rf.advanceCommit()
				} else {
					// 冲突回退：跳到冲突任期的第一条
					if reply.ConflictTerm != 0 {
						localIdx := rf.firstIndexWithTerm(reply.ConflictTerm)
						if localIdx != -1 {
							rf.nextIndex[i] = localIdx
						} else {
							rf.nextIndex[i] = reply.ConflictIndex
						}
					} else {
						rf.nextIndex[i] = reply.ConflictIndex
					}
					if rf.nextIndex[i] < 1 {
						rf.nextIndex[i] = 1
					}
				}
			}
		}(i, args)
	}
	rf.mu.Unlock()
}

func (rf *Raft) firstIndexWithTerm(term int) int {
	for i, e := range rf.log {
		if e.Term == term {
			return rf.lastIncludedIndex + 1 + i
		}
	}
	return -1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	// 听到 leader，刷新选举计时
	rf.resetElectionTimer()
	rf.lastContact[args.LeaderId] = time.Now()
	reply.Term = rf.currentTerm

	// 1) 日志一致性检查
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.Success = false
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return
	}
	if args.PrevLogIndex >= rf.lastIncludedIndex {
		localTerm := rf.entryTerm(args.PrevLogIndex)
		if localTerm != args.PrevLogTerm {
			// 任期冲突：告诉 leader 本节点该任期的第一条索引
			reply.Success = false
			reply.ConflictTerm = localTerm
			reply.ConflictIndex = rf.firstIndexWithTerm(localTerm)
			if reply.ConflictIndex == -1 {
				// 该任期已在快照里
				reply.ConflictIndex = rf.lastIncludedIndex + 1
			}
			return
		}
	}

	// 2) 追加新日志（处理冲突）
	newIdx := args.PrevLogIndex
	changed := false
	for _, entry := range args.Entries {
		newIdx++
		if newIdx <= rf.lastIncludedIndex {
			continue
		}
		pos := newIdx - rf.lastIncludedIndex - 1
		if pos < len(rf.log) && rf.log[pos].Term != entry.Term {
			rf.log = rf.log[:pos] // 截断冲突部分
			changed = true
		}
		if pos >= len(rf.log) {
			rf.log = append(rf.log, entry)
			changed = true
		}
	}
	// 仅当日志真正发生变化时才持久化；心跳（无新条目）无需重写整个状态。
	if changed {
		rf.persist()
	}

	// 3) 推进 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		last := rf.lastLogIndex()
		if args.LeaderCommit < last {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = last
		}
		rf.applyCond.Broadcast()
	}
	reply.Success = true
}

// ============================== 快照（2D）==============================

// Snapshot 由状态机调用，把已应用到 index 的状态压缩进快照。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}
	if index > rf.commitIndex {
		return // 不能快照尚未提交的部分
	}
	pos := index - rf.lastIncludedIndex - 1
	rf.lastIncludedTerm = rf.log[pos].Term
	// 保留 index 之后的日志
	rf.log = append([]LogEntry{}, rf.log[pos+1:]...)
	rf.lastIncludedIndex = index
	rf.snapshot = snapshot
	rf.persister.SaveSnapshot(snapshot)
	rf.persist()
}

// CondInstallSnapshot 由状态机在收到 InstallSnapshot 后调用。
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.lastIncludedIndex {
		return true // 已经有更新的快照
	}
	if lastIncludedIndex <= rf.lastLogIndex() {
		pos := lastIncludedIndex - rf.lastIncludedIndex - 1
		if rf.log[pos].Term == lastIncludedTerm {
			// 保留后面的日志
			rf.log = append([]LogEntry{}, rf.log[pos+1:]...)
		} else {
			rf.log = nil
		}
	} else {
		rf.log = nil
	}
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.snapshot = snapshot
	if rf.commitIndex < lastIncludedIndex {
		rf.commitIndex = lastIncludedIndex
	}
	if rf.lastApplied < lastIncludedIndex {
		rf.lastApplied = lastIncludedIndex
	}
	rf.persister.SaveSnapshot(snapshot)
	rf.persist()
	return true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	rf.resetElectionTimer()
	rf.lastContact[args.LeaderId] = time.Now()
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	rf.CondInstallSnapshot(args.LastIncludedTerm, args.LastIncludedIndex, args.Data)
}

// ============================== 后台循环 ==============================

func (rf *Raft) ticker() {
	for {
		select {
		case <-rf.killCh:
			return
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.role != Leader {
				rf.mu.Unlock()
				rf.startElection()
			} else {
				rf.mu.Unlock()
			}
			rf.resetElectionTimer()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.role == Leader {
				rf.lastContact[rf.me] = time.Now()
				rf.mu.Unlock()
				rf.broadcastAppendEntries()
			} else {
				rf.mu.Unlock()
			}
			rf.resetHeartbeatTimer()
		}
	}
}

// applier 把已提交日志按序应用给状态机。
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for !rf.killed() && rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.lastApplied++
		idx := rf.lastApplied
		var msg ApplyMsg
		if idx <= rf.lastIncludedIndex {
			// 快照内的部分，用快照消息通知状态机
			msg = ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			Metrics.Counter("snapshots_installed").Inc()
		} else {
			pos := idx - rf.lastIncludedIndex - 1
			msg = ApplyMsg{
				CommandValid: true,
				Command:      rf.log[pos].Command,
				CommandIndex: idx,
			}
			Metrics.Counter("log_applied").Inc()
		}
		rf.mu.Unlock()
		rf.applyCh <- msg
	}
}

// ============================== Make ==============================

func Make(peers []*ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		applyCh:           applyCh,
		role:              Follower,
		currentTerm:       0,
		votedFor:          -1,
		commitIndex:       0,
		lastApplied:       0,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		lastContact:       make([]time.Time, len(peers)),
		electionTimer:     time.NewTimer(ElectionTimeoutMax),
		heartbeatTimer:    time.NewTimer(HeartbeatInterval),
		killCh:            make(chan struct{}),
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.readPersist(persister.ReadRaftState())
	if snap := persister.ReadSnapshot(); snap != nil {
		rf.snapshot = snap
	}

	go rf.ticker()
	go rf.applier()

	return rf
}

// 便于调试的字符串化
func (rf *Raft) String() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return fmt.Sprintf("id=%d role=%s term=%d logLen=%d commit=%d applied=%d",
		rf.me, rf.role, rf.currentTerm, len(rf.log), rf.commitIndex, rf.lastApplied)
}
