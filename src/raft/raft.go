// raft.go —— 从零实现 Raft 共识算法（对照 Raft 论文 Figure 2）
// 涵盖 Lab2A 选举 / 2B 日志复制 / 2C 持久化 / 2D 日志压缩(Snapshot)。
package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"raftkv/src/metrics"
)

// Metrics 是 Raft 组件的可观测性指标（best-effort 进程级聚合）。
// 网关 / 演示程序可读取 raft.Metrics.Snapshot() 查看领导者变更、日志应用等。
var Metrics = metrics.NewRegistry()

// raftDebug 选举诊断日志开关：RAFT_DEBUG=1 时由 dbg 输出选举关键路径，便于定位
// 「kill 一投票副本后无新 leader 选出」类问题（见 deploy_smoke E5）。
var raftDebug = os.Getenv("RAFT_DEBUG") == "1"

func dbg(format string, args ...interface{}) {
	if raftDebug {
		fmt.Printf("[raft-debug] "+format+"\n", args...)
	}
}

func init() {
	// 注册成员变更命令类型（I192），使 gob 在跨进程 AppendEntries / 持久化时能正确
	// 编解码含 ConfChange 的日志条目（interface{} 中的具体类型须两端已知）。
	gob.Register(ConfChange{})
}

// ============================== 常量与类型 ==============================

const (
	ElectionTimeoutMin = 260 * time.Millisecond
	ElectionTimeoutMax = 480 * time.Millisecond
	HeartbeatInterval  = 110 * time.Millisecond
)

// Role 表示节点在共识中的角色（Follower/Candidate/Leader）。
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// String 返回 Role 的可读名称。
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

// ConfChange 是集群成员（投票配置）变更命令（Ongaro 论文 §6 单服变更）。
// NewVoters 为变更后「完整」的投票成员索引集合（peer 在 rf.peers 中的下标），
// 其中可含 witness（witness 是持日志、参与投票但不持状态机的成员）。applier 应用
// 该日志条目后原子切换 rf.cfg。一次仅允许一个进行中的成员变更（rf.pendingConf
// 守卫），保证任意时刻新旧配置只差一个成员、两者的多数派必重叠——这正是单服变更
// 安全性的充要条件（不会选出两个 leader、不会出现两个不相交的提交多数派）。
type ConfChange struct {
	NewVoters []int
}

// ============================== RPC 参数 ==============================

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply 是 RequestVote 的响应。
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

// RequestPreVoteReply 是预投票（PreVote）的响应。
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

// TimeoutNowReply 是 TimeoutNow 的响应（用于领导权转移）。
type TimeoutNowReply struct {
	Term int
}

// AppendEntriesArgs 是日志复制/心跳的入参。
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommit int
}

// AppendEntriesReply 是日志复制/心跳的响应。
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
	// LastIncludedConfig 是「快照点（LastIncludedIndex）处已提交的投票配置」。
	// 快照会丢弃 <=LastIncludedIndex 的全部日志，其中的 ConfChange 条目若不在快照里
	// 补上，接收方的 cfg/committedCfg 将永久停在旧配置（用错 quorum，可致双主）。
	// 为 nil 时（旧版本 leader / 测试构造）接收方保持原有配置不变。
	LastIncludedConfig []int
}

// InstallSnapshotReply 是安装快照的响应。
type InstallSnapshotReply struct {
	Term int
}

// ============================== Raft 结构体 ==============================

type Raft struct {
	mu        sync.Mutex
	peers     []*ClientEnd
	persister Persister
	me        int
	dead      int32
	applyCh   chan ApplyMsg

	applyCond *sync.Cond
	killCh    chan struct{}
	// kickCh 让 Start() 立刻唤醒一次复制，而不必等下一次心跳 tick（缓冲 1，天然合批：
	// 高并发写只会触发一次广播，多条新日志随同一批 AppendEntries 出去）。
	// 不加这个唤醒时，单次写入延迟被钉在心跳间隔（实测 p50≈123ms），吞吐等于
	// 并发数 ÷ 心跳间隔，与分片数无关——这是 experiments 场景 C 量出来的真实瓶颈。
	kickCh chan struct{}
	// leaderElections 是本副本赢得选举的累计次数（mu 保护），经 Status() 外传，
	// 供跨进程部署的 Prometheus 按 group/replica 观测 leader 切换（I152）。
	leaderElections int

	// ---- 持久化状态（论文 Figure 2 的 persistent state）----
	currentTerm int
	votedFor    int
	log         []LogEntry

	// ---- 易失状态 ----
	commitIndex int
	lastApplied int
	role        Role
	// isWitness 标记本副本是 Witness（见证者）副本（I189）。
	//
	// Witness 持有一份完整的 Raft 日志并参与投票，但**不持有状态机数据**：
	//  - 接收并持久化全部日志条目（故选举时 up-to-date 判断与日志安全性完全不变）；
	//  - 正常响应 RequestVote / RequestPreVote（这份票权正是它的全部价值）；
	//  - 不向 applyCh 投递任何消息（状态机恒为空，省下一份全量数据存储）；
	//  - 永不发起选举、拒绝 TimeoutNow（无数据，让它当 leader 会对外服务空状态机）。
	//
	// 收益：用 1 份「只存日志」的廉价副本补上投票权。典型部署 2 数据副本 + 1 witness
	// （3 节点 quorum=2）：任一数据副本宕机后，剩 1 数据 + 1 witness 仍达 quorum，
	// 集群仍可选主、提交写入、对外服务；而纯 2 数据副本（quorum=2）宕一即完全不可用。
	// 即：以 2 份数据存储的代价，换来 3 副本的容错能力。
	isWitness bool
	// leaderId 记录本节点当前认知的 leader 编号（仅 follower/candidate 视角有意义；
	// leader 自身在 Status() 中按 role 直接回报 me）。收到合法 AppendEntries 时更新，
	// 退位时清零，供运维/诊断在脑裂、任期翻滚时快速判断「我在跟谁」。
	leaderId int
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

	// ---- 成员配置（动态重配置，I192）----
	// cfg 用于 quorum 计票与选举的成员集合（rf.peers 的下标）。其更新时机刻意不对称：
	//   · follower 在 AppendEntries 追加到 ConfChange 条目时**立即**切到 C_new——这样旧
	//     leader 提交 C_new 后崩溃、存活 follower 也能以 C_new 凑齐 quorum 选主（E5 死锁根因）；
	//   · leader 在 ProposeConfChange 时**不**立即切换，而是沿用旧配置把 C_new 条目本身
	//     提交（Ongaro §6 安全变体，TestWitnessDynamicJoinLeave 依赖），待 applier 提交后才切；
	//   · applier 提交 ConfChange 时切到 C_new（与 follower 追加时一致，保证收敛）。
	// 即：follower 选举用「日志最新配置」，leader 提交用「旧配置」，二者在条目提交后收敛。
	// committedCfg 是「已提交」的投票成员集合，仅由 applier 在提交配置条目时切换，用于
	// 持久化与对外观测（VoterConfig）——绝不反映未提交配置，避免重启复活未生效的成员变更。
	// quorum = len(cfg)/2 + 1（计票用 cfg）；cfg 与 committedCfg 在配置条目提交后收敛到同一值。
	cfg []int
	// committedCfg 已提交的投票配置，仅 applier 提交配置条目时更新；persist/VoterConfig 用。
	committedCfg []int
	// snapshotCfg 是「快照点（lastIncludedIndex）对应的已提交投票配置」。
	// 存在的理由（快照吞配置 bug）：InstallSnapshot / Snapshot 会把 <=lastIncludedIndex
	// 的日志整段丢弃，其中的 ConfChange 条目因此既走不到 AppendEntries 的配置切换、
	// 也走不到 applier 的提交切换（applier 会把 lastApplied 直接跳到快照点）。若快照
	// 不自带配置，follower 的 cfg/committedCfg 将永久停在旧配置——用旧配置算 quorum，
	// 扩容后可能选出两个 leader（安全性破坏），缩容后则永远选不出主。
	// 因此快照必须携带自己那一点的配置，且随持久化落盘（重启后 leader 重发旧快照时
	// 也要带上正确配置）。
	snapshotCfg []int
	// pendingConf 标记 leader 已提议一个尚未提交的成员变更；期间禁止再提议新的变更，
	// 以保证单服变更安全性（新旧配置多数派必重叠，不会脑裂）。
	pendingConf bool

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
	// electionTimeoutFn 可注入的选举超时生成器（nil = 默认 [Min,Max) 均匀随机）。
	// 用途：跨机部署 RTT 更长时可注入更大区间；测试可注入确定性超时复现竞态时序。
	// 由 timerMu 保护（resetElectionTimer 的调用方常已持有 rf.mu，不能复用 rf.mu，
	// 否则死锁；timerMu 恰好是计时器域的既有锁，锁序不变）。
	electionTimeoutFn func() time.Duration

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
	// 持久化「已提交」配置 committedCfg：绝不序列化未提交的最新配置（rf.cfg 在 leader
	// 提议变更后即指向未提交配置），否则重启可能复活未生效的成员变更、破坏单服变更安全性。
	e.Encode(rf.committedCfg) // 已提交投票配置：重启后直接恢复，无需重放日志即可正确计 quorum
	// 快照点配置：必须与 committedCfg 一起落盘。否则重启后本节点重发历史快照时只能
	// 发空配置，落后的 follower 装完快照仍拿不到配置（快照吞配置 bug 的持久化缺口）。
	e.Encode(rf.snapshotCfg)
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
	// 防御：恢复出的 commitIndex 不得越过恢复后的日志末尾 lastIndex
	// （lii + len(logs)）。否则 applier 会把 lastApplied 推进到不存在的日志下标，
	// 触发 index out of range 越界 panic（见 applier else 分支）。持久化瞬间的
	// 不一致态在此一次性纠正，applier 启动前即拿到合法 commitIndex。
	if li := lii + len(logs); rf.commitIndex > li {
		rf.commitIndex = li
	}
	// 兼容旧格式：旧持久化数据不含 cfg 字段，Decode 在此返回 io.EOF，
	// rf.cfg 保留 makeRaft 里设定的默认（全部 peer），不影响正确性。
	var cfg []int
	if d.Decode(&cfg) == nil && len(cfg) > 0 {
		rf.cfg = cfg
		rf.committedCfg = append([]int(nil), cfg...)
	}
	// 兼容旧格式：旧持久化数据不含 snapshotCfg 字段，Decode 返回 io.EOF，此时退化用
	// committedCfg 作为快照点配置——旧格式下两者一致（旧代码没有快照携带配置的语义，
	// 快照点配置就等于当时恢复出来的已提交配置），不影响正确性。
	var snapCfg []int
	if d.Decode(&snapCfg) == nil && len(snapCfg) > 0 {
		rf.snapshotCfg = snapCfg
	} else if len(rf.snapshotCfg) == 0 {
		rf.snapshotCfg = append([]int(nil), rf.committedCfg...)
	}
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
	return rf.hasLeaderLeaseLocked()
}

// hasLeaderLeaseLocked 在调用方已持有 rf.mu 时判断 leader 是否在 ElectionTimeoutMin
// 内仍与多数派保持接触。抽出无锁版本供 Status() 在持锁态直接复用，避免重复加锁死锁。
func (rf *Raft) hasLeaderLeaseLocked() bool {
	if rf.role != Leader {
		return false
	}
	// 投票集合默认取当前配置 cfg；白盒单测可能直接构造 *Raft 而不设 cfg，
	// 此时回退到全部 peer，保持「多数派近期有接触」的语义不变。
	voters := rf.cfg
	if len(voters) == 0 {
		voters = make([]int, len(rf.peers))
		for i := range rf.peers {
			voters[i] = i
		}
	}
	lease := ElectionTimeoutMin
	contacted := 0
	for _, i := range voters {
		if i == rf.me {
			contacted++
			continue
		}
		if !rf.lastContact[i].IsZero() && time.Since(rf.lastContact[i]) <= lease {
			contacted++
		}
	}
	return contacted > len(voters)/2
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

// RaftStatus 是 Raft 节点运行时的只读快照，供运维/诊断/监控在不打扰共识热路径的
// 前提下读取关键状态。所有字段均在持 rf.mu 下采集，调用零副作用、可并发安全读取。
type RaftStatus struct {
	Me                   int  // 本节点编号
	Role                 Role // Follower / Candidate / Leader
	Term                 int  // 当前任期
	VotedFor             int  // 本任期投票对象（-1 表示未投）
	LeaderID             int  // 认知的 leader 编号（role==Leader 时为 me；未知为 -1）
	LastLogIndex         int  // 最后一条日志索引（含快照偏移）
	LastLogTerm          int  // 最后一条日志任期
	CommitIndex          int  // 已提交索引
	LastApplied          int  // 已应用索引
	CommittedCurrentTerm bool // leader 是否已在当前任期提交过条目（线性一致读守卫）
	HasLeaderLease       bool // leader 是否在 ElectionTimeoutMin 内与多数派保持接触
	// LeaderElections 是本副本自进程启动以来赢得选举（becomeLeader）的累计次数。
	// 单调不减，可直接被 Prometheus 当累计量 scrape：全组求和的增量即该 group 的
	// leader 切换次数（每次切换必有且仅有一个副本赢得选举）。跨进程部署时 gateway
	// 读不到远端节点内存，故必须随状态快照一起外传（I152）。
	LeaderElections int
	// 成员拓扑（I192 动态重配置运维可见性）：运维从 /status 与 Prometheus 一眼看清
	// 「谁在投票集合、谁是见证者、完整节点清单」，确认 witness Join/Leave 已生效。
	// 此前 RaftStatus 只暴露角色/任期/进度，成员配置是个黑盒——动态增删 witness 后
	// 运维无法确认变更是否真落地，只能另查 /admin/reconfigure。统一在此暴露，使
	// /status 成为成员变更观测的单一可信源。
	Voters    []int // 当前已提交投票成员集合（== VoterConfig()）
	Witnesses []int // 见证者：在 peers 内但不在 voters 内（参与投票/复制但不 apply）
	Peers     []int // 全部 raft 节点编号（0..len(peers)-1）
	IsWitness bool  // 本节点是否为见证者（不持状态机，只投票/复制）
}

// Status 返回当前节点的只读状态快照。仅在持锁下采集既有字段，不修改任何状态、
// 不触发任何 RPC，因此可在监控/诊断/排障路径高频调用（R6 可观测性——共识层
// 此前对运维完全不透明，脑裂/任期翻滚时无一手信号）。
func (rf *Raft) Status() RaftStatus {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	st := RaftStatus{
		Me:                   rf.me,
		Role:                 rf.role,
		Term:                 rf.currentTerm,
		VotedFor:             rf.votedFor,
		LeaderID:             rf.leaderId,
		CommitIndex:          rf.commitIndex,
		LastApplied:          rf.lastApplied,
		CommittedCurrentTerm: rf.committedCurrentTerm,
		HasLeaderLease:       rf.hasLeaderLeaseLocked(),
		LeaderElections:      rf.leaderElections,
	}
	if rf.role == Leader {
		st.LeaderID = rf.me
	}
	// 成员拓扑：默认以已提交配置为准（对外观测须反映已生效配置，与 VoterConfig 一致）。
	st.Voters = append([]int(nil), rf.committedCfg...)
	voterSet := make(map[int]bool, len(rf.committedCfg))
	for _, v := range rf.committedCfg {
		voterSet[v] = true
	}
	for i := 0; i < len(rf.peers); i++ {
		st.Peers = append(st.Peers, i)
		if !voterSet[i] {
			st.Witnesses = append(st.Witnesses, i)
		}
	}
	st.IsWitness = rf.isWitness
	// 日志索引/任期经 lastLogIndex/lastLogTerm 统一计入快照偏移（与 Start 一致）。
	st.LastLogIndex = rf.lastLogIndex()
	st.LastLogTerm = rf.lastLogTerm()
	return st
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

// Kill 关闭节点，停止选举/心跳/应用等后台协程，仅测试使用。
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
	// 控制面可观测性（best-effort，纯原子操作）：leader 追加日志条目计数，
	// 与 raft_log_applied(已应用) 区分，便于观察写入吞吐与复制滞后。
	Metrics.CounterWithHelp("raft_log_appends_total", "累计 leader 追加的日志条目数").Inc()
	rf.persist()
	// 立刻唤醒一次复制（非阻塞，缓冲 1 自动合批）。广播本身放到 ticker 里做，
	// 避免在持有 rf.mu 时发 RPC 造成死锁；心跳计时器仍是兜底路径。
	select {
	case rf.kickCh <- struct{}{}:
	default:
	}
	return idx, rf.currentTerm, true
}

// ============================== 成员配置（动态重配置，I192） ==============================

// inCfg 报告 peer i 是否属于当前已提交的投票配置（调用方持锁与否均可，读 cfg 切片）。
func (rf *Raft) inCfg(i int) bool {
	for _, v := range rf.cfg {
		if v == i {
			return true
		}
	}
	return false
}

// SetInitialConfig 在节点启动、参与选举前设定初始投票成员集合。
// 正常运行中变更成员必须走 ProposeConfChange（经 ConfChange 日志条目提交），不可直接
// 改 rf.cfg——否则会与已提交日志产生不一致、破坏单服变更安全性。仅测试/引导阶段使用。
func (rf *Raft) SetInitialConfig(voters []int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.cfg = append([]int(nil), voters...)
	rf.committedCfg = append([]int(nil), voters...)
	rf.persist()
}

// VoterConfig 返回当前已提交的投票成员集合快照（测试/运维观测用）。
// 注意：返回 committedCfg（已提交配置），而非 rf.cfg（日志最新配置）。两者仅在成员
// 变更「已提议未提交」的短暂窗口内不同；对外观测/计票应反映已生效配置，避免把未提交
// 的未生效成员变更误报为当前集群配置。
func (rf *Raft) VoterConfig() []int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return append([]int(nil), rf.committedCfg...)
}

// HasPendingConf 返回 leader 是否有在途、尚未提交的成员变更。
func (rf *Raft) HasPendingConf() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.pendingConf
}

// ProposeConfChange 由 leader 提议一次成员变更（Ongaro 论文 §6 单服变更）。
// 返回 (条目索引, 是否 leader)。pendingConf 守卫：上一次变更未提交前禁止再提议，保证
// 任意时刻新旧配置只差一个成员、两者多数派必重叠——单服变更安全性的充要条件（不会
// 选出两个 leader、不会出现两个不相交的提交多数派）。变更经旧配置多数派复制并提交后，
// 由 applier 原子切换 rf.cfg；此时新配置才开始参与 quorum 计票。
func (rf *Raft) ProposeConfChange(newVoters []int) (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, false
	}
	if rf.pendingConf {
		return -1, false // 上一次变更仍在途，拒绝堆叠
	}
	// 目标配置合法性校验：拦截会把集群推入不可恢复状态的运维误操作（越界/空/重复
	// 成员）。不在此拦，可能提交一个含不存在成员的配置，使 quorum 永远凑不齐 → 集群
	// 永久卡死（比单纯不生效更糟，因为已写入日志、需手工修复）。
	if err := rf.ValidateConfChange(newVoters); err != nil {
		dbg("me=%d ProposeConfChange rejected: %v", rf.me, err)
		return -1, false
	}
	idx := rf.lastLogIndex() + 1
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: ConfChange{NewVoters: append([]int(nil), newVoters...)}})
	// 注意：leader 这里**不**立即把 rf.cfg 切到 C_new（改用「提交后才生效」语义）。原因：
	// 单服变更里 leader 要在「旧配置」quorum 下提交 C_new 条目本身（Ongaro §6 的安全
	// 变体，I192 单测 TestWitnessDynamicJoinLeave 依赖——移除 witness 后存活数据副本
	// 不足新配置 quorum，必须由仍在旧配置里的 witness 一票把 C_new 提交）。follower 则
	// 在 AppendEntries 追加 C_new 时立即切换 rf.cfg（见下方），以便旧 leader 提交 C_new 后
	// 崩溃、存活 follower 也能以 C_new 凑齐 quorum 选主（E5 死锁根因）。二者分工：leader
	// 提交用旧配置、follower 选举用最新配置。
	dbg("me=%d propose ConfChange idx=%d term=%d newVoters=%v", rf.me, idx, rf.currentTerm, newVoters)
	rf.pendingConf = true
	rf.persist()
	// 立刻唤醒一次复制（非阻塞，缓冲 1 自动合批）。
	select {
	case rf.kickCh <- struct{}{}:
	default:
	}
	return idx, true
}

// ValidateConfChange 校验一次成员变更的目标投票集合是否合法（防运维误操作把集群推入
// 不可恢复状态）。非法情形：
//
//	· 空集合 —— quorum=len/2+1 但无成员，集群永久无法提交任何条目；
//	· 含越界/不存在的 peer 下标 —— 该成员永不在线，quorum 永远凑不齐 → 集群卡死；
//	· 重复下标 —— quorum 计票会重复计数同一成员，等价缩小配置、破坏单服变更安全性。
//
// 返回 nil 表示合法。注意：单服变更允许 leader 把自己移出投票集合（提交后由旧配置
// quorum 通过、再平滑退位），故此处不强制保留当前 leader。
func (rf *Raft) ValidateConfChange(newVoters []int) error {
	if len(newVoters) == 0 {
		return fmt.Errorf("empty voter set: cluster would have no voters and stall")
	}
	n := len(rf.peers)
	seen := make(map[int]bool, len(newVoters))
	for _, v := range newVoters {
		if v < 0 || v >= n {
			return fmt.Errorf("voter %d out of range [0,%d): nonexistent peer would deadlock quorum", v, n-1)
		}
		if seen[v] {
			return fmt.Errorf("duplicate voter %d: quorum would double-count one member", v)
		}
		seen[v] = true
	}
	return nil
}

// ============================== 选举 ==============================

// SetElectionTimeoutFn 注入自定义选举超时生成器（每次重置计时器时调用一次）。
// 必须在节点开始参与选举前设置（Make 返回后立刻调用）；传 nil 恢复默认随机区间。
// 典型用途：跨机部署网络 RTT 较大时注入更大的超时区间，避免频繁误触发选举；
// 测试注入确定性/受控随机的超时以复现特定时序。
func (rf *Raft) SetElectionTimeoutFn(fn func() time.Duration) {
	rf.timerMu.Lock()
	rf.electionTimeoutFn = fn
	rf.timerMu.Unlock()
	rf.resetElectionTimer()
}

func (rf *Raft) resetElectionTimer() {
	rf.timerMu.Lock()
	fn := rf.electionTimeoutFn
	rf.timerMu.Unlock()
	var d time.Duration
	if fn != nil {
		d = fn()
	} else {
		d = ElectionTimeoutMin + time.Duration(rand.Int63n(int64(ElectionTimeoutMax-ElectionTimeoutMin)))
	}
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

	dbg("me=%d startElection preTerm=%d cfg=%v lastIdx=%d lastTerm=%d", me, preTerm, rf.cfg, lastIdx, lastTerm)

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
				dbg("me=%d PreVote from=%d term=%d -> NOT granted", me, i, preTerm)
				return
			}
			dbg("me=%d PreVote from=%d term=%d -> granted (preVotes=%d)", me, i, preTerm, preVotes)
			// 只统计当前投票配置内的成员（I192）：被移除的节点票权不再计入 quorum。
			if !rf.inCfg(i) {
				return
			}
			pmu.Lock()
			preVotes++
			got := preVotes
			pmu.Unlock()
			if got == len(rf.cfg)/2+1 {
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
	dbg("me=%d doRealElection preTerm=%d curTerm=%d cfg=%v", me, preTerm, rf.currentTerm, rf.cfg)
	// 仅当本节点任期仍等于"预投票意向任期 - 1"时方可推进；否则说明期间出现了
	// 更高任期（其他节点已成 leader），放弃本次选举，避免重复/冲突的正式选举。
	if rf.currentTerm != preTerm-1 || rf.preVoteWon {
		rf.mu.Unlock()
		return
	}
	rf.preVoteWon = true
	rf.currentTerm = preTerm
	// 控制面可观测性：任期推进（发起选举）计数，配合 stepDown 的退位计数，
	// 便于观察任期翻转频率（频繁变更往往是网络分区/不稳定信号）。
	Metrics.CounterWithHelp("raft_term_changes_total", "累计任期变更次数(含选举发起与退位)").Inc()
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
				// 只统计当前投票配置内的成员（I192）。
				if !rf.inCfg(i) {
					return
				}
				mu.Lock()
				votes++
				got := votes
				mu.Unlock()
				if got == len(rf.cfg)/2+1 {
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
	// 护栏（I192 收尾）：在途成员变更（pendingConf）期间禁止转移领导权。
	// 否则尚未提交的 ConfChange 条目可能根本没复制到 target，旧 leader 退位后被
	// 新 leader 的 AppendEntries 截断丢弃——一次已"成功"提议的成员变更会静默消失，
	// 集群拓扑因此永远停在旧配置。etcd/rqlite 等实现同样拒绝在 pendingConf 时转移，
	// 让运维改为"等本次变更提交后再转移（pendingConf 清零）"。
	if rf.pendingConf {
		rf.mu.Unlock()
		dbg("me=%d LeadershipTransfer refused: membership change in flight (pendingConf)", rf.me)
		Metrics.CounterWithHelp("raft_leadership_transfer_refused_total",
			"领导权转移被拒绝次数(在途成员变更/非法目标)").Inc()
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
	rf.leaderId = rf.me    // 成为 leader 后自身即 leader，Status 据此回报 me
	rf.pendingConf = false // 新 leader 没有自己提议的在途成员变更（旧 leader 的 pending 与此无关）
	// 新任期必须重新提交一条当前任期 no-op 才能提交旧任期条目；重置该标记，
	// 确保 GetShard 等传输守卫在重新提交 no-op 之前不会传出陈旧快照。
	rf.committedCurrentTerm = false
	rf.preVoteWon = false
	rf.lastContact[rf.me] = time.Now()
	// 本副本视角的累计选举胜出次数：随 Status() 外传，使跨进程部署下
	// Prometheus 也能观测 leader 切换（包级 Metrics 是进程内全局的，
	// 混合了同进程所有 Raft 实例，无法按 group/replica 切片）。
	rf.leaderElections++
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
		// 控制面可观测性：因发现更高任期而退位（任期变更）计数。
		Metrics.CounterWithHelp("raft_term_changes_total", "累计任期变更次数(含选举发起与退位)").Inc()
	}
	rf.role = Follower
	rf.preVoteWon = false
	rf.leaderId = -1 // 退位后失去对 leader 的认知，待下次合法 AppendEntries 重新确认
	rf.resetElectionTimer()
}

// RequestVote 处理投票请求，按任期与日志完整性决定是否授权。
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
	dbg("me=%d Vote from=%d term=%d -> granted=%v votedFor=%d curTerm=%d upToDate=%v",
		rf.me, args.CandidateId, args.Term, grant, rf.votedFor, rf.currentTerm, upToDate)
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
	isWitness := rf.isWitness
	rf.mu.Unlock()
	// Witness 拒绝领导权转移（I189）：它不持有状态机，接管后会把组内数据读成空。
	// 与 ticker 里的「不发起选举」是同一条不变式的两个入口——witness 只投票，不当选。
	if isWitness {
		return
	}
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
// 多数派严格按当前投票配置 cfg 计算：只有 cfg 中的成员计票，quorum = len(cfg)/2+1。
// 这样 witness 加入/移除投票集合后，提交门槛随配置实时变化（动态重配置的核心）。
func (rf *Raft) advanceCommit() {
	if rf.role != Leader {
		return
	}
	quorum := len(rf.cfg)/2 + 1
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		count := 0
		for _, i := range rf.cfg {
			if i == rf.me {
				count++ // 自己默认已复制
				continue
			}
			if rf.matchIndex[i] >= n {
				count++
			}
		}
		if count >= quorum && rf.entryTerm(n) == rf.currentTerm {
			rf.commitIndex = n
			rf.committedCurrentTerm = true // 当前任期条目已提交，旧任期写现可安全服务
			rf.persist()                   // 持久化提交点：崩溃重启后据 commitIndex 重放已提交条目
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
				// 带上快照点的已提交配置：接收方据此重建 cfg/committedCfg，
				// 补上被快照吞掉的 ConfChange 条目（否则其配置永久停在旧值）。
				LastIncludedConfig: append([]int(nil), rf.snapshotCfg...),
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

// AppendEntries 处理日志追加与心跳，维护提交索引与冲突回退。
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
	// 听到 leader，刷新选举计时并认知其编号（供 Status/诊断只读快照使用）。
	rf.resetElectionTimer()
	rf.leaderId = args.LeaderId
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

	// 2) 追加新日志（处理冲突）：以 leader 为准，本地从 pos 起的全部条目作废并覆盖。
	// 即便任期相同也覆盖——follower 与 leader 在同一索引出现不同条目（日志分叉）时，
	// 必须信任 leader，否则成员变更等关键条目无法按 leader 意图收敛（I192 复现的根因：
	// 副本短暂自任 leader 写了分叉日志，follower 因同任期不截断而永久卡在陈旧配置）。
	newIdx := args.PrevLogIndex
	changed := false
	for _, entry := range args.Entries {
		newIdx++
		if newIdx <= rf.lastIncludedIndex {
			continue
		}
		pos := newIdx - rf.lastIncludedIndex - 1
		if pos < len(rf.log) {
			rf.log = rf.log[:pos] // 截断分叉部分（含当前位置），下面用 leader 条目覆盖
			changed = true
		}
		rf.log = append(rf.log, entry)
		changed = true
		if cc, ok := entry.Command.(ConfChange); ok {
			// 单服变更：新配置一入日志即生效（Ongaro §6.4），用于选举/quorum 计票，
			// 不等到提交。否则旧 leader 提交配置后崩溃、本节点仍持旧配置，将无法凑齐
			// quorum 选出新 leader（I192 死锁）。状态机侧仍由 applier 在提交后切换。
			rf.cfg = append([]int(nil), cc.NewVoters...)
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
		dbg("me=%d commit advance: LeaderCommit=%d -> commitIndex=%d lastLog=%d", rf.me, args.LeaderCommit, rf.commitIndex, last)
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
	if pos >= len(rf.log) {
		return // 越界保护：日志短于快照点（commitIndex 与日志不一致的异常情况）
	}
	rf.lastIncludedTerm = rf.log[pos].Term

	// 记账即将被丢弃的日志段里的成员变更（快照吞配置 bug 的修复点）。
	// 这些条目被截断后：AppendEntries 的配置切换再也走不到（条目没了），applier 也会
	// 把 lastApplied 直接跳到 lastIncludedIndex 从而整段跳过它们。若不在此记账，本节点
	// 的 snapshotCfg/committedCfg 将永久停在旧配置，pendingConf 也永远等不到清零。
	// 注意 index <= commitIndex（上面已校验），故这些变更一定**已提交**，把它记入
	// committedCfg 是安全的。
	end := pos + 1
	for _, e := range rf.log[:end] {
		if cc, ok := e.Command.(ConfChange); ok {
			rf.snapshotCfg = append([]int(nil), cc.NewVoters...)
			rf.committedCfg = append([]int(nil), cc.NewVoters...)
			// 该条目已提交且随后会被快照吞掉，applier 永远不会 apply 它——
			// 若不在此时清零，pendingConf 将永久为 true，此后所有成员变更都被拒。
			rf.pendingConf = false
		}
	}

	// 保留 index 之后的日志
	rf.log = append([]LogEntry{}, rf.log[pos+1:]...)
	rf.lastIncludedIndex = index
	rf.snapshot = snapshot
	rf.persister.SaveSnapshot(snapshot)
	rf.persist()
}

// CondInstallSnapshot 由状态机在收到 InstallSnapshot 后调用（导出兼容入口）。
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 导出版不带配置：保持原有行为（配置不变），供状态机直接投递本地快照的场景使用。
	return rf.condInstallSnapshotLocked(lastIncludedTerm, lastIncludedIndex, snapshot, nil)
}

// CondInstallSnapshotWithConfig 是带快照点配置的导出版（InstallSnapshot RPC 走这条）。
// lastIncludedCfg 为快照点处已提交的投票配置，接收方据此重建 cfg/committedCfg。
func (rf *Raft) CondInstallSnapshotWithConfig(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte, lastIncludedCfg []int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.condInstallSnapshotLocked(lastIncludedTerm, lastIncludedIndex, snapshot, lastIncludedCfg)
}

// condInstallSnapshotLocked 是安装快照的主体逻辑，调用方必须已持有 rf.mu。
// 两个历史 bug 的修复点（follower 快照追赶 600s 挂死复盘）：
//  1. 死锁：InstallSnapshot RPC 持锁后曾调用导出版 CondInstallSnapshot 再次 Lock——
//     sync.Mutex 不可重入，任何需要快照追赶的 follower 会把整个 raft 实例锁死，
//     所有后续 RPC（含心跳/投票）全部堆积在 rf.mu 上（dump 中 1346 个 goroutine）。
//  2. 状态机失联：此处曾把 lastApplied 直接顶到 lastIncludedIndex，applier 便永远
//     不会走「idx <= lastIncludedIndex」分支，SnapshotValid 消息永远不发——
//     即便不死锁，follower 的 KV 状态机也拿不到快照数据（隐性数据缺失）。
//     现在只推进 commitIndex 并唤醒 applier，由 applier 发 SnapshotValid 给状态机，
//     lastApplied 在 applier 的快照分支一次跳到位。
func (rf *Raft) condInstallSnapshotLocked(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte, lastIncludedCfg []int) bool {
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
	// 用快照自带的配置重建成员配置（快照吞配置 bug 的修复点）：上面的日志截断已经把
	// <=lastIncludedIndex 的 ConfChange 条目全部丢弃，而 AppendEntries 的配置切换与
	// applier 的提交切换都不会再经过这些条目。不补这一步，本节点的 cfg/committedCfg
	// 会永久停在旧配置：扩容后用旧（更小）配置算 quorum 可能出现两个 leader，
	// 缩容后则要求已移除成员投票、永远选不出主。
	if len(lastIncludedCfg) > 0 {
		rf.snapshotCfg = append([]int(nil), lastIncludedCfg...)
		rf.committedCfg = append([]int(nil), lastIncludedCfg...)
		rf.cfg = append([]int(nil), lastIncludedCfg...)
		// 重放残留日志（快照点之后）里的 ConfChange 推进 cfg：与 AppendEntries 追加
		// 路径保持同一语义——cfg 取「日志最新配置」，committedCfg 取「已提交配置」。
		for _, e := range rf.log {
			if cc, ok := e.Command.(ConfChange); ok {
				rf.cfg = append([]int(nil), cc.NewVoters...)
			}
		}
	}
	// 安装快照后必须统一夹紧 commitIndex / lastApplied 到 [lastIncludedIndex, lastIndex]。
	// 否则若本节点曾是更高任期 leader（commitIndex/lastApplied 偏高），而新日志因分叉被
	// 截断变短，二者会停在「已不存在的日志下标」之上；applier 随后按旧 commitIndex 把
	// lastApplied 推进到越界位置，触发 raft.go:1412 的 index out of range panic。
	// 这是 InstallSnapshot 的隐性记账遗漏：commitIndex/lastApplied 永远不得越过 lastIndex。
	lastIndex := rf.lastIncludedIndex + len(rf.log)
	if rf.commitIndex < lastIncludedIndex {
		rf.commitIndex = lastIncludedIndex
	}
	if rf.commitIndex > lastIndex {
		rf.commitIndex = lastIndex
	}
	if rf.lastApplied > lastIndex {
		rf.lastApplied = lastIndex
	}
	rf.persister.SaveSnapshot(snapshot)
	rf.persist()
	// 唤醒 applier：commitIndex 已前移，applier 将向状态机投递 SnapshotValid 消息。
	rf.applyCond.Signal()
	return true
}

// InstallSnapshot 接收并安装领导者推送的快照。
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
	// Witness 只吃快照元数据、不存状态机数据（I189）：日志压缩点
	// （lastIncludedIndex/Term）必须同步推进，否则后续日志索引会错乱、也无法正确
	// 参与选举时的 up-to-date 比较；但 Data（状态机全量）一分不存，因为它从不 apply。
	data := args.Data
	if rf.isWitness {
		data = nil
	}
	// 注意：Witness 虽然不存状态机数据（data=nil），但仍要吃下快照点配置——它以投票者
	// 身份参与共识，quorum 判断依赖正确的 cfg，配置错了同样会导致脑裂或选不出主。
	rf.condInstallSnapshotLocked(args.LastIncludedTerm, args.LastIncludedIndex, data, args.LastIncludedConfig)
}

// ============================== 后台循环 ==============================

func (rf *Raft) ticker() {
	for {
		select {
		case <-rf.killCh:
			return
		case <-rf.electionTimer.C:
			// Witness 不发起选举（I189）：它不持有状态机数据，一旦当选就会对外
			// 服务一个空状态机（读恒为空、分片恒缺失），等同于数据丢失。它只以
			// 投票者身份参与共识——把票投给日志够新的全量副本。
			rf.mu.Lock()
			shouldElect := rf.role != Leader && !rf.isWitness
			rf.mu.Unlock()
			if shouldElect {
				rf.startElection()
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
		case <-rf.kickCh:
			// 新日志到达：立刻复制一轮，不等心跳 tick。顺带重置心跳计时器
			// （刚广播过就不必紧跟一次空心跳，省一轮无谓 RPC）。
			rf.mu.Lock()
			if rf.role == Leader {
				rf.lastContact[rf.me] = time.Now()
				rf.mu.Unlock()
				rf.broadcastAppendEntries()
				rf.resetHeartbeatTimer()
			} else {
				rf.mu.Unlock()
			}
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
			// 快照内的部分，用快照消息通知状态机；lastApplied 一次跳到快照点，
			// 避免逐条自增重复投递同一份快照（快照消息本身覆盖 <=SnapshotIndex 全部状态）。
			msg = ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.lastApplied = rf.lastIncludedIndex
			Metrics.Counter("snapshots_installed").Inc()
		} else {
			pos := idx - rf.lastIncludedIndex - 1
			// 兜底防护：若 commitIndex/lastApplied 因任何记账路径（快照截断、持久化恢复、
			// 网络分叉后重发等）越过了当前日志末尾 lastIndex，则不向下标越界、不 panic，
			// 而是把 lastApplied 夹回 lastIndex 并（必要时）把 commitIndex 一并夹回，
			// 然后回到循环顶部等待——待日志补齐或 commitIndex 被新的 LeaderCommit/快照
			// 纠正后再继续。这是「commitIndex/lastApplied ≤ lastIndex」不变量的最后一道闸。
			if pos < 0 || pos >= len(rf.log) {
				lastIdx := rf.lastIncludedIndex + len(rf.log)
				if rf.commitIndex > lastIdx {
					rf.commitIndex = lastIdx
				}
				rf.lastApplied = lastIdx
				rf.mu.Unlock()
				continue
			}
			cmd := rf.log[pos].Command
			// 成员变更条目是 raft 内部命令（I192）：不投递给上层状态机，仅原子切换
			// 投票配置 cfg。pendingConf 同步清零，允许 leader 提议下一次变更。
			if cc, ok := cmd.(ConfChange); ok {
				dbg("me=%d apply ConfChange idx=%d NewVoters=%v", rf.me, idx, cc.NewVoters)
				rf.cfg = append([]int(nil), cc.NewVoters...)
				rf.committedCfg = append([]int(nil), cc.NewVoters...) // 已提交配置随 applier 切换
				rf.pendingConf = false
				rf.persist()
				rf.mu.Unlock()
				continue
			}
			msg = ApplyMsg{
				CommandValid: true,
				Command:      cmd,
				CommandIndex: idx,
			}
			Metrics.Counter("log_applied").Inc()
		}
		witness := rf.isWitness
		rf.mu.Unlock()
		if witness {
			// Witness 不持有状态机（I189）：lastApplied 已在上面正常推进（保证
			// 快照/日志压缩的一致记账），但**绝不向上层状态机投递**。上层收不到
			// 任何 apply 消息，状态机恒为空——这正是「省下一份全量数据」的落点，
			// 同时也让上层 Get 的分片归属守卫天然把 witness 挡在服务路径之外。
			continue
		}
		rf.applyCh <- msg
	}
}

// ============================== Make ==============================

// Make 创建一个普通的（全量数据）Raft 副本：参与选举、复制日志、apply 到状态机。
func Make(peers []*ClientEnd, me int, persister Persister, applyCh chan ApplyMsg) *Raft {
	return makeRaft(peers, me, persister, applyCh, false)
}

// MakeWitness 创建一个 Witness（见证者）副本（I189）：持有完整日志并参与投票，
// 但不持有状态机数据、永不成为 leader。
//
// 与 Make 的唯一差异是 isWitness 标志；它决定三条行为分支：ticker 不发起选举、
// applier 不向 applyCh 投递、InstallSnapshot 只吃元数据不吃状态机数据。其余
// （日志复制、投票、持久化、提交推进）与普通副本完全一致——这正是 witness 能
// 在不损失 Raft 安全性的前提下「补票权」的原因：日志仍落盘在多数派上。
//
// 典型用法：2 个全量副本 + 1 个 witness 组成一个 3 节点组（quorum=2）。任一全量副本
// 宕机后，剩 1 全量 + 1 witness 仍达 quorum，集群仍可选主并提交写入（数据仍在存活的
// 那个全量副本上，不丢）；而纯 2 副本组宕一即彻底不可用。
func MakeWitness(peers []*ClientEnd, me int, persister Persister, applyCh chan ApplyMsg) *Raft {
	return makeRaft(peers, me, persister, applyCh, true)
}

// makeRaft 是 Make / MakeWitness 的公共构造体。**isWitness 必须在启动 ticker 与
// applier 两个 goroutine 之前写入**：这两个 goroutine 会读取该标志决定行为分支，
// 若在它们启动后再赋值则构成数据竞争（-race 会报，且行为不确定）。
func makeRaft(peers []*ClientEnd, me int, persister Persister, applyCh chan ApplyMsg, isWitness bool) *Raft {
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
		kickCh:            make(chan struct{}, 1),
		isWitness:         isWitness,
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	// 默认投票配置：全部 peer 均为投票成员。运行中若经 ConfChange 变更，会由
	// readPersist 从持久化状态恢复（持久化早于本默认设定，覆盖之）。
	rf.cfg = make([]int, len(peers))
	for i := range peers {
		rf.cfg[i] = i
	}
	rf.committedCfg = append([]int(nil), rf.cfg...) // 初始已提交配置与 cfg 一致
	// 快照点配置初始化为初始配置：尚未产生任何快照/配置变更时，快照点配置即初始配置，
	// 保证 leader 首次发快照时带的一定是有效非空配置（而非让 follower 退回旧行为）。
	rf.snapshotCfg = append([]int(nil), rf.cfg...)

	rf.readPersist(persister.ReadRaftState())
	if snap := persister.ReadSnapshot(); snap != nil {
		rf.snapshot = snap
	}
	// Witness 不持有状态机，即使持久化里有历史快照数据也丢弃——它只需要日志与
	// 任期/提交点来正确参与投票。保留快照数据只会白占存储，且与「不 apply」的
	// 语义冲突（applier 对 witness 从不投递，这份数据永远不会被消费）。
	if isWitness {
		rf.snapshot = nil
	}

	go rf.ticker()
	go rf.applier()

	return rf
}

// IsWitness 返回本副本是否为 Witness（见证者）副本：持日志与投票权，但不持有
// 状态机数据、永不成为 leader。供上层（ShardKV/运维端点）据此拒绝把读或分片迁移
// 流量导向 witness。
func (rf *Raft) IsWitness() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.isWitness
}

// 便于调试的字符串化
func (rf *Raft) String() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return fmt.Sprintf("id=%d role=%s term=%d logLen=%d commit=%d applied=%d",
		rf.me, rf.role, rf.currentTerm, len(rf.log), rf.commitIndex, rf.lastApplied)
}
