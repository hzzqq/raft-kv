// raft_test.go —— 仿 MIT 6.824 的 config 测试框架 + 一组覆盖 Lab2A~2D 的测试
package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ============================== 测试配置 ==============================

type config struct {
	mu         sync.Mutex
	net        *Network
	rafts      []*Raft
	endnames   [][]*ClientEnd
	applyCh    []chan ApplyMsg
	persisters []*Persister
	logs       [][]interface{} // 各节点已应用的状态机命令（按序）
	connected  []bool
	n          int
	t          *testing.T
}

func makeConfig(t *testing.T, n int) *config {
	net := MakeNetwork()
	cfg := &config{net: net, n: n, t: t}
	cfg.rafts = make([]*Raft, n)
	cfg.endnames = make([][]*ClientEnd, n)
	cfg.applyCh = make([]chan ApplyMsg, n)
	cfg.persisters = make([]*Persister, n)
	cfg.logs = make([][]interface{}, n)
	cfg.connected = make([]bool, n)

	for i := 0; i < n; i++ {
		cfg.connected[i] = true
	}

	for i := 0; i < n; i++ {
		cfg.endnames[i] = make([]*ClientEnd, n)
		for j := 0; j < n; j++ {
			cfg.endnames[i][j] = net.MakeEnd(i*n+j, i)
		}
	}

	for i := 0; i < n; i++ {
		cfg.applyCh[i] = make(chan ApplyMsg, 4000)
		go cfg.monitor(i, cfg.applyCh[i])
		cfg.persisters[i] = MakeEmptyPersister()
		rf := Make(cfg.endnames[i], i, cfg.persisters[i], cfg.applyCh[i])
		cfg.rafts[i] = rf
		cfg.connectAll(i)
		cfg.addServerHandler(i, rf)
	}
	return cfg
}

func (cfg *config) connectAll(i int) {
	for j := 0; j < cfg.n; j++ {
		cfg.net.Connect(i*cfg.n+j, j)
	}
}

func (cfg *config) addServerHandler(i int, rf *Raft) {
	ii := i
	cfg.net.AddServer(i, func(method string, args, reply interface{}) {
		switch method {
		case "RequestVote":
			rf.RequestVote(args.(*RequestVoteArgs), reply.(*RequestVoteReply))
		case "AppendEntries":
			rf.AppendEntries(args.(*AppendEntriesArgs), reply.(*AppendEntriesReply))
		case "InstallSnapshot":
			rf.InstallSnapshot(args.(*InstallSnapshotArgs), reply.(*InstallSnapshotReply))
		default:
			panic("unknown RPC " + method)
		}
		_ = ii
	})
}

// monitor 持续把节点已应用的命令记录下来（节点重启后可能重放，已记录的跳过）。
func (cfg *config) monitor(i int, ch chan ApplyMsg) {
	for m := range ch {
		if !m.CommandValid {
			continue
		}
		cfg.mu.Lock()
		if m.CommandIndex <= len(cfg.logs[i]) {
			cfg.mu.Unlock()
			continue // 已记录（重启重放），忽略
		}
		for len(cfg.logs[i]) < m.CommandIndex-1 {
			cfg.logs[i] = append(cfg.logs[i], nil)
		}
		cfg.logs[i] = append(cfg.logs[i], m.Command)
		cfg.mu.Unlock()
	}
}

func (cfg *config) disconnect(i int) {
	cfg.connected[i] = false
	cfg.net.Enable(i, false)
}

func (cfg *config) connect(i int) {
	cfg.connected[i] = true
	cfg.net.Enable(i, true)
}

func (cfg *config) kill(i int) {
	if cfg.rafts[i] != nil {
		cfg.rafts[i].Kill()
	}
	cfg.connected[i] = false
	cfg.net.Enable(i, false)
}

// restart 用同一份 persister 重建节点（模拟掉电后重启、状态从磁盘恢复）。
func (cfg *config) restart(i int) {
	if cfg.rafts[i] != nil {
		cfg.rafts[i].Kill()
	}
	time.Sleep(60 * time.Millisecond)
	rf := Make(cfg.endnames[i], i, cfg.persisters[i], cfg.applyCh[i])
	cfg.rafts[i] = rf
	cfg.addServerHandler(i, rf)
	cfg.connected[i] = true
	cfg.net.Enable(i, true)
}

func (cfg *config) cleanup() {
	for i := 0; i < cfg.n; i++ {
		if cfg.rafts[i] != nil {
			cfg.rafts[i].Kill()
		}
	}
	cfg.net.Cleanup()
}

func (cfg *config) checkOneLeader() (int, int) {
	for iters := 0; iters < 20; iters++ {
		time.Sleep(150 * time.Millisecond)
		leaders := make(map[int]bool)
		var term int
		for i := 0; i < cfg.n; i++ {
			if cfg.connected[i] {
				t, isL := cfg.rafts[i].GetState()
				if isL {
					leaders[i] = true
					term = t
				}
			}
		}
		if len(leaders) == 1 {
			for id := range leaders {
				return id, term
			}
		}
	}
	return -1, 0
}

func (cfg *config) leader() int {
	for iters := 0; iters < 15; iters++ {
		time.Sleep(100 * time.Millisecond)
		for i := 0; i < cfg.n; i++ {
			if cfg.connected[i] {
				_, isL := cfg.rafts[i].GetState()
				if isL {
					return i
				}
			}
		}
	}
	return -1
}

func (cfg *config) start1(cmd interface{}) int {
	for iters := 0; iters < 40; iters++ {
		l := cfg.leader()
		if l >= 0 {
			if idx, _, ok := cfg.rafts[l].Start(cmd); ok {
				return idx
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	cfg.t.Fatalf("no leader found to start command %v", cmd)
	return -1
}

func (cfg *config) one(cmd interface{}, expectedServers int) int {
	idx := -1
	for iters := 0; iters < 40 && idx < 0; iters++ {
		l := cfg.leader()
		if l >= 0 {
			if i, _, ok := cfg.rafts[l].Start(cmd); ok {
				idx = i
			}
		}
		if idx < 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if idx < 0 {
		cfg.t.Fatalf("could not start command %v", cmd)
	}
	cfg.wait(idx, expectedServers, cmd)
	return idx
}

func (cfg *config) wait(index, n int, cmd interface{}) {
	to := time.Now().Add(12 * time.Second)
	for time.Now().Before(to) {
		count := 0
		for i := 0; i < cfg.n; i++ {
			if !cfg.connected[i] {
				continue
			}
			cfg.mu.Lock()
			sz := len(cfg.logs[i])
			var got interface{}
			if sz >= index {
				got = cfg.logs[i][index-1]
			}
			cfg.mu.Unlock()
			if sz >= index && got == cmd {
				count++
			}
		}
		if count >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	cfg.t.Fatalf("timeout: command %v not seen at index %d on %d servers", cmd, index, n)
}

// ============================== 测试 ==============================

// Lab 2A：初始选举能选出唯一 leader，且任期稳定。
func TestInitialElection(t *testing.T) {
	cfg := makeConfig(t, 3)
	defer cfg.cleanup()

	l, term := cfg.checkOneLeader()
	if l < 0 {
		t.Fatalf("no leader elected")
	}
	time.Sleep(2 * ElectionTimeoutMax)
	l2, term2 := cfg.checkOneLeader()
	if l2 != l {
		t.Fatalf("leader changed %d -> %d", l, l2)
	}
	if term2 < term {
		t.Fatalf("term decreased %d -> %d", term, term2)
	}
}

// Lab 2A：leader 掉线后能重新选举，重连后旧 leader 退位。
func TestReElection(t *testing.T) {
	cfg := makeConfig(t, 3)
	defer cfg.cleanup()

	l, _ := cfg.checkOneLeader()
	cfg.disconnect(l)
	l2, _ := cfg.checkOneLeader()
	if l2 < 0 {
		t.Fatalf("no new leader after disconnect")
	}
	if l2 == l {
		t.Fatalf("old leader %d still leader", l)
	}
	cfg.connect(l)
	l3, _ := cfg.checkOneLeader()
	if l3 < 0 {
		t.Fatalf("no leader after reconnect")
	}
}

// Lab 2B：基本日志复制，3 节点都提交同一条命令。
func TestBasicAgree(t *testing.T) {
	cfg := makeConfig(t, 3)
	defer cfg.cleanup()
	cfg.one(101, 3)
	cfg.one(102, 3)
	cfg.one(103, 3)
}

// Lab 2B：一个 follower 掉线，2/3 仍能提交。
func TestFailAgree(t *testing.T) {
	cfg := makeConfig(t, 3)
	defer cfg.cleanup()
	cfg.one(101, 3)
	l := cfg.leader()
	f := (l + 1) % cfg.n
	cfg.disconnect(f)
	cfg.one(102, 2)
	cfg.one(103, 2)
	cfg.connect(f)
	cfg.one(104, 3)
}

// Lab 2B：只剩 2/5 节点时无法形成多数，命令不应提交；恢复后集群能重新达成共识。
func TestFailNoAgree(t *testing.T) {
	cfg := makeConfig(t, 5)
	defer cfg.cleanup()
	cfg.one(101, 5)

	l := cfg.leader()
	disconnected := 0
	for i := 0; i < cfg.n && disconnected < 3; i++ {
		if i != l {
			cfg.disconnect(i)
			disconnected++
		}
	}
	idx := cfg.start1(102)
	// 仅 2 个节点在线，达不到多数(3)，leader 不应提交
	time.Sleep(2 * ElectionTimeoutMax)
	if cfg.rafts[l].LastApplied() >= idx {
		t.Fatalf("command %d committed with only 2/5 servers (no majority)", idx)
	}
	// 恢复全部节点。注意：分区期间未形成多数的 102 只是"未提交"条目，
	// Raft 只保证"已提交"条目在 leader 切换后不丢，所以 102 可能丢失是合法的。
	// 正确做法是验证集群重新具备共识能力——提交一条新命令并扩散到全部 5 节点。
	for i := 0; i < cfg.n; i++ {
		if i != l {
			cfg.connect(i)
		}
	}
	cfg.one(103, 5)
}

// Lab 2B：并发提交多条命令，顺序与内容一致。
func TestConcurrentStarts(t *testing.T) {
	cfg := makeConfig(t, 3)
	defer cfg.cleanup()
	cmds := []interface{}{100, 101, 102, 103, 104}
	idxs := make([]int, len(cmds))
	for i := range cmds {
		idxs[i] = cfg.start1(cmds[i])
	}
	for i := range cmds {
		cfg.wait(idxs[i], 3, cmds[i])
	}
}

// Lab 2C：持久化——全部重启后已提交日志不丢，并能继续提交。
func TestPersist1(t *testing.T) {
	cfg := makeConfig(t, 3)
	defer cfg.cleanup()
	cfg.one(101, 3)
	l := cfg.leader()
	cfg.disconnect(l)
	idx102 := cfg.one(102, 2) // 记录 102 的实际索引（no-op 会占用索引，不能写死）
	// 全部掉电重启（同一份 persister 恢复）
	for i := 0; i < cfg.n; i++ {
		cfg.restart(i)
	}
	// 重启后发一条新任期命令，会"隐式重提交"旧日志（含 102）
	cfg.one(103, 3)
	// 此时 102 已被重放并应用到全部 3 节点
	cfg.wait(idx102, 3, 102)
}

// Lab 2D：日志压缩——定期快照后，落后节点能通过 InstallSnapshot 追平。
func TestSnapshot(t *testing.T) {
	cfg := makeConfig(t, 3)
	defer cfg.cleanup()

	total := 20
	for i := 0; i < total; i++ {
		cfg.one(200+i, 3)
		if (i+1)%5 == 0 {
			for j := 0; j < cfg.n; j++ {
				if cfg.connected[j] {
					cfg.rafts[j].Snapshot(i+1, []byte(fmt.Sprintf("snap-%d", i+1)))
				}
			}
		}
	}

	l := cfg.leader()
	f := (l + 1) % cfg.n
	cfg.disconnect(f)
	var idxLast int
	for i := 0; i < 10; i++ {
		idxLast = cfg.one(300+i, 2)
	}
	cfg.connect(f)
	// f 落后且已有快照，重连后应通过快照+增量追平到最终状态
	cfg.wait(idxLast, 3, 300+9)
}
