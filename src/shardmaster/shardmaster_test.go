// shardmaster_test.go —— 配置服务测试 + 测试用网络框架
package shardmaster

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"raftkv/src/raft"
)

type smConfig struct {
	net      *raft.Network
	sm       []*ShardMaster
	names    []string
	make_end func(string) *raft.ClientEnd
	n        int
	t        *testing.T
}

func makeSMConfig(t *testing.T, n int) *smConfig {
	net := raft.MakeNetwork()
	cfg := &smConfig{net: net, n: n, t: t}
	ids := map[string]int{}
	for j := 0; j < n; j++ {
		name := fmt.Sprintf("m%d", j)
		cfg.names = append(cfg.names, name)
		ids[name] = j
	}

	peers := make([][]*raft.ClientEnd, n)
	for j := 0; j < n; j++ {
		peers[j] = make([]*raft.ClientEnd, n)
		for k := 0; k < n; k++ {
			e := net.MakeEnd(j*n+k, j)
			net.Connect(j*n+k, k)
			peers[j][k] = e
		}
	}

	for j := 0; j < n; j++ {
		p := raft.MakeEmptyPersister()
		sm := Make(peers[j], j, p)
		cfg.sm = append(cfg.sm, sm)
		jj := j
		net.AddServer(j, func(method string, args, reply interface{}) {
			switch method {
			case "RequestVote", "RequestPreVote", "AppendEntries", "InstallSnapshot", "TimeoutNow":
				sm.RaftRPC(method, args, reply)
			case "ShardMaster.Join":
				sm.Join(args.(*JoinArgs), reply.(*JoinReply))
			case "ShardMaster.Leave":
				sm.Leave(args.(*LeaveArgs), reply.(*LeaveReply))
			case "ShardMaster.Move":
				sm.Move(args.(*MoveArgs), reply.(*MoveReply))
			case "ShardMaster.Query":
				sm.Query(args.(*QueryArgs), reply.(*QueryReply))
			default:
				t.Fatalf("sm%d unexpected method %s", jj, method)
			}
		})
	}

	var mu sync.Mutex
	cache := map[string]*raft.ClientEnd{}
	cfg.make_end = func(name string) *raft.ClientEnd {
		mu.Lock()
		defer mu.Unlock()
		if e, ok := cache[name]; ok {
			return e
		}
		id := ids[name]
		e := net.MakeEnd(9000+id, 9000) // owner 9000 未注册 -> 视为可达
		net.Connect(9000+id, id)
		cache[name] = e
		return e
	}
	return cfg
}

func (cfg *smConfig) makeClerk() *Clerk {
	return MakeClerk(append([]string{}, cfg.names...), cfg.make_end)
}

func (cfg *smConfig) leader() int {
	for iters := 0; iters < 20; iters++ {
		time.Sleep(100 * time.Millisecond)
		for i := 0; i < cfg.n; i++ {
			if _, isL := cfg.sm[i].rf.GetState(); isL {
				return i
			}
		}
	}
	return -1
}

func (cfg *smConfig) disconnect(i int) {
	cfg.net.Enable(i, false)
}
func (cfg *smConfig) connect(i int) {
	cfg.net.Enable(i, true)
}
func (cfg *smConfig) cleanup() {
	for i := 0; i < cfg.n; i++ {
		cfg.sm[i].Kill()
	}
	cfg.net.Cleanup()
}

// ---------- 测试 ----------

// 单一 group 加入后，所有分片都应归属它。
func TestSMJoinSingle(t *testing.T) {
	cfg := makeSMConfig(t, 3)
	defer cfg.cleanup()

	ck := cfg.makeClerk()
	ck.Join(map[int][]string{1: {"g1s1", "g1s2"}})

	c := ck.Query(-1)
	if c.Num != 1 {
		t.Fatalf("config num = %d, want 1", c.Num)
	}
	if len(c.Groups) != 1 || len(c.Groups[1]) != 2 {
		t.Fatalf("groups = %v, want {1:[g1s1,g1s2]}", c.Groups)
	}
	for i := 0; i < NShards; i++ {
		if c.Shards[i] != 1 {
			t.Fatalf("shard %d owner = %d, want 1", i, c.Shards[i])
		}
	}
}

// 两个 group 加入后，分片应全部分配给这两个 gid 之一，且都出现。
func TestSMJoinTwo(t *testing.T) {
	cfg := makeSMConfig(t, 3)
	defer cfg.cleanup()

	ck := cfg.makeClerk()
	ck.Join(map[int][]string{1: {"g1s1"}, 2: {"g2s1"}})

	c := ck.Query(-1)
	if len(c.Groups) != 2 {
		t.Fatalf("groups = %v, want 2 groups", c.Groups)
	}
	seen := map[int]bool{}
	for i := 0; i < NShards; i++ {
		g := c.Shards[i]
		if g != 1 && g != 2 {
			t.Fatalf("shard %d owner = %d, want 1 or 2", i, g)
		}
		seen[g] = true
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("both gids must appear in shard assignment: %v", c.Shards)
	}
}

// Leave 后配置应推进，且分片被重新分配。
func TestSMLeave(t *testing.T) {
	cfg := makeSMConfig(t, 3)
	defer cfg.cleanup()

	ck := cfg.makeClerk()
	ck.Join(map[int][]string{1: {"g1s1"}, 2: {"g2s1"}})
	before := ck.Query(-1)
	if before.Num != 1 {
		t.Fatalf("after join num = %d, want 1", before.Num)
	}
	ck.Leave([]int{2})
	after := ck.Query(-1)
	if after.Num != 2 {
		t.Fatalf("after leave num = %d, want 2", after.Num)
	}
	if _, ok := after.Groups[2]; ok {
		t.Fatalf("gid 2 should be removed from groups: %v", after.Groups)
	}
	for i := 0; i < NShards; i++ {
		if after.Shards[i] != 1 {
			t.Fatalf("shard %d owner = %d, want 1 (only gid 1 left)", i, after.Shards[i])
		}
	}
}

// Query 历史：指定版本号应返回对应配置。
func TestSMQueryHistory(t *testing.T) {
	cfg := makeSMConfig(t, 3)
	defer cfg.cleanup()

	ck := cfg.makeClerk()
	ck.Join(map[int][]string{1: {"g1s1"}})
	ck.Join(map[int][]string{2: {"g2s1"}})

	c0 := ck.Query(0)
	if c0.Num != 0 {
		t.Fatalf("config 0 num = %d, want 0", c0.Num)
	}
	c1 := ck.Query(1)
	if c1.Num != 1 || len(c1.Groups) != 1 {
		t.Fatalf("config 1 = %+v, want num 1 with 1 group", c1)
	}
	c2 := ck.Query(2)
	if c2.Num != 2 || len(c2.Groups) != 2 {
		t.Fatalf("config 2 = %+v, want num 2 with 2 groups", c2)
	}
}

// 容错：杀掉 leader 后 Join 仍应成功。
func TestSMFaultTolerant(t *testing.T) {
	cfg := makeSMConfig(t, 3)
	defer cfg.cleanup()

	ck := cfg.makeClerk()
	ck.Join(map[int][]string{1: {"g1s1"}})

	l := cfg.leader()
	if l < 0 {
		t.Fatalf("no leader found")
	}
	cfg.sm[l].Kill()
	cfg.disconnect(l)

	ck.Join(map[int][]string{2: {"g2s1"}})
	c := ck.Query(-1)
	if len(c.Groups) != 2 {
		t.Fatalf("after killing leader, groups = %v, want 2", c.Groups)
	}
}

// I6：非法 Join/Leave/Move 必须返回 ErrInvalid，且不改变状态。
func TestShardMasterValidation(t *testing.T) {
	cfg := makeSMConfig(t, 3)
	defer cfg.cleanup()
	ck := cfg.makeClerk()

	ck.Join(map[int][]string{1: {"g1s1"}})
	if cfg.leader() < 0 {
		t.Fatalf("no leader found")
	}
	sm := cfg.sm[cfg.leader()]

	// --- Join 非法输入 ---
	// gid <= 0
	{
		args := &JoinArgs{Servers: map[int][]string{0: {"x"}}, CkId: 1, Seq: 1}
		reply := &JoinReply{}
		sm.Join(args, reply)
		if reply.Err != ErrInvalid {
			t.Fatalf("Join gid<=0: got %v, want ErrInvalid", reply.Err)
		}
	}
	// servers 条目为空
	{
		args := &JoinArgs{Servers: map[int][]string{2: {}}, CkId: 1, Seq: 2}
		reply := &JoinReply{}
		sm.Join(args, reply)
		if reply.Err != ErrInvalid {
			t.Fatalf("Join empty servers: got %v, want ErrInvalid", reply.Err)
		}
	}
	// 重复加入已存在的 gid
	{
		args := &JoinArgs{Servers: map[int][]string{1: {"g1s1"}}, CkId: 1, Seq: 3}
		reply := &JoinReply{}
		sm.Join(args, reply)
		if reply.Err != ErrInvalid {
			t.Fatalf("Join dup gid: got %v, want ErrInvalid", reply.Err)
		}
	}

	// 合法的 Join
	ck.Join(map[int][]string{2: {"g2s1"}})
	if len(ck.Query(-1).Groups) != 2 {
		t.Fatalf("valid Join should add gid 2")
	}

	// --- Leave 非法输入 ---
	// 不存在的 gid
	{
		args := &LeaveArgs{Gids: []int{99}, CkId: 1, Seq: 4}
		reply := &LeaveReply{}
		sm.Leave(args, reply)
		if reply.Err != ErrInvalid {
			t.Fatalf("Leave missing gid: got %v, want ErrInvalid", reply.Err)
		}
	}
	// 空列表
	{
		args := &LeaveArgs{Gids: []int{}, CkId: 1, Seq: 5}
		reply := &LeaveReply{}
		sm.Leave(args, reply)
		if reply.Err != ErrInvalid {
			t.Fatalf("Leave empty: got %v, want ErrInvalid", reply.Err)
		}
	}

	// --- Move 非法输入 ---
	// 分片越界
	{
		args := &MoveArgs{Shard: NShards, Gid: 1, CkId: 1, Seq: 6}
		reply := &MoveReply{}
		sm.Move(args, reply)
		if reply.Err != ErrInvalid {
			t.Fatalf("Move bad shard: got %v, want ErrInvalid", reply.Err)
		}
	}
	// 目标 gid 不存在
	{
		args := &MoveArgs{Shard: 0, Gid: 99, CkId: 1, Seq: 7}
		reply := &MoveReply{}
		sm.Move(args, reply)
		if reply.Err != ErrInvalid {
			t.Fatalf("Move bad gid: got %v, want ErrInvalid", reply.Err)
		}
	}

	// 合法 Move 仍按原样生效
	ck.Move(0, 2)
	if c := ck.Query(-1); c.Shards[0] != 2 {
		t.Fatalf("Move should set shard0->2, got %d", c.Shards[0])
	}
}

// I7：Join 后只移动必要的最小分片数，而非全量重排。
func TestRebalanceMinimalMoves(t *testing.T) {
	cfg := makeSMConfig(t, 3)
	defer cfg.cleanup()
	ck := cfg.makeClerk()

	ck.Join(map[int][]string{1: {"g1s1"}})
	before := ck.Query(-1) // 全部 10 个分片属于 gid1

	ck.Join(map[int][]string{2: {"g2s1"}})
	after := ck.Query(-1)

	changed := 0
	for i := 0; i < NShards; i++ {
		if before.Shards[i] != after.Shards[i] {
			changed++
		}
	}
	// 新加入的 group 只应承担约一半分片；不应发生全量重排（10 个都变）。
	if changed > NShards/2 {
		t.Fatalf("Join changed %d shards, want <= %d (minimal-move)", changed, NShards/2)
	}
}

// I7：多次 Join/Leave 后，各 group 负载差不超过 ±1。
func TestRebalanceKeepsBalance(t *testing.T) {
	cfg := makeSMConfig(t, 3)
	defer cfg.cleanup()
	ck := cfg.makeClerk()

	ck.Join(map[int][]string{1: {"g1s1"}})
	ck.Join(map[int][]string{2: {"g2s1"}})
	ck.Join(map[int][]string{3: {"g3s1"}})
	ck.Join(map[int][]string{4: {"g4s1"}})
	ck.Leave([]int{2})
	ck.Join(map[int][]string{5: {"g5s1"}})

	c := ck.Query(-1)
	load := map[int]int{}
	for i := 0; i < NShards; i++ {
		load[c.Shards[i]]++
	}
	max, min := 0, NShards+1
	for _, l := range load {
		if l > max {
			max = l
		}
		if l < min {
			min = l
		}
	}
	if max-min > 1 {
		t.Fatalf("imbalance max-min=%d (>1), loads=%v", max-min, load)
	}
}
