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
			case "RequestVote", "AppendEntries", "InstallSnapshot":
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
