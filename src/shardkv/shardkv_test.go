// shardkv_test.go —— Lab 4 分片 KV 测试 + 多 group 网络框架
package shardkv

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"raftkv/src/raft"
	"raftkv/src/shardmaster"
)

type skvConfig struct {
	mu        sync.Mutex
	net       *raft.Network
	sm        []*shardmaster.ShardMaster
	smNames   []string
	nameToID  map[string]int
	cache     map[string]*raft.ClientEnd
	make_end  func(string) *raft.ClientEnd
	groups    [][]*ShardKV
	groupNames [][]string
	nGroups   int
	nReplicas int
	nSM       int
	maxraftstate int
	t         *testing.T
}

func makeSKVConfig(t *testing.T, nGroups, nReplicas, nSM, maxraftstate int) *skvConfig {
	net := raft.MakeNetwork()
	cfg := &skvConfig{
		net:          net,
		nameToID:     map[string]int{},
		cache:        map[string]*raft.ClientEnd{},
		groups:       make([][]*ShardKV, nGroups),
		groupNames:   make([][]string, nGroups),
		nGroups:      nGroups,
		nReplicas:    nReplicas,
		nSM:          nSM,
		maxraftstate: maxraftstate,
		t:            t,
	}

	make_end := func(name string) *raft.ClientEnd {
		cfg.mu.Lock()
		defer cfg.mu.Unlock()
		if e, ok := cfg.cache[name]; ok {
			return e
		}
		id := cfg.nameToID[name]
		e := net.MakeEnd(8000+id, 9000) // owner 9000 未注册 -> 视为可达
		net.Connect(8000+id, id)
		cfg.cache[name] = e
		return e
	}
	cfg.make_end = make_end

	// ---- shardmaster 集群 ----
	for j := 0; j < nSM; j++ {
		name := fmt.Sprintf("m%d", j)
		cfg.smNames = append(cfg.smNames, name)
		cfg.nameToID[name] = j
	}
	for j := 0; j < nSM; j++ {
		peers := make([]*raft.ClientEnd, nSM)
		for k := 0; k < nSM; k++ {
			e := net.MakeEnd(j*nSM+k, j)
			net.Connect(j*nSM+k, k)
			peers[k] = e
		}
		p := raft.MakeEmptyPersister()
		sm := shardmaster.Make(peers, j, p)
		cfg.sm = append(cfg.sm, sm)
		jj := j
		net.AddServer(j, func(method string, args, reply interface{}) {
			switch method {
			case "RequestVote", "AppendEntries", "InstallSnapshot":
				sm.RaftRPC(method, args, reply)
			case "ShardMaster.Join":
				sm.Join(args.(*shardmaster.JoinArgs), reply.(*shardmaster.JoinReply))
			case "ShardMaster.Leave":
				sm.Leave(args.(*shardmaster.LeaveArgs), reply.(*shardmaster.LeaveReply))
			case "ShardMaster.Move":
				sm.Move(args.(*shardmaster.MoveArgs), reply.(*shardmaster.MoveReply))
			case "ShardMaster.Query":
				sm.Query(args.(*shardmaster.QueryArgs), reply.(*shardmaster.QueryReply))
			default:
				t.Fatalf("sm%d unexpected method %s", jj, method)
			}
		})
	}

	// ---- shardkv 各 group ----
	for g := 0; g < nGroups; g++ {
		for r := 0; r < nReplicas; r++ {
			name := fmt.Sprintf("g%d-%d", g, r)
			id := 1000 + g*100 + r
			cfg.nameToID[name] = id
			cfg.groupNames[g] = append(cfg.groupNames[g], name)

			peers := make([]*raft.ClientEnd, nReplicas)
			for r2 := 0; r2 < nReplicas; r2++ {
				e := net.MakeEnd(id*nReplicas+r2, id)
				net.Connect(id*nReplicas+r2, 1000+g*100+r2)
				peers[r2] = e
			}
			applyCh := make(chan raft.ApplyMsg, 4000)
			p := raft.MakeEmptyPersister()
			rf := raft.Make(peers, r, p, applyCh)
			kv := MakeShardKV(g+1, cfg.smNames, make_end, rf, applyCh, maxraftstate)
			cfg.groups[g] = append(cfg.groups[g], kv)

			net.AddServer(id, func(method string, args, reply interface{}) {
				switch method {
				case "RequestVote":
					rf.RequestVote(args.(*raft.RequestVoteArgs), reply.(*raft.RequestVoteReply))
				case "AppendEntries":
					rf.AppendEntries(args.(*raft.AppendEntriesArgs), reply.(*raft.AppendEntriesReply))
				case "InstallSnapshot":
					rf.InstallSnapshot(args.(*raft.InstallSnapshotArgs), reply.(*raft.InstallSnapshotReply))
				case "ShardKV.Get":
					kv.Get(args.(*GetArgs), reply.(*GetReply))
				case "ShardKV.PutAppend":
					kv.PutAppend(args.(*PutAppendArgs), reply.(*PutAppendReply))
				case "ShardKV.SendShard":
					kv.SendShard(args.(*SendShardArgs), reply.(*SendShardReply))
				case "ShardKV.GetShard":
					kv.GetShard(args.(*GetShardArgs), reply.(*GetShardReply))
				default:
					t.Fatalf("g%d-%d unexpected method %s", g, r, method)
				}
			})
		}
	}
	return cfg
}

func (cfg *skvConfig) makeClerk() *Clerk {
	return MakeClerk(cfg.smNames, cfg.make_end)
}

// 注意：harness 中 group 实际 gid = 组下标+1（避免 gid 0 与"未分配"哨兵冲突），
// 因此这些 helper 接收组下标，内部统一 +1 作为 shardmaster 的 gid。
func (cfg *skvConfig) joinGroup(g int) {
	ck := shardmaster.MakeClerk(cfg.smNames, cfg.make_end)
	ck.Join(map[int][]string{g + 1: append([]string{}, cfg.groupNames[g]...)})
}
func (cfg *skvConfig) leaveGroup(g int) {
	ck := shardmaster.MakeClerk(cfg.smNames, cfg.make_end)
	ck.Leave([]int{g + 1})
}
func (cfg *skvConfig) moveShard(shard, g int) {
	ck := shardmaster.MakeClerk(cfg.smNames, cfg.make_end)
	ck.Move(shard, g+1)
}

// groupConfigNum 返回某 group 内某副本当前生效的配置版本号。
func (cfg *skvConfig) groupConfigNum(g, r int) int {
	return cfg.groups[g][r].config.Num
}

func (cfg *skvConfig) waitGroupConfig(g, r, num int) {
	for i := 0; i < 100; i++ {
		if cfg.groupConfigNum(g, r) >= num {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (cfg *skvConfig) disconnectGroup(g int) {
	for r := 0; r < cfg.nReplicas; r++ {
		cfg.net.Enable(1000+g*100+r, false)
	}
}
func (cfg *skvConfig) connectGroup(g int) {
	for r := 0; r < cfg.nReplicas; r++ {
		cfg.net.Enable(1000+g*100+r, true)
	}
}

func (cfg *skvConfig) cleanup() {
	for g := 0; g < cfg.nGroups; g++ {
		for r := 0; r < cfg.nReplicas; r++ {
			if cfg.groups[g][r] != nil {
				cfg.groups[g][r].Kill()
			}
		}
	}
	for j := 0; j < cfg.nSM; j++ {
		if cfg.sm[j] != nil {
			cfg.sm[j].Kill()
		}
	}
	cfg.net.Cleanup()
}

// ============================== 测试 ==============================

// 单 group 基本读写。
func TestSKVBasic(t *testing.T) {
	cfg := makeSKVConfig(t, 1, 3, 3, 0)
	defer cfg.cleanup()

	ck := cfg.makeClerk()
	cfg.joinGroup(0)
	cfg.waitGroupConfig(0, 0, 1)

	ck.Put("k1", "v1")
	if v := ck.Get("k1"); v != "v1" {
		t.Fatalf("after Put got %q want v1", v)
	}
	ck.Append("k1", "x")
	if v := ck.Get("k1"); v != "v1x" {
		t.Fatalf("after Append got %q want v1x", v)
	}
	ck.Put("k1", "v2")
	if v := ck.Get("k1"); v != "v2" {
		t.Fatalf("after overwrite got %q want v2", v)
	}
}

// Move：把某个分片从 group1 迁移到 group2，数据应随之迁移且可读。
func TestSKVMove(t *testing.T) {
	cfg := makeSKVConfig(t, 2, 3, 3, 0)
	defer cfg.cleanup()

	ck := cfg.makeClerk()
	cfg.joinGroup(0)
	cfg.waitGroupConfig(0, 0, 1)

	shard := key2shard("alpha")
	ck.Put("alpha", "from-g1")
	cfg.joinGroup(1)
	cfg.waitGroupConfig(1, 0, 2)
	cfg.moveShard(shard, 1) // 把该分片迁到 group1
	// 等两个 group 都应用 Move 后的配置
	cfg.waitGroupConfig(0, 0, 3)
	cfg.waitGroupConfig(1, 0, 3)
	time.Sleep(300 * time.Millisecond) // 等迁移完成

	if v := ck.Get("alpha"); v != "from-g1" {
		t.Fatalf("after Move shard %d: got %q want \"from-g1\"", shard, v)
	}
}

// Join/Leave：写入两个 group，移除 group1 后数据仍在 group2 上可读。
func TestSKVJoinLeave(t *testing.T) {
	cfg := makeSKVConfig(t, 2, 3, 3, 0)
	defer cfg.cleanup()

	ck := cfg.makeClerk()
	cfg.joinGroup(0)
	cfg.waitGroupConfig(0, 0, 1)
	cfg.joinGroup(1)
	cfg.waitGroupConfig(1, 0, 2)

	ck.Put("a", "va")
	ck.Put("b", "vb")
	ck.Append("a", "!")
	cfg.leaveGroup(0)
	// 等 group2 应用 Leave 后的配置（分片从 g1 迁到 g2）
	cfg.waitGroupConfig(1, 0, 3)
	time.Sleep(500 * time.Millisecond)

	// watchdog：若读取在数秒内未完成，打印各 group 状态辅助定位卡死。
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			for g := 0; g < cfg.nGroups; g++ {
				for r := 0; r < cfg.nReplicas; r++ {
					t.Logf("[watchdog] g%d-%d: %s", g, r, cfg.groups[g][r].DebugState())
				}
			}
			smReply := &shardmaster.QueryReply{}
			cfg.sm[0].Query(&shardmaster.QueryArgs{Num: -1}, smReply)
			t.Logf("[watchdog] shardmaster latest config: %+v", smReply.Config)
		}
	}()

	if v := ck.Get("a"); v != "va!" {
		t.Fatalf("after Leave g1: Get(a)=%q want \"va!\"", v)
	}
	if v := ck.Get("b"); v != "vb" {
		t.Fatalf("after Leave g1: Get(b)=%q want \"vb\"", v)
	}
	close(done)
}

// TestSKVConcurrent：多个客户端并发读写各自独占的 key，同时后台不断做配置变更
// （Move 分片在两组间漂移）。任何客户都应能读到自己刚写入的值（线性一致），
// 即便读写过程中分片发生迁移——验证迁移 + 并发 + 客户端幂等的正确性。
func TestSKVConcurrent(t *testing.T) {
	cfg := makeSKVConfig(t, 2, 3, 3, 0)
	defer cfg.cleanup()
	ck := cfg.makeClerk()
	cfg.joinGroup(0)
	cfg.joinGroup(1)
	cfg.waitGroupConfig(1, 0, 2)

	const nClerks = 3
	const nKeys = 5
	const rounds = 20

	// 后台 churn：周期性把某个分片在两组间移动。
	done := make(chan struct{})
	go func() {
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
				cfg.moveShard((i*3)%NShards, i%2)
				time.Sleep(40 * time.Millisecond)
			}
		}
	}()

	var wg sync.WaitGroup
	errCh := make(chan string, nClerks*nKeys)
	for c := 0; c < nClerks; c++ {
		for k := 0; k < nKeys; k++ {
			wg.Add(1)
			go func(c, k int) {
				defer wg.Done()
				// 每个 goroutine 使用独立的 Clerk：独立 clientId + 独立 seq 计数器。
				// 共享单个 Clerk 会导致所有 key 复用同一 clientId，而 Clerk 在调用时
				// （而非提交时）分配 seq，Raft 又按网络到达顺序提交——于是先拿到较小
				// seq 的 Put 可能因后到的较大 seq 已提交而被去重逻辑当作陈旧重放丢弃，
				// 该 key 永不被写入，紧接的 Get 读到空值而误报。独立的 Clerk 使每个
				// key 的 clientId 内 seq 严格随"调用顺序==提交顺序"单调递增，去重不再跨
				// key 串扰，紧接读回的线性一致断言才成立（也契合"多个客户端"的测试本意）。
				localCk := cfg.makeClerk()
				key := fmt.Sprintf("c%d_k%d", c, k)
				for seq := 0; seq < rounds; seq++ {
					val := fmt.Sprintf("c%d_k%d_%d", c, k, seq)
					localCk.Put(key, val)
					if got := localCk.Get(key); got != val {
						errCh <- fmt.Sprintf("key %s: want %s got %s", key, val, got)
						return
					}
				}
			}(c, k)
		}
	}
	wg.Wait()
	close(done)
	close(errCh)
	for err := range errCh {
		t.Fatalf(err)
	}

	// 最终校验：每个 key 应读到最后一轮写入的值。
	for c := 0; c < nClerks; c++ {
		for k := 0; k < nKeys; k++ {
			key := fmt.Sprintf("c%d_k%d", c, k)
			want := fmt.Sprintf("c%d_k%d_%d", c, k, rounds-1)
			if got := ck.Get(key); got != want {
				t.Fatalf("final key %s: want %s got %s", key, want, got)
			}
		}
	}
}

// TestSKVGC：分片从 g0 迁到 g1 后，旧 owner(g0) 应当回收该分片
// （GetShard 返回 ErrWrongGroup），新 owner(g1) 持有它（返回 OK）。
func TestSKVGC(t *testing.T) {
	cfg := makeSKVConfig(t, 2, 3, 3, 0)
	defer cfg.cleanup()
	ck := cfg.makeClerk()
	cfg.joinGroup(0)
	cfg.joinGroup(1)
	cfg.waitGroupConfig(1, 0, 2)

	shard := key2shard("gckey")
	ck.Put("gckey", "v")
	cfg.moveShard(shard, 1) // 迁到 g1
	cfg.waitGroupConfig(0, 0, 3)
	cfg.waitGroupConfig(1, 0, 3)
	time.Sleep(1 * time.Second)

	// 旧 owner 是 g0：遍历 g0 所有副本，数据应已被 GC（没有任何副本返回 OK，
	// leader 返回 ErrWrongGroup、follower 返回 ErrWrongLeader，都表示"不再持有该分片"）。
	time.Sleep(2 * time.Second) // 给 push+GC 路径更充分的时间
	oldHasShard := false
	for r := 0; r < cfg.nReplicas; r++ {
		oldEnd := cfg.make_end(cfg.groupNames[0][r])
		oldReply := &GetShardReply{}
		if oldEnd.Call("ShardKV.GetShard", &GetShardArgs{Shard: shard}, oldReply) && oldReply.Err == OK {
			oldHasShard = true
			break
		}
	}
	if oldHasShard {
		// 诊断：打印双方所有副本状态与最新配置
		for g := 0; g < cfg.nGroups; g++ {
			for r := 0; r < cfg.nReplicas; r++ {
				t.Logf("[GC-diag] g%d-%d: %s", g, r, cfg.groups[g][r].DebugState())
			}
		}
		smReply := &shardmaster.QueryReply{}
		cfg.sm[0].Query(&shardmaster.QueryArgs{Num: -1}, smReply)
		t.Logf("[GC-diag] shardmaster latest: %+v", smReply.Config)
		t.Fatalf("old owner g0 still serves shard %d after GC (want GC'd)", shard)
	}
	// 新 owner 是 g1：遍历 g1 所有副本，至少有一份返回 OK 即表示它持有该分片
	//（GetShard 仅 leader 响应，follower 返回 ErrWrongLeader，故需检查全部副本）。
	newHasShard := false
	for r := 0; r < cfg.nReplicas; r++ {
		newEnd := cfg.make_end(cfg.groupNames[1][r])
		newReply := &GetShardReply{}
		if newEnd.Call("ShardKV.GetShard", &GetShardArgs{Shard: shard}, newReply) && newReply.Err == OK {
			newHasShard = true
			break
		}
	}
	if !newHasShard {
		for g := 0; g < cfg.nGroups; g++ {
			for r := 0; r < cfg.nReplicas; r++ {
				t.Logf("[GC-diag2] g%d-%d: %s", g, r, cfg.groups[g][r].DebugState())
			}
		}
		t.Fatalf("new owner g1 does not serve shard %d after move (want OK)", shard)
	}
	if v := ck.Get("gckey"); v != "v" {
		t.Fatalf("after GC Get(gckey)=%q want \"v\"", v)
	}
}
