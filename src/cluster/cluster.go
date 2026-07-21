// cluster.go —— 可复用的进程内 ShardKV 集群框架
//
// 把 shardkv_test.go 里 makeSKVConfig 的"内存 labrpc 网络 + ShardMaster 集群 +
// 多 replica group"搭建逻辑抽成独立、可 import 的包，供 demo / gateway / kvcli
// 等上层组件复用，避免重复造轮子。
//
// 约定：第 g 个 group 的 gid = g+1（避开 shardmaster 中 gid 0 的"未分配"哨兵），
// 因此本包所有接收组下标的方法内部统一 +1 作为 shardmaster 的 gid。
package cluster

import (
	"fmt"
	"sync"
	"time"

	"raftkv/src/raft"
	"raftkv/src/shardkv"
	"raftkv/src/shardmaster"
)

// Cluster 持有一个完整的内存 ShardKV 集群（含 ShardMaster）。
type Cluster struct {
	mu       sync.Mutex
	Net      *raft.Network
	SMNames  []string
	nameToID map[string]int
	cache    map[string]*raft.ClientEnd
	make_end func(string) *raft.ClientEnd

	Groups       [][]string
	KVs          [][]*shardkv.ShardKV
	SM           []*shardmaster.ShardMaster
	nGroups      int
	nReplicas    int
	nSM          int
	maxraftstate int
}

// StartCluster 启动一个含 nSM 个 ShardMaster 副本、nGroups 个 replica group
// （每组 nReplicas 副本）的内存集群。maxraftstate>0 时开启日志压缩。
func StartCluster(nGroups, nReplicas, nSM, maxraftstate int) *Cluster {
	net := raft.MakeNetwork()
	c := &Cluster{
		Net:          net,
		nameToID:     map[string]int{},
		cache:        map[string]*raft.ClientEnd{},
		Groups:       make([][]string, nGroups),
		KVs:          make([][]*shardkv.ShardKV, nGroups),
		nGroups:      nGroups,
		nReplicas:    nReplicas,
		nSM:          nSM,
		maxraftstate: maxraftstate,
	}

	make_end := func(name string) *raft.ClientEnd {
		c.mu.Lock()
		defer c.mu.Unlock()
		if e, ok := c.cache[name]; ok {
			return e
		}
		id := c.nameToID[name]
		e := net.MakeEnd(8000+id, 9000) // owner 9000 未注册 -> 视为可达
		net.Connect(8000+id, id)
		c.cache[name] = e
		return e
	}
	c.make_end = make_end

	// ---- ShardMaster 集群 ----
	for j := 0; j < nSM; j++ {
		name := fmt.Sprintf("m%d", j)
		c.SMNames = append(c.SMNames, name)
		c.nameToID[name] = j
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
		c.SM = append(c.SM, sm)
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
				panic(fmt.Sprintf("sm%d unexpected method %s", jj, method))
			}
		})
	}

	// ---- ShardKV 各 group ----
	for g := 0; g < nGroups; g++ {
		for r := 0; r < nReplicas; r++ {
			name := fmt.Sprintf("g%d-%d", g, r)
			id := 1000 + g*100 + r
			c.nameToID[name] = id
			c.Groups[g] = append(c.Groups[g], name)

			peers := make([]*raft.ClientEnd, nReplicas)
			for r2 := 0; r2 < nReplicas; r2++ {
				e := net.MakeEnd(id*nReplicas+r2, id)
				net.Connect(id*nReplicas+r2, 1000+g*100+r2)
				peers[r2] = e
			}
			applyCh := make(chan raft.ApplyMsg, 4000)
			p := raft.MakeEmptyPersister()
			rf := raft.Make(peers, r, p, applyCh)
			kv := shardkv.MakeShardKV(g+1, c.SMNames, make_end, rf, applyCh, maxraftstate)
			c.KVs[g] = append(c.KVs[g], kv)

			// 捕获 g/r 为局部副本，避免闭包共享循环变量（仅 default panic 分支用到）。
			gg, rr := g, r
			net.AddServer(id, func(method string, args, reply interface{}) {
				switch method {
				case "RequestVote":
					rf.RequestVote(args.(*raft.RequestVoteArgs), reply.(*raft.RequestVoteReply))
				case "AppendEntries":
					rf.AppendEntries(args.(*raft.AppendEntriesArgs), reply.(*raft.AppendEntriesReply))
				case "InstallSnapshot":
					rf.InstallSnapshot(args.(*raft.InstallSnapshotArgs), reply.(*raft.InstallSnapshotReply))
				case "ShardKV.Get":
					kv.Get(args.(*shardkv.GetArgs), reply.(*shardkv.GetReply))
				case "ShardKV.PutAppend":
					kv.PutAppend(args.(*shardkv.PutAppendArgs), reply.(*shardkv.PutAppendReply))
				case "ShardKV.SendShard":
					kv.SendShard(args.(*shardkv.SendShardArgs), reply.(*shardkv.SendShardReply))
				case "ShardKV.GetShard":
					kv.GetShard(args.(*shardkv.GetShardArgs), reply.(*shardkv.GetShardReply))
				default:
					panic(fmt.Sprintf("g%d-%d unexpected method %s", gg, rr, method))
				}
			})
		}
	}
	return c
}

// Clerk 返回一个绑定到本集群 ShardMaster 的 ShardKV 客户端。
func (c *Cluster) Clerk() *shardkv.Clerk {
	return shardkv.MakeClerk(c.SMNames, c.make_end)
}

// Configs 返回 shardmaster 的完整配置历史（configs[0..latest]），供网关
// /debug/configs 展示 rebalance 轨迹。经公开的 shardmaster.Clerk 查 leader 副本，
// 逐号 Query 取回每段配置（调试用途，允许读到各副本已提交状态）。
func (c *Cluster) Configs() []shardmaster.Config {
	ck := shardmaster.MakeClerk(c.SMNames, c.make_end)
	latest := ck.Query(-1)
	out := make([]shardmaster.Config, 0, latest.Num+1)
	for i := 0; i <= latest.Num; i++ {
		out = append(out, ck.Query(i))
	}
	return out
}

// Join 把第 g 个 group 加入集群（gid = g+1）。
func (c *Cluster) Join(g int) {
	ck := shardmaster.MakeClerk(c.SMNames, c.make_end)
	ck.Join(map[int][]string{g + 1: append([]string{}, c.Groups[g]...)})
}

// Leave 把第 g 个 group 移出集群。
func (c *Cluster) Leave(g int) {
	ck := shardmaster.MakeClerk(c.SMNames, c.make_end)
	ck.Leave([]int{g + 1})
}

// Move 把某个分片迁到第 g 个 group（gid = g+1）。
func (c *Cluster) Move(shard, g int) {
	ck := shardmaster.MakeClerk(c.SMNames, c.make_end)
	ck.Move(shard, g+1)
}

// ConfigNum 返回第 g 个 group 第 r 个副本当前生效的配置版本号。
func (c *Cluster) ConfigNum(g, r int) int {
	return c.KVs[g][r].ConfigNum()
}

// WaitConfig 轮询直到第 g 个 group 第 r 个副本的配置推进到 num（最多 ~5s）。
func (c *Cluster) WaitConfig(g, r, num int) {
	for i := 0; i < 100; i++ {
		if c.ConfigNum(g, r) >= num {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// WaitAllConfigs 轮询直到所有 group 的配置都推进到 >= num（每 group 最多 ~5s）。
func (c *Cluster) WaitAllConfigs(num int) {
	for g := 0; g < c.nGroups; g++ {
		c.WaitConfig(g, 0, num)
	}
}

// Churn 在 groups 间做 rounds 轮分片漂移：每轮把第 (i*shardStep)%NShards 号分片迁到
// 第 i%nGroups 组，轮间间隔 interval。制造可控的「多 group 迁移并发」，供测试断言
// 配置持续推进、数据不丢。属于 Move 式 churn（非 Join/Leave 再平衡），是安全迁移路径。
func (c *Cluster) Churn(rounds int, interval time.Duration, shardStep int) {
	for i := 0; i < rounds; i++ {
		c.Move((i*shardStep)%shardmaster.NShards, i%c.nGroups)
		time.Sleep(interval)
	}
}

// Cleanup 关闭所有 ShardKV / ShardMaster 并清理网络（回收 goroutine）。
func (c *Cluster) Cleanup() {
	for g := 0; g < c.nGroups; g++ {
		for r := 0; r < c.nReplicas; r++ {
			if c.KVs[g][r] != nil {
				c.KVs[g][r].Kill()
			}
		}
	}
	for j := 0; j < c.nSM; j++ {
		if c.SM[j] != nil {
			c.SM[j].Kill()
		}
	}
	c.Net.Cleanup()
}
