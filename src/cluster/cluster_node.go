// cluster_node.go —— 真·跨机部署的两块拼图（I21/I22）：
//
//  1. StartNodeTCP：**单节点**启动。一个 OS 进程只跑地址清单中的一个节点
//     （ShardMaster "m<j>" 或 ShardKV "g<g>-<r>"），节点间 RPC 走真实 TCP。
//     与 StartClusterTCP（单进程起全部节点、TCP 只在 loopback 上走）不同，
//     这才是「每台机器跑一个进程」的部署形态，配套进程入口见 src/kvnode。
//  2. ConnectTCP：**纯客户端**接入。不在本进程起任何节点，只按地址清单构造
//     make_end，返回一个"远程视图"的 Cluster——Clerk/Join/Move/Configs 可用，
//     供 gateway 以 -connect 模式挂到一个已在别处运行的集群前面。
//
// 两者共用 cluster_tcp.go 的 serveNode / nodeServiceDesc / newTransportEnd，
// 序列化同样必须是 gob（raft 日志 Command 是 interface{}，见 cluster_tcp.go 头注）。
package cluster

import (
	"fmt"
	"sync"

	"raftkv/src/raft"
	"raftkv/src/shardkv"
	"raftkv/src/shardmaster"
	"raftkv/src/transport"
)

// TCPNode 是单节点进程内的运行句柄（StartNodeTCP 的返回值）。
type TCPNode struct {
	Name string // "m<j>" 或 "g<g>-<r>"

	srv   *transport.Server
	conns []*transport.ClientConn
	sm    *shardmaster.ShardMaster // 仅 ShardMaster 节点非 nil
	kv    *shardkv.ShardKV         // 仅 ShardKV 节点非 nil
}

// Stop 停止节点：杀业务状态机、关 TCP 服务端与全部出向连接。
func (n *TCPNode) Stop() {
	if n.kv != nil {
		n.kv.Kill()
	}
	if n.sm != nil {
		n.sm.Kill()
	}
	if n.srv != nil {
		n.srv.Stop()
	}
	for _, cc := range n.conns {
		_ = cc.Close()
	}
}

// parseNodeName 解析节点名："m<j>" → (true, j, -1, -1)；"g<g>-<r>" → (false, -1, g, r)。
func parseNodeName(name string) (isSM bool, j, g, r int, err error) {
	if len(name) > 1 && name[0] == 'm' {
		if _, e := fmt.Sscanf(name, "m%d", &j); e == nil {
			return true, j, -1, -1, nil
		}
	}
	if len(name) > 1 && name[0] == 'g' {
		if _, e := fmt.Sscanf(name, "g%d-%d", &g, &r); e == nil {
			return false, -1, g, r, nil
		}
	}
	return false, 0, 0, 0, fmt.Errorf("非法节点名 %q（期望 m<j> 或 g<g>-<r>）", name)
}

// buildEndFactory 按地址清单构造 make_end（带连接缓存）。返回工厂函数与「已建连接
// 收集器」，收集器供调用方在 Stop/Cleanup 时统一关闭。**必须并发安全**：shardkv 的
// 分片迁移（sendShard/fetchShard）与 SM clerk 会在运行期从多个 goroutine 并发调用
// make_end（首个测试运行即抓到 concurrent map access panic）。
func buildEndFactory(addrs []TCPNodeAddr) (func(string) *raft.ClientEnd, *[]*transport.ClientConn, error) {
	addrMap := make(map[string]string, len(addrs))
	for _, a := range addrs {
		if _, dup := addrMap[a.Name]; dup {
			return nil, nil, fmt.Errorf("节点名重复 %q", a.Name)
		}
		addrMap[a.Name] = a.Addr
	}
	var mu sync.Mutex
	cache := map[string]*raft.ClientEnd{}
	conns := &[]*transport.ClientConn{}
	makeEnd := func(name string) *raft.ClientEnd {
		mu.Lock()
		defer mu.Unlock()
		if e, ok := cache[name]; ok {
			return e
		}
		addr, ok := addrMap[name]
		if !ok {
			panic(fmt.Sprintf("make_end: 地址清单缺少节点 %q", name))
		}
		e, cc := newTransportEnd(addr)
		cache[name] = e
		*conns = append(*conns, cc)
		return e
	}
	return makeEnd, conns, nil
}

// StartNodeTCP 在本进程启动地址清单中名为 name 的**单个**节点并开始对外服务。
// cfg.DataDir 非空则该节点状态落盘（崩溃后同目录重启即恢复）。
func StartNodeTCP(cfg ClusterTCPConfig, name string) (*TCPNode, error) {
	isSM, j, g, r, err := parseNodeName(name)
	if err != nil {
		return nil, err
	}
	var addr string
	for _, a := range cfg.Nodes {
		if a.Name == name {
			addr = a.Addr
		}
	}
	if addr == "" {
		return nil, fmt.Errorf("StartNodeTCP: 地址清单缺少节点 %q", name)
	}
	makeEnd, conns, err := buildEndFactory(cfg.Nodes)
	if err != nil {
		return nil, err
	}
	pf := PersisterFactory(func(string, int, int) raft.Persister { return raft.MakeEmptyPersister() })
	if cfg.DataDir != "" {
		pf = FilePersisterFactory(cfg.DataDir)
	}
	smNames := make([]string, cfg.NSM)
	for k := range smNames {
		smNames[k] = fmt.Sprintf("m%d", k)
	}

	node := &TCPNode{Name: name}
	var kind string
	var handler func(string, interface{}, interface{})

	if isSM {
		if j >= cfg.NSM {
			return nil, fmt.Errorf("StartNodeTCP: m%d 超出 n_sm=%d", j, cfg.NSM)
		}
		peers := make([]*raft.ClientEnd, cfg.NSM)
		for k := 0; k < cfg.NSM; k++ {
			peers[k] = makeEnd(fmt.Sprintf("m%d", k))
		}
		sm := shardmaster.Make(peers, j, pf("sm", -1, j))
		node.sm = sm
		kind = "sm"
		handler = func(method string, args, reply interface{}) {
			switch method {
			case "RequestVote", "RequestPreVote", "AppendEntries", "InstallSnapshot", "TimeoutNow":
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
				panic(fmt.Sprintf("%s unexpected method %s", name, method))
			}
		}
	} else {
		if g >= cfg.NGroups || r >= cfg.NReplicas {
			return nil, fmt.Errorf("StartNodeTCP: %s 超出 n_groups=%d/n_replicas=%d", name, cfg.NGroups, cfg.NReplicas)
		}
		peers := make([]*raft.ClientEnd, cfg.NReplicas)
		for r2 := 0; r2 < cfg.NReplicas; r2++ {
			peers[r2] = makeEnd(fmt.Sprintf("g%d-%d", g, r2))
		}
		applyCh := make(chan raft.ApplyMsg, 4000)
		rf := raft.Make(peers, r, pf("kv", g, r), applyCh)
		kv := shardkv.MakeShardKV(g+1, smNames, makeEnd, rf, applyCh, cfg.MaxRaftState)
		node.kv = kv
		kind = "kv"
		handler = func(method string, args, reply interface{}) {
			switch method {
			case "RequestVote":
				rf.RequestVote(args.(*raft.RequestVoteArgs), reply.(*raft.RequestVoteReply))
			case "RequestPreVote":
				rf.RequestPreVote(args.(*raft.RequestPreVoteArgs), reply.(*raft.RequestPreVoteReply))
			case "AppendEntries":
				rf.AppendEntries(args.(*raft.AppendEntriesArgs), reply.(*raft.AppendEntriesReply))
			case "InstallSnapshot":
				rf.InstallSnapshot(args.(*raft.InstallSnapshotArgs), reply.(*raft.InstallSnapshotReply))
			case "TimeoutNow":
				rf.TimeoutNow(args.(*raft.TimeoutNowArgs), reply.(*raft.TimeoutNowReply))
			case "ShardKV.Get":
				kv.Get(args.(*shardkv.GetArgs), reply.(*shardkv.GetReply))
			case "ShardKV.PutAppend":
				kv.PutAppend(args.(*shardkv.PutAppendArgs), reply.(*shardkv.PutAppendReply))
			case "ShardKV.SendShard":
				kv.SendShard(args.(*shardkv.SendShardArgs), reply.(*shardkv.SendShardReply))
			case "ShardKV.GetShard":
				kv.GetShard(args.(*shardkv.GetShardArgs), reply.(*shardkv.GetShardReply))
			default:
				panic(fmt.Sprintf("%s unexpected method %s", name, method))
			}
		}
	}

	srv, err := serveNode(name, kind, handler, addr)
	if err != nil {
		node.Stop() // 关闭已建立的出向连接与状态机
		return nil, err
	}
	node.srv = srv
	node.conns = *conns
	return node, nil
}

// ConnectTCP 以**纯客户端**身份接入一个已在别处运行的跨机集群：本进程不起任何节点，
// 仅构造 make_end 远程视图。返回的 Cluster 上 Clerk/Configs/Join/Leave/Move/WaitConfig
// 可用；依赖本地节点句柄的方法（EnableKV/KVRaftStatus/debug 遍历 KVs）自然为空或 panic。
func ConnectTCP(cfg ClusterTCPConfig) (*Cluster, error) {
	makeEnd, conns, err := buildEndFactory(cfg.Nodes)
	if err != nil {
		return nil, err
	}
	c := &Cluster{
		remote:       true,
		nameToID:     map[string]int{},
		cache:        map[string]*raft.ClientEnd{},
		Groups:       make([][]string, cfg.NGroups),
		KVs:          make([][]*shardkv.ShardKV, cfg.NGroups), // 内层空切片：debug 遍历自然降级
		nGroups:      cfg.NGroups,
		nReplicas:    cfg.NReplicas,
		nSM:          cfg.NSM,
		maxraftstate: cfg.MaxRaftState,
	}
	for j := 0; j < cfg.NSM; j++ {
		c.SMNames = append(c.SMNames, fmt.Sprintf("m%d", j))
	}
	for g := 0; g < cfg.NGroups; g++ {
		for r := 0; r < cfg.NReplicas; r++ {
			c.Groups[g] = append(c.Groups[g], fmt.Sprintf("g%d-%d", g, r))
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.make_end = makeEnd
	c.remoteConns = conns
	return c, nil
}

// SMLatestConfigNum 直接向各 ShardMaster 副本发一次 Query(-1)（transport 层带 2s 超时），
// 返回 leader 视角的最新配置号；全部失败/无 leader 返回 -1。与 shardmaster.Clerk.Query
// 的区别：**不无限重试**，适合就绪探针（/readyz）与远程 WaitConfig 轮询这类必须快速
// 返回的场景。
func (c *Cluster) SMLatestConfigNum() int {
	for _, name := range c.SMNames {
		end := c.make_end(name)
		args := shardmaster.QueryArgs{Num: -1}
		var reply shardmaster.QueryReply
		if end.Call("ShardMaster.Query", &args, &reply) && reply.Err == shardmaster.OK {
			return reply.Config.Num
		}
	}
	return -1
}
