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

	"raftkv/src/diagnostics"
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

// NodeDiagnostics 是单个节点进程的健康快照（跨机运维可观测性）。
//
// 背景：真·跨机部署下每个 kvnode 进程只持有一个节点（ShardMaster 副本或 group 副本），
// 而已有 HTTP 端点只存在于 gateway 进程——一旦某台机器上的节点异常（落后、卡在
// pendingIn、丢了 leader 租约），运维无从查询该节点自身状态，只能翻日志。本结构把
// 「Raft 运行状态 + 分片持有/迁移状态 + diagnostics 判定」聚合成一份可 JSON 序列化的
// 快照，由 kvnode 的 -http 端点自曝，使跨机巡检可以逐节点 curl。
// 字段命名沿用 gateway.ShardDebugView 的惯例（PascalCase、可选字段 omitempty），
// 使本端点与既有 /debug/shards 输出风格一致，运维侧解析逻辑可直接复用。
type NodeDiagnostics struct {
	Name      string
	Kind      string // "shardmaster" | "shardkv"
	ConfigNum int    // 本节点已生效的配置版本号
	// RaftRole 是 Raft 角色的文字形式。raft.Role 底层是 int 且未实现 MarshalJSON，
	// 直接序列化只得到 0/1/2，人工 curl 时得查表；故冗余一份可读值。
	RaftRole   string                 `json:",omitempty"`
	Raft       *raft.RaftStatus       `json:",omitempty"` // Raft 运行状态（任期/提交进度/租约）
	Shard      *shardkv.ShardDebug    `json:",omitempty"` // 分片持有与迁移未决态（仅 ShardKV）
	RaftCheck  *diagnostics.Diagnosis `json:",omitempty"` // Raft 层不变量自检
	ShardCheck *diagnostics.Diagnosis `json:",omitempty"` // 分片层不变量自检（仅 ShardKV）
}

// Diagnostics 采集本节点的健康快照。只读、无副作用，可安全地在 HTTP 处理中并发调用。
func (n *TCPNode) Diagnostics() NodeDiagnostics {
	d := NodeDiagnostics{Name: n.Name}
	switch {
	case n.kv != nil:
		d.Kind = "shardkv"
		rs := n.kv.RaftStatus()
		sd := n.kv.ShardDebug()
		d.Raft, d.Shard = &rs, &sd
		d.RaftRole = rs.Role.String()
		d.ConfigNum = sd.ConfigNum
		rc, sc := diagnostics.RaftCheck(rs), diagnostics.ShardCheck(sd)
		d.RaftCheck, d.ShardCheck = &rc, &sc
	case n.sm != nil:
		d.Kind = "shardmaster"
		rs := n.sm.RaftStatus()
		d.Raft = &rs
		d.RaftRole = rs.Role.String()
		rc := diagnostics.RaftCheck(rs)
		d.RaftCheck = &rc
	default:
		d.Kind = "unknown"
	}
	return d
}

// ProposeConfChange 对本节点所属的 raft 组提议一次成员（投票配置）变更（Ongaro §6
// 单服变更，I192）。仅当本节点是该组 leader 时生效，返回 (条目索引, 是否 leader)。
// 运维端点 /admin/reconfigure 调用它来**热增删 witness / 副本**而不重启集群：非 leader
// 节点返回 false，调用方应重定向到组 leader 重试。
func (n *TCPNode) ProposeConfChange(voters []int) (int, bool) {
	switch {
	case n.kv != nil:
		return n.kv.Raft().ProposeConfChange(voters)
	case n.sm != nil:
		return n.sm.Raft().ProposeConfChange(voters)
	default:
		return -1, false
	}
}

// VoterConfig 返回本节点所属 raft 组当前已提交的投票成员集合快照（运维观测用，I192）。
func (n *TCPNode) VoterConfig() []int {
	switch {
	case n.kv != nil:
		return n.kv.Raft().VoterConfig()
	case n.sm != nil:
		return n.sm.Raft().VoterConfig()
	default:
		return nil
	}
}

// ValidateConfChange 校验运维侧提议的成员变更目标集合是否合法（I192 防误操作护栏）：
// 拦截空集合 / 越界成员 / 重复成员，避免把集群推入 quorum 永远凑不齐的卡死状态。
// 运维端点 /admin/reconfigure 在调用 ProposeConfChange 前应先用它做 400 预检。
func (n *TCPNode) ValidateConfChange(voters []int) error {
	switch {
	case n.kv != nil:
		return n.kv.Raft().ValidateConfChange(voters)
	case n.sm != nil:
		return n.sm.Raft().ValidateConfChange(voters)
	default:
		return fmt.Errorf("node has no raft group")
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
	// witness 节点名 "g<g>-w<k>" 不参与普通 parseNodeName（会失败），先判定。
	isWitnessNode := false
	isSM, j, g, r := false, -1, -1, -1
	if wg, _, ok := parseWitnessName(name); ok {
		isWitnessNode = true
		g = wg
	} else {
		var perr error
		isSM, j, g, r, perr = parseNodeName(name)
		if perr != nil {
			return nil, perr
		}
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
		if g >= cfg.NGroups {
			return nil, fmt.Errorf("StartNodeTCP: %s 超出 n_groups=%d", name, cfg.NGroups)
		}
		if !isWitnessNode && r >= cfg.NReplicas {
			return nil, fmt.Errorf("StartNodeTCP: %s 超出 n_replicas=%d", name, cfg.NReplicas)
		}
		// 构造本组 raft peers：投票副本 + witness（顺序同 StartClusterTCP / groupPeerNames）。
		names := groupPeerNames(cfg, g)
		idx := -1
		for i, nm := range names {
			if nm == name {
				idx = i
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("StartNodeTCP: 配置缺少本组节点 %q", name)
		}
		peers := make([]*raft.ClientEnd, len(names))
		for i, nm := range names {
			peers[i] = makeEnd(nm)
		}
		applyCh := make(chan raft.ApplyMsg, 4000)
		var rf *raft.Raft
		if isWitnessNode {
			rf = raft.MakeWitness(peers, idx, pf("kv", g, idx), applyCh)
		} else {
			rf = raft.Make(peers, idx, pf("kv", g, idx), applyCh)
		}
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
