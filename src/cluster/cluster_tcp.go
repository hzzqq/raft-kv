// cluster_tcp.go —— 跨进程 / 跨机部署：用真实 TCP 传输层（src/transport）串起集群。
//
// StartCluster 走进程内 labrpc 内存网络；本文件提供 StartClusterTCP，让节点间 RPC
// 走真实 TCP（localhost 或跨机），从而可以把 ShardMaster / 各 replica group 分布到
// 不同进程甚至不同机器。两者共用同一套 raft / shardkv / shardmaster 业务逻辑，仅
// "传输层"不同——这正是 transport 包存在的意义。
package cluster

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"reflect"
	"time"

	"raftkv/src/raft"
	"raftkv/src/shardkv"
	"raftkv/src/shardmaster"
	"raftkv/src/transport"
)

// tcpRPCTimeout 是跨机 RPC 的单次调用超时。raft 心跳周期约 100ms、选举超时数百 ms，
// 上层对失败的语义就是"重试"，因此超时宜短：过长会让失败节点拖慢选举收敛，
// 且每个在途 Invoke 都占一个 goroutine——无超时曾导致 goroutine 泄漏到千万级（挂死复盘）。
const tcpRPCTimeout = 2 * time.Second

// gobEncode / gobDecode：跨机传输采用 gob 而非 JSON。
// 关键原因：raft 日志条目的 Command 是 interface{}（如 shardkv.Op），业务包已在 init()
// 里 gob.Register 具体类型，gob 能带类型信息还原；JSON 解码只会得到
// map[string]interface{}，follower 状态机的 msg.Command.(Op) 断言必然失败，
// 表现为命令永远不被 apply、客户端无限重试（600s 挂死复盘的主根因）。
func gobEncode(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gobDecode(data []byte, v interface{}) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

// TCPNodeAddr 描述跨机部署中某个真实节点的监听地址。
type TCPNodeAddr struct {
	Name string `json:"name"` // 与内存模式一致：ShardMaster 为 "m<j>"，ShardKV 为 "g<g>-<r>"
	Addr string `json:"addr"` // host:port，节点将在此监听真实 TCP
}

// ClusterTCPConfig 是跨机部署的节点地址清单（可序列化到 JSON 文件，供 gateway/demo 加载）。
type ClusterTCPConfig struct {
	NGroups      int           `json:"n_groups"`
	NReplicas    int           `json:"n_replicas"`
	NSM          int           `json:"n_sm"`
	MaxRaftState int           `json:"max_raft_state"`
	DataDir      string        `json:"data_dir"` // 空=内存；非空=各节点状态落盘到该目录
	Nodes        []TCPNodeAddr `json:"nodes"`
}

// LoadTCPConfig 从 JSON 文件加载跨机部署配置。
func LoadTCPConfig(path string) (ClusterTCPConfig, error) {
	var cfg ClusterTCPConfig
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// StartClusterFromConfig 按配置启动跨机集群：data_dir 非空则落盘（崩溃恢复），否则内存。
func StartClusterFromConfig(cfg ClusterTCPConfig) (*Cluster, error) {
	pf := PersisterFactory(func(string, int, int) raft.Persister { return raft.MakeEmptyPersister() })
	if cfg.DataDir != "" {
		pf = FilePersisterFactory(cfg.DataDir)
	}
	return StartClusterTCP(cfg.NGroups, cfg.NReplicas, cfg.NSM, cfg.MaxRaftState, pf, cfg.Nodes)
}

// StartClusterTCP 用真实 TCP 传输层串起集群，实现跨进程/跨机部署。
// addrs 必须覆盖所有节点（nSM 个 ShardMaster + nGroups*nReplicas 个 ShardKV），
// 命名与内存模式一致（"m0".."m{nSM-1}"，"g0-0"..）。pf 为真部署化持久化工厂。
func StartClusterTCP(nGroups, nReplicas, nSM, maxraftstate int, pf PersisterFactory, addrs []TCPNodeAddr) (*Cluster, error) {
	addrMap := make(map[string]string, len(addrs))
	for _, a := range addrs {
		if _, dup := addrMap[a.Name]; dup {
			return nil, fmt.Errorf("StartClusterTCP: 节点名重复 %q", a.Name)
		}
		addrMap[a.Name] = a.Addr
	}
	// 地址清单完整性校验：SM + KV 都必须在。
	for j := 0; j < nSM; j++ {
		if _, ok := addrMap[fmt.Sprintf("m%d", j)]; !ok {
			return nil, fmt.Errorf("StartClusterTCP: 缺少 ShardMaster 节点 m%d 的地址", j)
		}
	}
	for g := 0; g < nGroups; g++ {
		for r := 0; r < nReplicas; r++ {
			if _, ok := addrMap[fmt.Sprintf("g%d-%d", g, r)]; !ok {
				return nil, fmt.Errorf("StartClusterTCP: 缺少 ShardKV 节点 g%d-%d 的地址", g, r)
			}
		}
	}

	c := &Cluster{
		nameToID:     map[string]int{},
		cache:        map[string]*raft.ClientEnd{},
		Groups:       make([][]string, nGroups),
		KVs:          make([][]*shardkv.ShardKV, nGroups),
		nGroups:      nGroups,
		nReplicas:    nReplicas,
		nSM:          nSM,
		maxraftstate: maxraftstate,
	}
	c.make_end = func(name string) *raft.ClientEnd {
		addr, ok := addrMap[name]
		if !ok {
			panic(fmt.Sprintf("StartClusterTCP: 缺少节点 %q 的地址", name))
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if e, ok := c.cache[name]; ok {
			return e
		}
		e, cc := newTransportEnd(addr)
		c.cache[name] = e
		c.tcpConns = append(c.tcpConns, cc)
		return e
	}

	// ---- ShardMaster 集群 ----
	for j := 0; j < nSM; j++ {
		name := fmt.Sprintf("m%d", j)
		c.nameToID[name] = j
		c.SMNames = append(c.SMNames, name)
	}
	for j := 0; j < nSM; j++ {
		peers := make([]*raft.ClientEnd, nSM)
		for k := 0; k < nSM; k++ {
			peers[k] = c.make_end(fmt.Sprintf("m%d", k))
		}
		p := pf("sm", -1, j)
		sm := shardmaster.Make(peers, j, p)
		c.SM = append(c.SM, sm)
		name := fmt.Sprintf("m%d", j)
		jj := j
		handler := func(method string, args, reply interface{}) {
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
				panic(fmt.Sprintf("sm%d unexpected method %s", jj, method))
			}
		}
		srv, err := serveNode(name, "sm", handler, addrMap[name])
		if err != nil {
			return nil, err
		}
		c.tcpServers = append(c.tcpServers, srv)
	}

	// ---- ShardKV 各 group ----
	for g := 0; g < nGroups; g++ {
		for r := 0; r < nReplicas; r++ {
			name := fmt.Sprintf("g%d-%d", g, r)
			c.nameToID[name] = 1000 + g*100 + r
			c.Groups[g] = append(c.Groups[g], name)

			peers := make([]*raft.ClientEnd, nReplicas)
			for r2 := 0; r2 < nReplicas; r2++ {
				peers[r2] = c.make_end(fmt.Sprintf("g%d-%d", g, r2))
			}
			applyCh := make(chan raft.ApplyMsg, 4000)
			p := pf("kv", g, r)
			rf := raft.Make(peers, r, p, applyCh)
			kv := shardkv.MakeShardKV(g+1, c.SMNames, c.make_end, rf, applyCh, maxraftstate)
			c.KVs[g] = append(c.KVs[g], kv)

			gg, rr := g, r
			handler := func(method string, args, reply interface{}) {
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
					panic(fmt.Sprintf("g%d-%d unexpected method %s", gg, rr, method))
				}
			}
			srv, err := serveNode(name, "kv", handler, addrMap[name])
			if err != nil {
				return nil, err
			}
			c.tcpServers = append(c.tcpServers, srv)
		}
	}
	return c, nil
}

// serveNode 在 addr 上起一个 transport.Server，注册该节点的 RPC 方法分发，后台 Serve。
func serveNode(name, kind string, handler func(string, interface{}, interface{}), addr string) (*transport.Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("serveNode %s: %w", name, err)
	}
	srv := transport.NewServer()
	srv.Register(nodeServiceDesc(kind, handler))
	go srv.Serve(lis)
	return srv, nil
}

// newTransportEnd 构造一个走真实 TCP 的客户端端点，返回端点与底层连接（供 Cleanup 关闭）。
// 编解码用 gob（见 gobEncode 注释）；每次调用带 tcpRPCTimeout 超时——上层 raft/clerk
// 对失败的语义就是重试，超时兜底防止对端 hang 时 goroutine 无限堆积。
func newTransportEnd(addr string) (*raft.ClientEnd, *transport.ClientConn) {
	cc := transport.Dial(addr)
	end := raft.MakeSendFnEnd(func(method string, args, reply interface{}) bool {
		reqData, err := gobEncode(args)
		if err != nil {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), tcpRPCTimeout)
		defer cancel()
		// 与 nodeServiceDesc 注册的 "/raft/<method>" 对应。
		respData, err := cc.Invoke(ctx, "/raft/"+method, reqData)
		if err != nil {
			return false
		}
		if err := gobDecode(respData, reply); err != nil {
			return false
		}
		return true
	})
	return end, cc
}

// nodeServiceDesc 为某类节点（sm/kv）构造 transport.ServiceDesc：把每个 RPC 方法映射到
// 「gob 反序列化 → 调用节点 handler → gob 序列化」的处理器。方法名注册为 "/raft/<method>"，
// 与客户端发送的完整方法名一致。编解码必须用 gob（Command interface{} 类型还原，见文件头）。
func nodeServiceDesc(kind string, handler func(string, interface{}, interface{})) transport.ServiceDesc {
	// 每次调用都必须 new 全新的 args/reply 实例：并发 RPC（raft 心跳/选举是并发的）
	// 若复用同一对指针会互相踩数据（数据竞态）。这里存原型的反射类型，按需实例化。
	methods := map[string]func() (interface{}, interface{}){}
	add := func(m string, a, r interface{}) {
		at, rt := reflect.TypeOf(a).Elem(), reflect.TypeOf(r).Elem()
		methods[m] = func() (interface{}, interface{}) {
			return reflect.New(at).Interface(), reflect.New(rt).Interface()
		}
	}
	// raft 共识方法（两类节点共有）
	add("RequestVote", &raft.RequestVoteArgs{}, &raft.RequestVoteReply{})
	add("RequestPreVote", &raft.RequestPreVoteArgs{}, &raft.RequestPreVoteReply{})
	add("AppendEntries", &raft.AppendEntriesArgs{}, &raft.AppendEntriesReply{})
	add("InstallSnapshot", &raft.InstallSnapshotArgs{}, &raft.InstallSnapshotReply{})
	add("TimeoutNow", &raft.TimeoutNowArgs{}, &raft.TimeoutNowReply{})
	if kind == "sm" {
		add("ShardMaster.Join", &shardmaster.JoinArgs{}, &shardmaster.JoinReply{})
		add("ShardMaster.Leave", &shardmaster.LeaveArgs{}, &shardmaster.LeaveReply{})
		add("ShardMaster.Move", &shardmaster.MoveArgs{}, &shardmaster.MoveReply{})
		add("ShardMaster.Query", &shardmaster.QueryArgs{}, &shardmaster.QueryReply{})
	} else {
		add("ShardKV.Get", &shardkv.GetArgs{}, &shardkv.GetReply{})
		add("ShardKV.PutAppend", &shardkv.PutAppendArgs{}, &shardkv.PutAppendReply{})
		add("ShardKV.SendShard", &shardkv.SendShardArgs{}, &shardkv.SendShardReply{})
		add("ShardKV.GetShard", &shardkv.GetShardArgs{}, &shardkv.GetShardReply{})
	}
	handlers := map[string]transport.MethodHandler{}
	for m, mk := range methods {
		mm := m
		makeArgs := mk
		// transport.Register 会把 ServiceDesc.Name("raft") 与方法名拼成 "/raft/<m>"。
		handlers[m] = func(ctx context.Context, reqData []byte) ([]byte, error) {
			args, reply := makeArgs()
			if err := gobDecode(reqData, args); err != nil {
				return nil, err
			}
			handler(mm, args, reply)
			return gobEncode(reply)
		}
	}
	return transport.ServiceDesc{Name: "raft", Methods: handlers}
}
