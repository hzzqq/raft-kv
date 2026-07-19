// labrpc.go —— 极简进程内 RPC 层（仿 MIT 6.824 的 labrpc）
// 不依赖网络，用 channel 在进程内投递 RPC，便于确定性测试与分区模拟。
package raft

import "sync"

// ClientEnd 代表一个"指向某个 server 的客户端端点"。
// Raft 节点的 peers[i] 就是这样一个端点，调用 Call 即向第 i 个节点发 RPC。
type ClientEnd struct {
	net      *Network
	endname  int // 本端点名字（用于网络路由）
	owner    int // 拥有该端点的 server（用于判断"发送方是否在线"）
	serverId int // 真正要送达的 server id
}

// Call 同步发起一次 RPC，返回是否成功送达（收发任一方掉线则为 false）。
func (e *ClientEnd) Call(method string, args interface{}, reply interface{}) bool {
	return e.net.Send(e.endname, method, args, reply)
}

// RpcMsg 是网络在节点之间传递的 RPC 信封。
type RpcMsg struct {
	method string
	args   interface{}
	reply  interface{}
	done   chan struct{}
}

// Server 是一个 Raft 节点对应的网络端点，后台 goroutine 不断取出 RPC 并分派。
type Server struct {
	id  int
	ch  chan *RpcMsg
	done chan struct{}
}

func (s *Server) loop(handler func(method string, args, reply interface{})) {
	for {
		select {
		case m := <-s.ch:
			handler(m.method, m.args, m.reply)
			close(m.done)
		case <-s.done:
			// 关闭时要排空尚未处理的 RPC：否则若有 Send 正阻塞在 <-m.done，
			// 旧循环直接 return 会让发送方永久挂起（节点重启竞态）。
			for {
				select {
				case m := <-s.ch:
					close(m.done)
				default:
					return
				}
			}
		}
	}
}

// Network 管理所有 server 与 endpoint，并提供 Enable/Disable 来模拟网络分区。
type Network struct {
	mu      sync.Mutex
	servers map[int]*Server
	ends    map[int]*ClientEnd
	enabled map[int]bool // 每个 server 是否可达
}

func MakeNetwork() *Network {
	return &Network{
		servers: make(map[int]*Server),
		ends:    make(map[int]*ClientEnd),
		enabled: make(map[int]bool),
	}
}

// AddServer 注册一个 Raft 节点，handler 负责把 RPC 分派给具体方法。
// 若已存在同 id 的 server，会先停掉旧循环再重建（便于节点重启）。
func (n *Network) AddServer(id int, handler func(method string, args, reply interface{})) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if old, ok := n.servers[id]; ok {
		close(old.done) // 通知旧循环退出
	}
	s := &Server{id: id, ch: make(chan *RpcMsg, 200), done: make(chan struct{})}
	go s.loop(handler)
	n.servers[id] = s
	n.enabled[id] = true
}

// MakeEnd 创建一个客户端端点（名字 endname 全局唯一，owner 是拥有该端点的 server）。
func (n *Network) MakeEnd(endname int, owner int) *ClientEnd {
	n.mu.Lock()
	defer n.mu.Unlock()
	e := &ClientEnd{net: n, endname: endname, owner: owner}
	n.ends[endname] = e
	return e
}

// Connect 把端点 endname 连到 serverId。
func (n *Network) Connect(endname int, serverId int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if e, ok := n.ends[endname]; ok {
		e.serverId = serverId
	}
}

// Enable 控制某个 server 是否可达（false 模拟该节点掉线/分区）。
func (n *Network) Enable(serverId int, b bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.enabled[serverId] = b
}

// Send 把 RPC 投递给目标 server；收发任一方掉线或 server 不存在时返回 false。
func (n *Network) Send(endname int, method string, args interface{}, reply interface{}) bool {
	n.mu.Lock()
	e, ok := n.ends[endname]
	if !ok {
		n.mu.Unlock()
		return false
	}
	sid := e.serverId     // 接收方
	oid := e.owner        // 发送方
	srv, exists := n.servers[sid]
	enabledDst := n.enabled[sid]
	// 未注册的 owner（例如客户端/Clerk 端点）视为可达；只有显式 Enable(oid,false) 才不可达。
	enabledSrc := true
	if v, ok := n.enabled[oid]; ok {
		enabledSrc = v
	}
	n.mu.Unlock()

	if !exists || !enabledDst || !enabledSrc {
		return false
	}
	m := &RpcMsg{method: method, args: args, reply: reply, done: make(chan struct{})}
	// 若投递过程中该 server 被重启/关停（done 关闭），直接放弃本次 RPC，
	// 让上层按"不可达"重试，避免阻塞在 <-m.done 造成死锁。
	select {
	case srv.ch <- m:
		<-m.done
		return true
	case <-srv.done:
		return false
	}
}

// Cleanup 关闭所有 server 的循环。
func (n *Network) Cleanup() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, s := range n.servers {
		close(s.done)
	}
}
