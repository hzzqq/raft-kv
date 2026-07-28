package cluster

import (
	"fmt"
	"net"
	"testing"

	"raftkv/src/raft"
	"raftkv/src/shardmaster"
)

// TestTCPRPCPipe 是跨机传输管道的隔离诊断：不启动真实 raft，仅验证
// newTransportEnd → 真实 TCP → serveNode → handler 的一来一回中，
// args 能完整到达、reply 能完整返回（gob 编解码 + 帧协议 + 方法路由）。
func TestTCPRPCPipe(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()

	gotArgs := make(chan *raft.RequestPreVoteArgs, 1)
	handler := func(method string, args, reply interface{}) {
		switch method {
		case "RequestPreVote":
			a := args.(*raft.RequestPreVoteArgs)
			gotArgs <- a
			r := reply.(*raft.RequestPreVoteReply)
			r.Term = a.Term
			r.VoteGranted = true
		case "ShardMaster.Query":
			r := reply.(*shardmaster.QueryReply)
			r.Err = shardmaster.OK
			r.Config = shardmaster.Config{Num: 42}
		default:
			panic(fmt.Sprintf("unexpected method %s", method))
		}
	}
	srv, err := serveNode("diag", "sm", handler, addr)
	if err != nil {
		t.Fatalf("serveNode: %v", err)
	}
	defer srv.Stop()

	end, cc := newTransportEnd(addr)
	defer cc.Close()

	// raft 共识方法
	args := &raft.RequestPreVoteArgs{Term: 7, CandidateId: 3, LastLogIndex: 5, LastLogTerm: 2}
	reply := &raft.RequestPreVoteReply{}
	if ok := end.Call("RequestPreVote", args, reply); !ok {
		t.Fatal("RequestPreVote RPC 失败（Call 返回 false）")
	}
	a := <-gotArgs
	if a.Term != 7 || a.CandidateId != 3 || a.LastLogIndex != 5 || a.LastLogTerm != 2 {
		t.Fatalf("服务端收到的 args 不完整: %+v", a)
	}
	if !reply.VoteGranted || reply.Term != 7 {
		t.Fatalf("客户端收到的 reply 不完整: %+v", reply)
	}

	// 业务方法（带嵌套结构体的 reply）
	qr := &shardmaster.QueryReply{}
	if ok := end.Call("ShardMaster.Query", &shardmaster.QueryArgs{Num: -1}, qr); !ok {
		t.Fatal("ShardMaster.Query RPC 失败")
	}
	if qr.Err != shardmaster.OK || qr.Config.Num != 42 {
		t.Fatalf("Query reply 不完整: %+v", qr)
	}
}
