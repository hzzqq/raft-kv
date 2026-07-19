// kvraft_test.go —— Lab 3 测试：基于 Raft 的 KV 存储
package kvraft

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"raftkv/src/raft"
)

type kvConfig struct {
	mu         sync.Mutex
	net        *raft.Network
	kvs        []*KVServer
	clerks     []*Clerk
	rafts      [][]*raft.ClientEnd
	kvEnds     []*raft.ClientEnd
	persisters []*raft.Persister
	applyCh    []chan raft.ApplyMsg
	connected  []bool
	n          int
	t          *testing.T
}

func makeKVConfig(t *testing.T, n int) *kvConfig {
	net := raft.MakeNetwork()
	ck := &kvConfig{net: net, n: n, t: t}
	ck.kvs = make([]*KVServer, n)
	ck.clerks = make([]*Clerk, n)
	ck.rafts = make([][]*raft.ClientEnd, n)
	ck.kvEnds = make([]*raft.ClientEnd, n)
	ck.persisters = make([]*raft.Persister, n)
	ck.applyCh = make([]chan raft.ApplyMsg, n)
	ck.connected = make([]bool, n)
	for i := 0; i < n; i++ {
		ck.connected[i] = true
	}

	for i := 0; i < n; i++ {
		ck.rafts[i] = make([]*raft.ClientEnd, n)
		for j := 0; j < n; j++ {
			ck.rafts[i][j] = net.MakeEnd(i*n+j, i)
			net.Connect(i*n+j, j)
		}
		ck.kvEnds[i] = net.MakeEnd(1000+i, 1000+i)
		net.Connect(1000+i, 1000+i)
	}

	for i := 0; i < n; i++ {
		ck.applyCh[i] = make(chan raft.ApplyMsg, 4000)
		ck.persisters[i] = raft.MakeEmptyPersister()
		rf := raft.Make(ck.rafts[i], i, ck.persisters[i], ck.applyCh[i])
		kv := MakeKVServer(i, rf, ck.applyCh[i], 0)
		ck.kvs[i] = kv
		ck.clerks[i] = MakeClerk(ck.kvEnds)

		ii := i
		rrf := rf
		net.AddServer(i, func(method string, args, reply interface{}) {
			switch method {
			case "RequestVote":
				rrf.RequestVote(args.(*raft.RequestVoteArgs), reply.(*raft.RequestVoteReply))
			case "AppendEntries":
				rrf.AppendEntries(args.(*raft.AppendEntriesArgs), reply.(*raft.AppendEntriesReply))
			case "InstallSnapshot":
				rrf.InstallSnapshot(args.(*raft.InstallSnapshotArgs), reply.(*raft.InstallSnapshotReply))
			}
		})
		kvh := kv
		net.AddServer(1000+i, func(method string, args, reply interface{}) {
			switch method {
			case "Get":
				kvh.Get(args.(*GetArgs), reply.(*GetReply))
			case "PutAppend":
				kvh.PutAppend(args.(*PutAppendArgs), reply.(*PutAppendReply))
			}
		})
		_ = ii
	}
	return ck
}

func (ck *kvConfig) makeClerk() *Clerk {
	return MakeClerk(append([]*raft.ClientEnd{}, ck.kvEnds...))
}

func (ck *kvConfig) leader() int {
	for iters := 0; iters < 15; iters++ {
		time.Sleep(100 * time.Millisecond)
		for i := 0; i < ck.n; i++ {
			if ck.connected[i] {
				_, isL := ck.kvs[i].rf.GetState()
				if isL {
					return i
				}
			}
		}
	}
	return -1
}

func (ck *kvConfig) disconnect(i int) {
	ck.connected[i] = false
	ck.net.Enable(i, false)
	ck.net.Enable(1000+i, false)
}

func (ck *kvConfig) connect(i int) {
	ck.connected[i] = true
	ck.net.Enable(i, true)
	ck.net.Enable(1000+i, true)
}

func (ck *kvConfig) kill(i int) {
	ck.kvs[i].Kill()
	ck.kvs[i].rf.Kill()
	ck.connected[i] = false
	ck.net.Enable(i, false)
	ck.net.Enable(1000+i, false)
}

func (ck *kvConfig) restart(i int) {
	ck.kvs[i].Kill()
	ck.kvs[i].rf.Kill()
	time.Sleep(60 * time.Millisecond)
	rf := raft.Make(ck.rafts[i], i, ck.persisters[i], ck.applyCh[i])
	kv := MakeKVServer(i, rf, ck.applyCh[i], 0)
	ck.kvs[i] = kv

	rrf := rf
	ck.net.AddServer(i, func(method string, args, reply interface{}) {
		switch method {
		case "RequestVote":
			rrf.RequestVote(args.(*raft.RequestVoteArgs), reply.(*raft.RequestVoteReply))
		case "AppendEntries":
			rrf.AppendEntries(args.(*raft.AppendEntriesArgs), reply.(*raft.AppendEntriesReply))
		case "InstallSnapshot":
			rrf.InstallSnapshot(args.(*raft.InstallSnapshotArgs), reply.(*raft.InstallSnapshotReply))
		}
	})
	kvh := kv
	ck.net.AddServer(1000+i, func(method string, args, reply interface{}) {
		switch method {
		case "Get":
			kvh.Get(args.(*GetArgs), reply.(*GetReply))
		case "PutAppend":
			kvh.PutAppend(args.(*PutAppendArgs), reply.(*PutAppendReply))
		}
	})
	ck.connect(i)
}

func (ck *kvConfig) cleanup() {
	for i := 0; i < ck.n; i++ {
		if ck.kvs[i] != nil {
			ck.kvs[i].Kill()
			ck.kvs[i].rf.Kill()
		}
	}
	ck.net.Cleanup()
}

// ============================== 测试 ==============================

// 基础：Put 覆盖、Get 读取、Append 拼接。
func TestKVBasic(t *testing.T) {
	ck := makeKVConfig(t, 3)
	defer ck.cleanup()

	ck.clerks[0].Put("k1", "v1")
	if v := ck.clerks[0].Get("k1"); v != "v1" {
		t.Fatalf("after Put got %q want v1", v)
	}
	ck.clerks[0].Put("k1", "v2")
	if v := ck.clerks[0].Get("k1"); v != "v2" {
		t.Fatalf("after Put overwrite got %q want v2", v)
	}
	ck.clerks[0].Append("k1", "x")
	if v := ck.clerks[0].Get("k1"); v != "v2x" {
		t.Fatalf("after Append got %q want v2x", v)
	}
}

// 并发：每个 clerk 给各自 key 追加 N 次，最终长度正好 N。
func TestKVConcurrency(t *testing.T) {
	ck := makeKVConfig(t, 3)
	defer ck.cleanup()

	nClerks := 3
	perClerk := 10
	var wg sync.WaitGroup
	for c := 0; c < nClerks; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			cl := ck.makeClerk()
			key := fmt.Sprintf("k%d", c)
			for i := 0; i < perClerk; i++ {
				cl.Append(key, "x")
			}
		}(c)
	}
	wg.Wait()

	for c := 0; c < nClerks; c++ {
		key := fmt.Sprintf("k%d", c)
		v := ck.clerks[c].Get(key)
		if len(v) != perClerk {
			t.Fatalf("key %s len=%d want %d", key, len(v), perClerk)
		}
	}
}

// 故障转移：杀掉 leader 后，客户端重试仍能读写。
func TestKVFail(t *testing.T) {
	ck := makeKVConfig(t, 3)
	defer ck.cleanup()

	ck.clerks[0].Put("k", "v")
	if v := ck.clerks[0].Get("k"); v != "v" {
		t.Fatalf("initial Get got %q want v", v)
	}
	l := ck.leader()
	ck.kill(l)
	ck.clerks[1].Put("k", "v2")
	if v := ck.clerks[1].Get("k"); v != "v2" {
		t.Fatalf("after failover got %q want v2", v)
	}
}

// 持久化：全部掉电重启后，旧日志重放，状态机被重建。
func TestKVPersist(t *testing.T) {
	ck := makeKVConfig(t, 3)
	defer ck.cleanup()

	ck.clerks[0].Put("k", "v")
	ck.clerks[0].Append("k", "z")
	for i := 0; i < ck.n; i++ {
		ck.restart(i)
	}
	// 重启后第一次 Get 会以新任期提交，隐式把旧 put/append 一起提交并重放
	if v := ck.clerks[0].Get("k"); v != "vz" {
		t.Fatalf("after restart got %q want vz", v)
	}
}
