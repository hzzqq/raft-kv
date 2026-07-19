// kvraft_test.go —— Lab 3 测试：基于 Raft 的 KV 存储
package kvraft

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"raftkv/src/raft"
)

type kvConfig struct {
	mu           sync.Mutex
	net          *raft.Network
	kvs          []*KVServer
	clerks       []*Clerk
	rafts        [][]*raft.ClientEnd
	kvEnds       []*raft.ClientEnd
	persisters   []*raft.Persister
	applyCh      []chan raft.ApplyMsg
	connected    []bool
	n            int
	t            *testing.T
	maxraftstate int
}

func makeKVConfig(t *testing.T, n int, maxraftstate ...int) *kvConfig {
	mrs := 0
	if len(maxraftstate) > 0 {
		mrs = maxraftstate[0]
	}
	net := raft.MakeNetwork()
	ck := &kvConfig{net: net, n: n, t: t, maxraftstate: mrs}
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
		kv := MakeKVServer(i, rf, ck.applyCh[i], mrs)
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
	// 关键：每次重启使用全新的 applyCh。被 Kill 的旧 KV/Raft applier 仍阻塞在
	// 旧 channel 上（不会关闭），若复用同一 channel，新 applier 会与之竞争
	// ApplyMsg，导致 notify 信号被已死 applier 吞掉、客户端永久挂起。
	ck.applyCh[i] = make(chan raft.ApplyMsg, 4000)
	rf := raft.Make(ck.rafts[i], i, ck.persisters[i], ck.applyCh[i])
	kv := MakeKVServer(i, rf, ck.applyCh[i], ck.maxraftstate)
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

// 快照压缩：超过 maxraftstate 阈值后状态机主动快照，raft 状态大小保持有界；
// 重启后从快照恢复，数据不丢且仍能继续写入（Lab 2D ↔ KV 集成）。
func TestKVSnapshot(t *testing.T) {
	const mrs = 1000
	ck := makeKVConfig(t, 3, mrs)
	defer ck.cleanup()

	ck.clerks[0].Put("k", "")
	n := 200
	for i := 0; i < n; i++ {
		ck.clerks[0].Append("k", "abcdefghij") // 每条 10 字节，使日志远超阈值
	}
	want := strings.Repeat("abcdefghij", n)
	if v := ck.clerks[0].Get("k"); v != want {
		t.Fatalf("after appends len=%d want %d", len(v), len(want))
	}
	// 快照后 raft 状态大小应有界，远小于不快照时的无限增长
	for i := 0; i < ck.n; i++ {
		sz := ck.kvs[i].rf.RaftStateSize()
		if sz > mrs*4 {
			t.Fatalf("server %d raft state size %d > bound %d (snapshot missing?)", i, sz, mrs*4)
		}
	}
	// 全部重启后从快照恢复，数据不丢
	for i := 0; i < ck.n; i++ {
		ck.restart(i)
	}
	if v := ck.clerks[0].Get("k"); v != want {
		t.Fatalf("after restart len=%d want %d", len(v), len(want))
	}
	// 重启后仍能继续写入（快照恢复的状态机可继续演进）
	ck.clerks[0].Append("k", "END")
	if v := ck.clerks[0].Get("k"); v != want+"END" {
		t.Fatalf("after restart+append len=%d want %d", len(v), len(want)+3)
	}
}

// 快照压力：并发写入的同时周期性断开/重连/重启节点（抖动），
// 验证快照压缩在动态拓扑下不丢数据且 raft 状态保持有界（Lab 2D ↔ KV 集成）。
func TestKVSnapshotStress(t *testing.T) {
	const mrs = 1000
	ck := makeKVConfig(t, 3, mrs)
	defer ck.cleanup()

	nClerks := 3
	perClerk := 20
	unit := "xy" // 每条 2 字节

	var wg sync.WaitGroup
	for c := 0; c < nClerks; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			cl := ck.makeClerk()
			key := fmt.Sprintf("k%d", c)
			for i := 0; i < perClerk; i++ {
				cl.Append(key, unit)
			}
		}(c)
	}

	// 抖动：周期性断开/重连或重启一个节点（始终保留多数派）。
	// 强度刻意保持温和——server 为单 RPC 串行处理，过于激进的抖动会
	// 让 waitApplied 阻塞的 handler 占住 server，导致 channel 积压、整体变慢。
	stop := make(chan struct{})
	jitterDone := make(chan struct{})
	go func() {
		defer close(jitterDone)
		i := 0
		for {
			select {
			case <-stop:
				return
			case <-time.After(250 * time.Millisecond):
				idx := i % 3
				if i%3 == 0 {
					ck.restart(idx)
				} else {
					ck.disconnect(idx)
					time.Sleep(60 * time.Millisecond)
					ck.connect(idx)
				}
				i++
			}
		}
	}()

	wg.Wait()      // 等所有写入完成
	close(stop)    // 停止抖动
	<-jitterDone   // 等抖动 goroutine 退出
	time.Sleep(300 * time.Millisecond)

	want := strings.Repeat(unit, perClerk)
	for c := 0; c < nClerks; c++ {
		key := fmt.Sprintf("k%d", c)
		v := ck.clerks[c].Get(key)
		if len(v) != len(want) {
			t.Fatalf("key %s len=%d want %d (snapshot+churn lost data?)", key, len(v), len(want))
		}
	}
	for i := 0; i < ck.n; i++ {
		sz := ck.kvs[i].rf.RaftStateSize()
		if sz > mrs*6 {
			t.Fatalf("server %d raft state size %d > bound %d", i, sz, mrs*6)
		}
	}
}
