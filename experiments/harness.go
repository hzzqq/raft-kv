// harness.go —— 三个可展示实验共用的小工具。
//
// 这些实验用 src/cluster 的内存 labrpc 集群框架真实跑 Raft/ShardKV，注入故障
// （kill 副本 / 网络分区）并断言恢复，打印一条带时间戳的时间线。目的是把
// 「系统确实容错」从测试通过变成可演示、可放进作品集的证据——而非给项目再加
// 一套健康分校验脚本（那属于被冻结的工具链）。
package main

import (
	"fmt"
	"time"

	"raftkv/src/cluster"
	"raftkv/src/raft"
)

var start time.Time

func resetClock() { start = time.Now() }

// log 打印一行带相对时间戳的实验进展。
func log(format string, a ...interface{}) {
	dt := time.Since(start).Seconds()
	fmt.Printf("[t=+%.2fs] "+format+"\n", append([]interface{}{dt}, a...)...)
}

// findLeader 返回第 g 组当前 Role==Leader 的副本下标与状态；无则 (-1, zero)。
func findLeader(c *cluster.Cluster, g, nR int) (int, raft.RaftStatus) {
	for r := 0; r < nR; r++ {
		st := c.KVRaftStatus(g, r)
		if st.Role == raft.Leader {
			return r, st
		}
	}
	return -1, raft.RaftStatus{}
}

// bootstrap 把各组加入 ShardMaster 并等配置生效（等价于 gateway.Server.Init）。
// 内存集群 StartCluster 不会自动 Join，不 Join 则 ShardMaster 没有分片→组的映射，
// Clerk 的读写会无的放矢。
func bootstrap(c *cluster.Cluster, nGroups int) {
	for g := 0; g < nGroups; g++ {
		c.Join(g)
		c.WaitConfig(g, 0, g+1)
	}
}

// waitLeaderIn 只在给定副本子集里等新 leader（term>minTerm）。分区场景必须限定范围：
// 被隔离的旧 leader 收不到更高任期的心跳，本地仍自称 leader（这是分区语义，不是 bug）。
func waitLeaderIn(c *cluster.Cluster, g int, cand []int, minTerm int, timeout time.Duration) (int, raft.RaftStatus) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, r := range cand {
			st := c.KVRaftStatus(g, r)
			if st.Role == raft.Leader && st.Term > minTerm {
				return r, st
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	return -1, raft.RaftStatus{}
}

// waitCaughtUp 轮询直到第 g 组第 r 个副本的 LastApplied 追上 target。
func waitCaughtUp(c *cluster.Cluster, g, r, target int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.KVRaftStatus(g, r).LastApplied >= target {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// waitLeader 轮询直到出现 Role==Leader 且 term>minTerm 的副本（用于探测新 leader）。
func waitLeader(c *cluster.Cluster, g, nR, minTerm int, timeout time.Duration) (int, raft.RaftStatus) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for r := 0; r < nR; r++ {
			st := c.KVRaftStatus(g, r)
			if st.Role == raft.Leader && st.Term > minTerm {
				return r, st
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	return -1, raft.RaftStatus{}
}
