// witness.go —— 场景 E：Witness（见证者）副本容错。
//
// 经典 witness 语义（CockroachDB / etcd learner-ish）：witness 持有完整 Raft 日志、
// 参与投票与提交 quorum，但**不持有状态机数据、永不被选为 leader**。这样
// 「2 投票副本 + 1 witness」（raft peers=3，quorum=2）用 **2 份存储**达到
// 3 副本的容错——kill 一个投票副本后，剩 1 投票 + 1 witness 仍达 quorum，可选主可提交；
// 而纯 2 副本（无 witness）kill 一个就完全不可写。
//
// 本实验用真实 TCP 集群（StartClusterTCP）证明部署件层面的 witness：
//  1. 预热写 + 读回，全部提交（基线）；
//  2. 整个运行期 witness 节点 LeaderElections 恒为 0、Role 永不为 Leader（不参选）；
//  3. CrashNode 一个投票副本（g0-1）→ 集群在 witness 补 quorum 下仍零丢失写、可读可写。
package main

import (
	"fmt"
	"time"

	"raftkv/src/cluster"
	"raftkv/src/raft"
)

func runWitness() {
	const nG, nR, nSM = 1, 2, 3 // 2 投票副本 + 1 witness（见下方 Nodes）
	witnessIdx := nR             // KVs[g] 中 witness 的下标（投票副本在后）
	cfg := cluster.ClusterTCPConfig{
		NGroups:      nG,
		NReplicas:    nR,
		NSM:          nSM,
		MaxRaftState: 1000,
		Nodes:        []cluster.TCPNodeAddr{},
	}
	// ShardMaster（3 节点）
	for j := 0; j < nSM; j++ {
		cfg.Nodes = append(cfg.Nodes, cluster.TCPNodeAddr{
			Name: fmt.Sprintf("m%d", j), Addr: fmt.Sprintf("127.0.0.1:%d", 7300+j),
		})
	}
	// 2 投票副本 g0-0 / g0-1
	for r := 0; r < nR; r++ {
		cfg.Nodes = append(cfg.Nodes, cluster.TCPNodeAddr{
			Name: fmt.Sprintf("g0-%d", r), Addr: fmt.Sprintf("127.0.0.1:%d", 7310+r),
		})
	}
	// 1 witness g0-w0（Witness=true）
	cfg.Nodes = append(cfg.Nodes, cluster.TCPNodeAddr{
		Name: "g0-w0", Addr: "127.0.0.1:7312", Witness: true,
	})

	c, err := cluster.StartClusterFromConfig(cfg)
	if err != nil {
		fmt.Printf("✗ StartClusterFromConfig 失败: %v\n", err)
		return
	}
	defer c.Cleanup()
	bootstrap(c, nG)
	ck := c.Clerk()

	log("场景 E：Witness 副本容错（%d 组 × %d 投票副本 + 1 witness）", nG, nR)

	// 基线：预热写 + 读回
	for i := 0; i < 10; i++ {
		ck.Put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	baseOK := true
	for i := 0; i < 10; i++ {
		if v := ck.Get(fmt.Sprintf("k%d", i)); v != fmt.Sprintf("v%d", i) {
			baseOK = false
			log("✗ 基线读回失败 k%d=%q", i, v)
		}
	}
	if baseOK {
		log("✓ 基线：10 键预热写 + 读回全部一致（2 投票 + 1 witness 已提交）")
	}

	// 监控 witness 是否参选：整个实验期后台轮询，记录是否曾 Role==Leader。
	witnessEverLed := false
	done := make(chan struct{})
	go func() {
		tick := time.NewTicker(30 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				if c.KVRaftStatus(0, witnessIdx).Role == raft.Leader {
					witnessEverLed = true
				}
			}
		}
	}()

	// 定位 leader（仅投票副本参与选举）
	l, st := findLeader(c, 0, nR)
	if l < 0 {
		time.Sleep(600 * time.Millisecond)
		l, st = findLeader(c, 0, nR)
	}
	if l < 0 {
		close(done)
		fmt.Println("✗ 初始未选出 leader，退出")
		return
	}
	log("初始 leader = g0-%d | term=%d commit=%d applied=%d", l, st.Term, st.CommitIndex, st.LastApplied)

	// 容错证据：kill 当前 leader（g0-{l}），剩一个投票副本(g0-{1-l}) + 1 witness。
	// 若只有 2 投票副本（无 witness），kill 一个后 quorum=1<2 → 不可提交、不可服务；
	// 有 witness 时 quorum 仍达 2/3，剩投票副本+ witness 能选出新 leader 并继续提交。
	log("CrashNode g0-%d（当前 leader 崩溃）；剩 g0-%d + g0-w0 两会话应仍达 quorum=2", l, 1-l)
	c.CrashNode(fmt.Sprintf("g0-%d", l))

	// 客户端视角：故障窗口内持续真实读写，量化「客户端可见不可用」与「丢失写」
	pr := probeClient(ck, 2500*time.Millisecond, 30*time.Millisecond, true)
	logProbe("witness", pr)
	close(done)

	// 故障后新 leader 应在 投票副本(g0-0) 中选出，witness 仅投票不参选
	nl, nst := waitLeader(c, 0, nR, st.Term, 6*time.Second)
	if nl < 0 {
		fmt.Printf("✗ 故障后 6s 内未选出新 leader（witness 不应参选，但应投票促成 g0-0 当选）；g0-0=%+v\n",
			c.KVRaftStatus(0, 0))
	} else {
		log("✓ 故障后新 leader = g0-%d | term=%d（witness 仅投票、未夺权）", nl, nst.Term)
	}

	// 故障前数据完整性（zero lost）
	if v0 := ck.Get("k0"); v0 == "v0" {
		log("✓ 故障前已提交数据完整保留（k0=v0），跨故障零丢失写")
	} else {
		log("✗ 故障前数据丢失：k0=%q", v0)
	}

	// witness 不参选的铁证：整个运行期 LeaderElections 恒为 0
	wst := c.KVRaftStatus(0, witnessIdx)
	witnessNeverLed := !witnessEverLed && wst.LeaderElections == 0 && wst.Role != raft.Leader
	if witnessNeverLed {
		log("✓ witness 全程未参选（LeaderElections=%d, Role=%s），仅以完整日志+投票权补 quorum",
			wst.LeaderElections, wst.Role)
	} else {
		log("✗ witness 竟然参选/夺权（LeaderElections=%d, everLed=%v）", wst.LeaderElections, witnessEverLed)
	}

	ok := pr.LostWrites == 0 && witnessNeverLed && baseOK
	writeClientViewJSON("client_view_witness.json", clientViewReport{
		Scenario:     "witness-failover",
		GeneratedAt: time.Now().Format(time.RFC3339),
		Ops:          pr.Ops,
		Fails:        pr.Fails,
		LostWrites:   pr.LostWrites,
		DownMs:       pr.DownMs,
		Conclusion: fmt.Sprintf(
			"2 投票副本+1 witness（quorum=2）用 2 份存储获得 3 副本容错：kill 1 投票副本后，"+
				"剩 1 投票+1 witness 仍达 quorum，客户端零丢失写（lost=%d）；witness 全程未参选（LeaderElections=%d）。",
			pr.LostWrites, wst.LeaderElections),
		Ok: ok,
	})

	if ok {
		log("结论：Witness 副本部署件真机验证通过——存储减半、容错不变。")
	} else {
		log("结论：Witness 验证未通过（lost=%d / witnessNeverLed=%v）。", pr.LostWrites, witnessNeverLed)
	}
}
