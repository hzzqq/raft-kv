// kvadmin —— 跨机部署的 ShardMaster 运维 CLI（I188）。
//
// 这是部署路径一直缺失的「管理面入口」：kvnode/gateway 只做数据面与控制面，
// 但分片在组间迁移（rebalance）、组上线/下线（Join/Leave）、配置版本查询
// 这些 ShardMaster 配置变更，此前在「真实部署」里没有任何外部触发手段——
// 只能靠 in-process 的 experiments 测试桩（cluster 包）驱动，部署件本身从没被验证过
// 能跑多组。本工具直接复用 cluster 包里的 make_end（transport.Dial + gob）与
// shardmaster.MakeClerk，对运行中的 ShardMaster 发真实配置变更 RPC。
//
// 用法：
//
//	kvadmin -config deploy.json query                 # 打印最新配置（Num + 分片→组分布）
//	kvadmin -config deploy.json move <shard> <gid>     # 把某分片迁到目标组
//	kvadmin -config deploy.json churn <rounds> <step>  # 反复把分片在组间漂移，制造迁移流
//	kvadmin -config deploy.json join <gid>             # 把某组重新加入配置（配合 leave 做配置抖动）
//	kvadmin -config deploy.json leave <gid>            # 把某组移出配置
//
// 与 experiments/migration.go 的 churn 同语义，但作用在真实 TCP 部署集群上。
package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"flag"
	"fmt"
	"os"
	"time"

	"raftkv/src/cluster"
	"raftkv/src/raft"
	"raftkv/src/shardmaster"
	"raftkv/src/transport"
)

const tcpRPCTimeout = 2 * time.Second

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

// makeEnd 复刻 cluster 包 cluster_tcp.go 的 newTransportEnd：把 ShardMaster 节点名
// 解析成真实 TCP 客户端端点，RPC 走 gob 编解码，方法名拼 "/raft/<Method>"。
func makeEnd(addr string) *raft.ClientEnd {
	cc := transport.Dial(addr)
	return raft.MakeSendFnEnd(func(method string, args, reply interface{}) bool {
		reqData, err := gobEncode(args)
		if err != nil {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), tcpRPCTimeout)
		defer cancel()
		respData, err := cc.Invoke(ctx, "/raft/"+method, reqData)
		if err != nil {
			return false
		}
		if err := gobDecode(respData, reply); err != nil {
			return false
		}
		return true
	})
}

func main() {
	cfgPath := flag.String("config", "", "部署配置 JSON（cluster.ClusterTCPConfig）")
	flag.Parse()
	if *cfgPath == "" {
		fmt.Fprintln(os.Stderr, "用法: kvadmin -config deploy.json <query|move|churn|join|leave> [args]")
		os.Exit(2)
	}
	cfg, err := cluster.LoadTCPConfig(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load config:", err)
		os.Exit(1)
	}

	// 取 ShardMaster 节点地址（m0..m{nSM-1}）。
	smNames := make([]string, 0, cfg.NSM)
	smAddr := map[string]string{}
	for j := 0; j < cfg.NSM; j++ {
		name := fmt.Sprintf("m%d", j)
		smNames = append(smNames, name)
		for _, n := range cfg.Nodes {
			if n.Name == name {
				smAddr[name] = n.Addr
			}
		}
	}
	make_end := func(name string) *raft.ClientEnd {
		addr, ok := smAddr[name]
		if !ok {
			fmt.Fprintf(os.Stderr, "kvadmin: 找不到 ShardMaster 节点 %q 的地址\n", name)
			os.Exit(1)
		}
		return makeEnd(addr)
	}
	ck := shardmaster.MakeClerk(smNames, make_end)

	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "子命令: query | move <shard> <gid> | churn <rounds> <step> | join <gid> | leave <gid>")
		os.Exit(2)
	}

	switch args[0] {
	case "query":
		c := ck.Query(-1)
		dist := map[int]int{}
		for _, g := range c.Shards {
			dist[g]++
		}
		fmt.Printf("config Num=%d  nShards=%d\n", c.Num, len(c.Shards))
		fmt.Printf("分片分布(组→分片数): ")
		first := true
		// 注意：部署路径 group id 从 1 起（1..ng），不能用 len(dist) 作循环上界——
		// len(dist) 是「不同 gid 的个数」，会漏掉 gid >= len(dist) 的组。遍历到
		// NShards 上界即可覆盖所有可能的 gid 值（不持有的组 dist 中无键，自动跳过）。
		for g := 0; g <= len(c.Shards); g++ {
			if n, ok := dist[g]; ok {
				if !first {
					fmt.Printf(", ")
				}
				fmt.Printf("gid%d=%d", g, n)
				first = false
			}
		}
		fmt.Println()
		fmt.Printf("详情: %v\n", c.Shards)

	case "move":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "move 需要 <shard> <gid>")
			os.Exit(2)
		}
		var shard, gid int
		fmt.Sscanf(args[1], "%d", &shard)
		fmt.Sscanf(args[2], "%d", &gid)
		ck.Move(shard, gid)
		fmt.Printf("✓ 已提交 Move(shard=%d → gid=%d)\n", shard, gid)

	case "churn":
		rounds, step := 25, 1
		if len(args) >= 2 {
			fmt.Sscanf(args[1], "%d", &rounds)
		}
		if len(args) >= 3 {
			fmt.Sscanf(args[2], "%d", &step)
		}
		ng := cfg.NGroups
		for i := 0; i < rounds; i++ {
			shard := (i * step) % shardmaster.NShards
			// 部署路径 ShardMaster group id 从 1 起（gateway.Init 把组索引映射到 gid+1），
			// 故 churn 在 1..ng 之间漂移，而非 0..ng-1。
			gid := 1 + i%ng
			ck.Move(shard, gid)
			time.Sleep(150 * time.Millisecond)
		}
		fmt.Printf("✓ churn 完成：%d 轮，分片在 %d 组间漂移\n", rounds, ng)

	case "join":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "join 需要 <gid>")
			os.Exit(2)
		}
		var gid int
		fmt.Sscanf(args[1], "%d", &gid)
		// 部署路径 ShardMaster group id 从 1 起，节点名用 0 基组索引：g<gid-1>-<r>。
		gidx := gid - 1
		servers := map[int][]string{}
		for r := 0; r < cfg.NReplicas; r++ {
			name := fmt.Sprintf("g%d-%d", gidx, r)
			for _, n := range cfg.Nodes {
				if n.Name == name {
					servers[gid] = append(servers[gid], n.Addr)
				}
			}
		}
		if len(servers[gid]) != cfg.NReplicas {
			fmt.Fprintf(os.Stderr, "join gid%d: 仅找到 %d/%d 个副本地址（节点 g%d-*）\n",
				gid, len(servers[gid]), cfg.NReplicas, gidx)
			os.Exit(1)
		}
		ck.Join(servers)
		fmt.Printf("✓ 已提交 Join(gid=%d, %d 副本)\n", gid, cfg.NReplicas)

	case "leave":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "leave 需要 <gid>")
			os.Exit(2)
		}
		var gid int
		fmt.Sscanf(args[1], "%d", &gid)
		ck.Leave([]int{gid})
		fmt.Printf("✓ 已提交 Leave(gid=%d)\n", gid)

	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n", args[0])
		os.Exit(2)
	}
}
