// kvnode/main.go —— 跨机部署的**单节点**进程入口（I23）。
//
// 每台机器（或每个进程）跑一个节点：
//
//	go run ./src/kvnode -config deploy.json -name m0      # ShardMaster 副本 0
//	go run ./src/kvnode -config deploy.json -name g0-2    # group0 副本 2
//
// deploy.json 即 cluster.ClusterTCPConfig（n_groups/n_replicas/n_sm/nodes 地址清单，
// data_dir 非空则状态落盘、崩溃后重启同目录即恢复）。全部节点起来后，用
// `gateway -connect deploy.json` 挂一个 HTTP 网关即可对外服务。
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"raftkv/src/cluster"
)

func main() {
	cfgPath := flag.String("config", "", "部署配置 JSON（cluster.ClusterTCPConfig）")
	name := flag.String("name", "", "本节点名：m<j> 或 g<g>-<r>")
	flag.Parse()
	if *cfgPath == "" || *name == "" {
		fmt.Fprintln(os.Stderr, "用法: kvnode -config deploy.json -name m0|g0-0")
		os.Exit(2)
	}
	cfg, node, err := startNode(*cfgPath, *name)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start node:", err)
		os.Exit(1)
	}
	fmt.Printf("kvnode %s serving (groups=%d replicas=%d sm=%d dataDir=%q)\n",
		*name, cfg.NGroups, cfg.NReplicas, cfg.NSM, cfg.DataDir)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
	fmt.Printf("kvnode %s stopping...\n", *name)
	node.Stop()
}

// startNode 加载配置并启动单节点，抽出来便于测试 main 的入口逻辑。
func startNode(cfgPath, name string) (cluster.ClusterTCPConfig, *cluster.TCPNode, error) {
	cfg, err := cluster.LoadTCPConfig(cfgPath)
	if err != nil {
		return cluster.ClusterTCPConfig{}, nil, err
	}
	node, err := cluster.StartNodeTCP(cfg, name)
	if err != nil {
		return cluster.ClusterTCPConfig{}, nil, err
	}
	return cfg, node, nil
}
