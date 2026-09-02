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
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"raftkv/src/cluster"
)

func main() {
	cfgPath := flag.String("config", "", "部署配置 JSON（cluster.ClusterTCPConfig）")
	name := flag.String("name", "", "本节点名：m<j> 或 g<g>-<r>")
	httpAddr := flag.String("http", "", "诊断 HTTP 监听地址（如 :9100）；留空则不起 HTTP")
	flag.Parse()
	if *cfgPath == "" || *name == "" {
		fmt.Fprintln(os.Stderr, "用法: kvnode -config deploy.json -name m0|g0-0 [-http :9100]")
		os.Exit(2)
	}
	cfg, node, err := startNode(*cfgPath, *name)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start node:", err)
		os.Exit(1)
	}
	fmt.Printf("kvnode %s serving (groups=%d replicas=%d sm=%d dataDir=%q)\n",
		*name, cfg.NGroups, cfg.NReplicas, cfg.NSM, cfg.DataDir)

	// 诊断 HTTP：跨机部署下让每个节点进程自曝健康状态（逐台 curl 即可巡检）。
	var srv *http.Server
	if *httpAddr != "" {
		srv = &http.Server{Addr: *httpAddr, Handler: diagHandler(node)}
		go func() {
			fmt.Printf("kvnode %s diagnostics on %s (/healthz /status)\n", *name, *httpAddr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Fprintln(os.Stderr, "diagnostics http:", err)
			}
		}()
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
	fmt.Printf("kvnode %s stopping...\n", *name)
	if srv != nil {
		_ = srv.Close()
	}
	node.Stop()
}

// diagHandler 暴露节点自身的诊断端点：
//
//	GET /healthz  存活探针（进程在即 200，便于 k8s/负载均衡探活）
//	GET /status   本节点健康快照 JSON（Raft 状态 + 分片持有/迁移 + diagnostics 判定）
//	GET /metrics  同一份快照的 Prometheus 文本格式（I152，供 Prometheus 逐节点 scrape）
func diagHandler(node *cluster.TCPNode) http.Handler {
	mux := http.NewServeMux()
	// Prometheus 指标：跨进程部署下 gateway 读不到远端节点的共识状态，
	// 「leader 切换次数 / 当前 leader / apply 落后」只能由节点自曝。详见 metrics.go。
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		d := node.Diagnostics()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := writeNodeMetrics(w, d); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		d := node.Diagnostics()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(d)
	})
	// 便于人工巡检：GET / 给出一行式摘要（无需解析 JSON）
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		d := node.Diagnostics()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		raftDesc := "n/a"
		if d.Raft != nil {
			raftDesc = fmt.Sprintf("role=%v term=%d commit=%d applied=%d lease=%v",
				d.Raft.Role, d.Raft.Term, d.Raft.CommitIndex, d.Raft.LastApplied,
				d.Raft.HasLeaderLease)
		}
		shardDesc := "n/a"
		if d.Shard != nil {
			shardDesc = fmt.Sprintf("gid=%d leader=%v owned=%d pendingIn=%v pendingOut=%v stall=%.1fs",
				d.Shard.GID, d.Shard.Leader, len(d.Shard.Owned),
				d.Shard.PendingIn, d.Shard.PendingOut, d.Shard.StallSeconds)
		}
		fmt.Fprintf(w, "name=%s kind=%s config=%d\nraft : %s\nshard: %s\n",
			d.Name, d.Kind, d.ConfigNum, raftDesc, shardDesc)
	})
	// 运维只读端点（I192）：返回本节点所属 raft 组当前已提交的投票成员集合，
	// 供人工巡检 / 自动化断言观测「witness 当前是否在投票集合内」。
	mux.HandleFunc("GET /admin/reconfigure", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"voters": node.VoterConfig(),
		})
	})
	// 运维端点（I192 操作性闭环）：对本节点所属 raft 组热提议一次成员变更。
	// 仅组 leader 接受；非 leader 返回 409，调用方应重定向到 leader 重试。
	// 成功后等待 ConfChange 日志条目提交并切换 rf.cfg（applier 生效），再回包。
	mux.HandleFunc("POST /admin/reconfigure", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Voters []int `json:"voters"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Voters) == 0 {
			http.Error(w, `bad body: expect {"voters":[...]}`, http.StatusBadRequest)
			return
		}
		idx, ok := node.ProposeConfChange(body.Voters)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if !ok {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":    false,
				"error": "not leader or pending conf change; retry on group leader",
			})
			return
		}
		// 等待配置实际提交生效（applier 原子切换 rf.cfg）。
		target := append([]int(nil), body.Voters...)
		sort.Ints(target)
		deadline := time.Now().Add(5 * time.Second)
		applied := false
		for time.Now().Before(deadline) {
			got := node.VoterConfig()
			g := append([]int(nil), got...)
			sort.Ints(g)
			if len(g) == len(target) {
				match := true
				for i := range target {
					if g[i] != target[i] {
						match = false
						break
					}
				}
				if match {
					applied = true
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !applied {
			w.WriteHeader(http.StatusRequestTimeout)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":     false,
				"error":  "config not applied within timeout",
				"index":  idx,
				"voters": node.VoterConfig(),
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":     true,
			"index":  idx,
			"voters": node.VoterConfig(),
		})
	})
	return mux
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
