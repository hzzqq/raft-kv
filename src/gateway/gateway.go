// gateway.go —— 基于进程内 ShardKV 集群的 HTTP REST 网关
//
// 把 cluster 包启动的内存集群暴露成 REST 接口（GET/PUT/POST-append /kv/{key}），
// 供上层 kvcli 或任意 HTTP 客户端访问。Handler() 返回 http.Handler，便于用
// httptest 做单测而无需绑定端口。
//
// 说明：网关自带一个进程内集群（labrpc 网络），因此是一个"自包含演示网关"；
// 生产环境应由网关连接一组独立部署、走真实网络传输的 ShardKV 节点（而非本文件
// 里的内存集群）。
package main

import (
	"encoding/json"
	"io"
	"net/http"

	"raftkv/src/cluster"
	"raftkv/src/shardkv"
)

// Server 持有集群与绑定到它的 ShardKV 客户端。
type Server struct {
	c     *cluster.Cluster
	clerk *shardkv.Clerk
}

// NewServer 用给定集群构造网关（不立即加入 group，需先 Init）。
func NewServer(c *cluster.Cluster) *Server {
	return &Server{c: c, clerk: c.Clerk()}
}

// Init 依次加入 nGroups 个 replica group，使分片可写。
func (s *Server) Init(nGroups int) {
	for g := 0; g < nGroups; g++ {
		s.c.Join(g)
		s.c.WaitConfig(g, 0, g+1)
	}
}

// Handler 返回 HTTP 路由（Go 1.22 的 method+path 模式）。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("PUT /kv/{key}", s.handlePut)
	mux.HandleFunc("POST /kv/{key}/append", s.handleAppend)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// 可观测性：把进程内 ShardKV 的 Metrics 注册表 JSON 序列化暴露出来。
	// 指标由 cycle 11 的 metrics 包在热路径上以纯原子操作累计，零行为影响。
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	return mux
}

// handleMetrics 返回 shardkv.Metrics 的 JSON 快照（counters + histograms 分位数）。
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snap := shardkv.Metrics.Snapshot()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(snap); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	v := s.clerk.Get(key)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, v)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	val, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	s.clerk.Put(key, string(val))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleAppend(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	val, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	s.clerk.Append(key, string(val))
	w.WriteHeader(http.StatusOK)
}
