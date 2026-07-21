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
	"fmt"
	"io"
	"net/http"
	"strings"

	"raftkv/src/cluster"
	"raftkv/src/shardkv"
	"raftkv/src/shardmaster"
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
	// 诊断：暴露每个 group/副本的「分片归属 + 待接收/待迁出」状态，便于定位
	// 3-group 再平衡卡死等迁移问题（pendingIn/pendingOut 残留会冻结配置推进）。
	mux.HandleFunc("GET /debug/shards", s.handleDebugShards)
	// 集群健康总览（程序化消费）：每 group 是否有 leader、当前 config 号、拥有分片数、
	// 待接收/待迁出/孤儿 incoming 分片，以及是否存在卡滞（StallSeconds>0 即冻结风险）。
	mux.HandleFunc("GET /status", s.handleStatus)
	// 迁移进度（人类可读，供 CLI `start.sh migrate` 直接展示）：每个 group leader 副本的
	// 实时迁移状态 + 集群最新 config 号，一眼看清再平衡是否卡住。
	mux.HandleFunc("GET /debug/migrate", s.handleDebugMigrate)
	// 配置历史（人类/程序可读）：展示 shardmaster 从初始到最新的每段配置，便于复盘
	// rebalance 轨迹、确认分片在哪些 group 间迁移（排查 3-group 冻结时尤其有用）。
	mux.HandleFunc("GET /debug/configs", s.handleDebugConfigs)
	return mux
}

// GroupStatus 是 /status 中单个 replica group 的健康快照（取 leader 副本视角）。
type GroupStatus struct {
	Group         int     `json:"group"`
	HasLeader     bool    `json:"has_leader"`
	LeaderReplica int     `json:"leader_replica"`
	ConfigNum     int     `json:"config_num"`
	OwnedCount    int     `json:"owned_count"`
	PendingIn     []int   `json:"pending_in"`
	PendingOut    []int   `json:"pending_out"`
	Incoming      []int   `json:"incoming"`
	StallSeconds  float64 `json:"stall_seconds"`
}

// ClusterStatus 是 /status 的聚合响应。
type ClusterStatus struct {
	Groups       []GroupStatus `json:"groups"`
	Healthy      bool          `json:"healthy"`       // 所有 group 均无卡滞 = true
	MaxConfigNum int           `json:"max_config_num"` // 集群最新已应用 config 号
}

// stallUnhealthySec 是判定「再平衡冻结」的卡滞阈值（秒）。正常多跳迁移的分片在
// 数百毫秒内完成，pendingIn/pendingOut 的 StallSeconds 仅瞬时正值；只有真正冻结
// （配置推进卡死、分片永久不可达）才会持续 > 阈值。取 2s（低于 pollConfig 看门狗的
// 3s），使 /status 的 healthy 标志能先于看门狗触发即暴露冻结，又不误报正常瞬时迁移。
const stallUnhealthySec = 2.0

// handleStatus 返回集群健康总览（JSON），便于监控/告警系统轮询判断再平衡是否冻结。
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st := ClusterStatus{Groups: []GroupStatus{}}
	healthy := true
	maxCfg := 0
	for g := range s.c.KVs {
		gs := GroupStatus{Group: g}
		for r := range s.c.KVs[g] {
			d := s.c.KVs[g][r].ShardDebug()
			if d.Leader {
				gs.HasLeader = true
				gs.LeaderReplica = r
				gs.ConfigNum = d.ConfigNum
				gs.OwnedCount = len(d.Owned)
				gs.PendingIn = d.PendingIn
				gs.PendingOut = d.PendingOut
				gs.Incoming = d.Incoming
				gs.StallSeconds = d.StallSeconds
				if d.StallSeconds > stallUnhealthySec {
					healthy = false
				}
				break
			}
		}
		if gs.ConfigNum > maxCfg {
			maxCfg = gs.ConfigNum
		}
		st.Groups = append(st.Groups, gs)
	}
	st.Healthy = healthy
	st.MaxConfigNum = maxCfg
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(st); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleDebugMigrate 返回每个 group leader 副本的实时迁移进度（纯文本），供 CLI 直接展示。
func (s *Server) handleDebugMigrate(w http.ResponseWriter, r *http.Request) {
	var b strings.Builder
	maxCfg := 0
	for g := range s.c.KVs {
		for r := range s.c.KVs[g] {
			d := s.c.KVs[g][r].ShardDebug()
			if d.ConfigNum > maxCfg {
				maxCfg = d.ConfigNum
			}
			if !d.Leader {
				continue
			}
			fmt.Fprintf(&b, "group %d (leader r%d, config=%d): owned=%d pendingIn=%v pendingOut=%v incoming=%v stall=%.1fs\n",
				g, r, d.ConfigNum, len(d.Owned), d.PendingIn, d.PendingOut, d.Incoming, d.StallSeconds)
			break
		}
	}
	fmt.Fprintf(&b, "latest config=%d\n", maxCfg)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, b.String())
}

// ConfigView 是 /debug/configs 中单段配置的视图。
type ConfigView struct {
	Num    int                `json:"num"`
	Groups map[int][]string   `json:"groups"`
	Shards [shardmaster.NShards]int `json:"shards"`
}

// handleDebugConfigs 返回 shardmaster 的完整配置历史（JSON），便于复盘 rebalance 轨迹。
func (s *Server) handleDebugConfigs(w http.ResponseWriter, r *http.Request) {
	cfgs := s.c.Configs()
	if len(cfgs) == 0 {
		http.Error(w, "no configs", http.StatusInternalServerError)
		return
	}
	views := make([]ConfigView, 0, len(cfgs))
	for _, cfg := range cfgs {
		views = append(views, ConfigView{Num: cfg.Num, Groups: cfg.Groups, Shards: cfg.Shards})
	}
	out := map[string]interface{}{
		"latest_num": cfgs[len(cfgs)-1].Num,
		"configs":    views,
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
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

// ShardDebugView 把集群中某个 group/副本的 ShardDebug 与其坐标打包，便于 JSON 输出。
type ShardDebugView struct {
	Group   int
	Replica int
	shardkv.ShardDebug
}

// handleDebugShards 返回集群所有 group 所有副本的分片归属与迁移状态（JSON 数组），
// 用于诊断 3-group 再平衡卡死等迁移问题（pendingIn/pendingOut 残留会冻结配置推进）。
func (s *Server) handleDebugShards(w http.ResponseWriter, r *http.Request) {
	var out []ShardDebugView
	for g := range s.c.KVs {
		for r := range s.c.KVs[g] {
			out = append(out, ShardDebugView{
				Group:      g,
				Replica:    r,
				ShardDebug: s.c.KVs[g][r].ShardDebug(),
			})
		}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// statusForErr 把 ShardKV 错误映射成恰当的 HTTP 状态码，使 REST 语义正确：
// 分片不归本组（含迁移中）-> 409 Conflict；非 leader（瞬态）-> 503；
// 超时/不可达 -> 504；其它 -> 500。
func statusForErr(e shardkv.Err) int {
	switch e {
	case shardkv.ErrWrongGroup:
		return http.StatusConflict
	case shardkv.ErrWrongLeader:
		return http.StatusServiceUnavailable
	case shardkv.ErrTimeout:
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	v, err := s.clerk.GetE(key)
	if err != shardkv.OK {
		http.Error(w, string(err), statusForErr(err))
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, v)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	val, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err := s.clerk.PutE(key, string(val)); err != shardkv.OK {
		http.Error(w, string(err), statusForErr(err))
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleAppend(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	val, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err := s.clerk.AppendE(key, string(val)); err != shardkv.OK {
		http.Error(w, string(err), statusForErr(err))
		return
	}
	w.WriteHeader(http.StatusOK)
}
