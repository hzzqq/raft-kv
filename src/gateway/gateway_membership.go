package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"raftkv/src/shardmaster"
)

// smClient 是 shardmaster 配置变更客户端接口。*shardmaster.Clerk 天然满足该接口
// （Join/Leave/Move 签名与 shardmaster.Clerk 完全一致），抽象出来便于单测注入
// fake 客户端、在 cluster-free 场景下量化验证「底层客户端调用被正确触发」，无需
// 启动真实内存集群。
type smClient interface {
	// Join 把一组副本（gid -> servers）加入集群配置。
	Join(servers map[int][]string)
	// Leave 把若干副本组移出集群配置。
	Leave(gids []int)
	// Move 把某个分片迁往指定 gid 的副本组。
	Move(shard, gid int)
}

// joinReq 是 POST /join 的请求体：把 gid 标识的副本组加入集群，
// servers 为该组各副本的接入地址（与 shardmaster.Join 的 map[int][]string 对齐）。
// gid 采用 shardmaster 的「组标识」语义（正整数，区别于 cluster 内部的组下标）。
type joinReq struct {
	GID     int      `json:"gid"`
	Servers []string `json:"servers"`
}

// leaveReq 是 POST /leave 的请求体：移除若干副本组。兼容单值 gid 写法
// （仅提供 gid 时按 [gid] 处理），也支持批量 gids 数组。
type leaveReq struct {
	GIDs []int `json:"gids"`
	GID  int   `json:"gid"`
}

// moveReq 是 POST /move 的请求体：把 shard 编号的分片迁往 gid 标识的副本组。
// 对应 shardmaster.Move（单次分片重分配），也是控制台「重新平衡」单步动作的底层触发。
type moveReq struct {
	Shard int `json:"shard"`
	GID   int `json:"gid"`
}

// membershipResp 是 Join/Leave/Move 三个端点共用的响应体（成功/错误同构，便于
// 控制台统一解析）。OK=false 时 error 给出人读原因。
type membershipResp struct {
	OK     bool   `json:"ok"`
	GID    int    `json:"gid,omitempty"`
	Shard  int    `json:"shard,omitempty"`
	GIDs   []int  `json:"gids,omitempty"`
	Error  string `json:"error,omitempty"`
}

// smClientOrErr 返回当前生效的 shardmaster 客户端；未挂载集群（cluster-free 的
// Server）时返回错误，由调用方映射成 503，避免在 nil 客户端上 panic。
func (s *Server) smClientOrErr() (smClient, error) {
	if s.sm == nil {
		return nil, fmt.Errorf("shardmaster client unavailable (gateway has no cluster)")
	}
	return s.sm, nil
}

// writeJSON 以 JSON 写出响应（统一 Content-Type），供三个端点的一致成功/错误输出。
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

// handleJoin 实现 POST /join：把一个新副本组加入集群（对应 shardmaster.Join）。
// 成功返回 200 + {"ok":true,"gid":N}；参数非法（gid<=0 或 servers 为空或 JSON 错误）
// 返回 400；未挂载集群返回 503。调用经 s.sm.Join 真正写入 shardmaster 配置。
func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ck, err := s.smClientOrErr()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, membershipResp{OK: false, Error: err.Error()})
		return
	}
	var req joinReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, membershipResp{OK: false, Error: "bad json: " + err.Error()})
		return
	}
	if req.GID <= 0 {
		writeJSON(w, http.StatusBadRequest, membershipResp{OK: false, Error: "gid must be a positive integer"})
		return
	}
	if len(req.Servers) == 0 {
		writeJSON(w, http.StatusBadRequest, membershipResp{OK: false, Error: "servers must not be empty"})
		return
	}
	ck.Join(map[int][]string{req.GID: req.Servers})
	writeJSON(w, http.StatusOK, membershipResp{OK: true, GID: req.GID})
}

// handleLeave 实现 POST /leave：把若干副本组移出集群（对应 shardmaster.Leave）。
// 接受 gids 数组或单个 gid；二者皆空返回 400。成功返回 200 + {"ok":true,"gids":[...]}。
func (s *Server) handleLeave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ck, err := s.smClientOrErr()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, membershipResp{OK: false, Error: err.Error()})
		return
	}
	var req leaveReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, membershipResp{OK: false, Error: "bad json: " + err.Error()})
		return
	}
	gids := req.GIDs
	if len(gids) == 0 && req.GID > 0 {
		gids = []int{req.GID}
	}
	if len(gids) == 0 {
		writeJSON(w, http.StatusBadRequest, membershipResp{OK: false, Error: "gids (or gid) must be provided"})
		return
	}
	for _, g := range gids {
		if g <= 0 {
			writeJSON(w, http.StatusBadRequest, membershipResp{OK: false, Error: fmt.Sprintf("gid %d must be a positive integer", g)})
			return
		}
	}
	ck.Leave(gids)
	writeJSON(w, http.StatusOK, membershipResp{OK: true, GIDs: gids})
}

// handleMove 实现 POST /move：把某个分片迁往指定副本组（对应 shardmaster.Move），
// 即控制台「重新平衡」的单步触发。shard 必须在 [0, NShards) 且 gid>0，否则 400。
func (s *Server) handleMove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ck, err := s.smClientOrErr()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, membershipResp{OK: false, Error: err.Error()})
		return
	}
	var req moveReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, membershipResp{OK: false, Error: "bad json: " + err.Error()})
		return
	}
	if req.Shard < 0 || req.Shard >= shardmaster.NShards {
		writeJSON(w, http.StatusBadRequest, membershipResp{
			OK:    false,
			Error: fmt.Sprintf("shard %d out of range [0, %d)", req.Shard, shardmaster.NShards),
		})
		return
	}
	if req.GID <= 0 {
		writeJSON(w, http.StatusBadRequest, membershipResp{OK: false, Error: "gid must be a positive integer"})
		return
	}
	ck.Move(req.Shard, req.GID)
	writeJSON(w, http.StatusOK, membershipResp{OK: true, Shard: req.Shard, GID: req.GID})
}
