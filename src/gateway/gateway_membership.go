package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"raftkv/src/shardmaster"
)

// smClient 是 shardmaster 配置变更客户端接口。*shardmaster.Clerk 天然满足该接口
// （TryJoin/TryLeave/TryMove/Query 签名与 shardmaster.Clerk 完全一致），抽象出来便于
// 单测注入 fake 客户端、在 cluster-free 场景下量化验证「底层客户端调用被正确触发」，
// 无需启动真实内存集群。端点统一走 Try* 变体：对服务端确定性拒绝（ErrInvalid）快速
// 返回错误而非无限重试（详见下方护栏说明与 tryOp 协议注释）。
type smClient interface {
	// TryJoin 把一组副本（gid -> servers）加入集群配置；确定性拒绝返回 ErrInvalid。
	TryJoin(servers map[int][]string) shardmaster.Err
	// TryLeave 把若干副本组移出集群配置；确定性拒绝返回 ErrInvalid。
	TryLeave(gids []int) shardmaster.Err
	// TryMove 把某个分片迁往指定 gid 的副本组；确定性拒绝返回 ErrInvalid。
	TryMove(shard, gid int) shardmaster.Err
	// Query 读取集群配置（num<0 取最新），供端点派发前做语义校验。
	Query(num int) shardmaster.Config
}

// 语义校验护栏（cycle 205）+ Try* 派发（cycle 206）双层防护：shardmaster.Clerk 的
// Join/Leave/Move 对任何非 OK 回复——包括 ErrInvalid（服务端 validateJoin/validateLeave/
// validateMove 对「重复 Join gid / Leave 不存在或重复 gid / Move 目标组不在配置」的
// 确定性拒绝）——都会无限重试、永不返回。若把语义无效请求直接派发给 Clerk，HTTP
// handler 将永久阻塞：客户端虽在 requestTimeout 后收到 TimeoutHandler 的 503，但
// handler goroutine 及 wrap 已占用的并发信号量槽位永不释放，反复请求可耗尽并发预算
//（全网关 429 DoS）。因此：
//  1. 第一层（cycle 205）：三端点派发前经 Query(-1) 拉取最新配置做与 validate* 同口径
//     的语义校验，语义无效请求快速 400，根本不派发；
//  2. 第二层（cycle 206）：派发走 TryJoin/TryLeave/TryMove——覆盖「校验通过后、派发前
//     配置被并发变更」的竞态残余边界：Try* 对派发期 ErrInvalid 立即返回而非无限重试，
//     端点以 409 Conflict 快速失败（见 dispatchErrStatus），handler 与并发槽位不再泄漏，
//     客户端重查配置重试即可。

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

// dispatchErrStatus 把 Try* 派发期错误映射为 HTTP 状态码：ErrInvalid 属「语义校验时
// 有效、派发时被并发配置变更击败」的冲突 → 409（客户端重查配置后重试即可成功）；
// 其他错误按 tryOp 契约不应出现（Try* 只返回 OK 或 ErrInvalid），防御性映射 503。
func dispatchErrStatus(e shardmaster.Err) int {
	if e == shardmaster.ErrInvalid {
		return http.StatusConflict
	}
	return http.StatusServiceUnavailable
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
// 或语义无效（gid 已存在于当前配置）返回 400；校验通过但派发时配置被并发变更
//（TryJoin 返回 ErrInvalid）返回 409；未挂载集群返回 503。调用经 s.sm.TryJoin
// 真正写入 shardmaster 配置。
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
	// 语义校验：重复 Join 已存在的 gid 会被 validateJoin 确定性拒绝（ErrInvalid），
	// 先行快速 400，避免派发后 Clerk 无限重试挂死 handler。
	cfg := ck.Query(-1)
	if _, exists := cfg.Groups[req.GID]; exists {
		writeJSON(w, http.StatusBadRequest, membershipResp{
			OK:    false,
			Error: fmt.Sprintf("gid %d already exists in latest config (num %d)", req.GID, cfg.Num),
		})
		return
	}
	// 派发：TryJoin 在服务端确定性拒绝（ErrInvalid，即「校验通过后配置被并发变更」的
	// 竞态窗口）时立即返回错误而非无限重试，以 409 快速失败，不再泄漏 handler 与槽位。
	if err := ck.TryJoin(map[int][]string{req.GID: req.Servers}); err != shardmaster.OK {
		writeJSON(w, dispatchErrStatus(err), membershipResp{
			OK:    false,
			GID:   req.GID,
			Error: fmt.Sprintf("dispatch rejected by shardmaster: %v (config may have changed concurrently; re-query and retry)", err),
		})
		return
	}
	writeJSON(w, http.StatusOK, membershipResp{OK: true, GID: req.GID})
}

// handleLeave 实现 POST /leave：把若干副本组移出集群（对应 shardmaster.Leave）。
// 接受 gids 数组或单个 gid；二者皆空、gid 非法、重复 gid 或 gid 不存在于当前配置
// 返回 400；校验通过但派发时配置被并发变更（TryLeave 返回 ErrInvalid）返回 409。
// 成功返回 200 + {"ok":true,"gids":[...]}。
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
	// 语义校验：重复 gid 或当前配置中不存在的 gid 都会被 validateLeave 确定性拒绝
	//（ErrInvalid），先行快速 400，避免派发后 Clerk 无限重试挂死 handler。
	seen := map[int]bool{}
	for _, g := range gids {
		if seen[g] {
			writeJSON(w, http.StatusBadRequest, membershipResp{
				OK:    false,
				Error: fmt.Sprintf("duplicate gid %d", g),
			})
			return
		}
		seen[g] = true
	}
	cfg := ck.Query(-1)
	for _, g := range gids {
		if _, exists := cfg.Groups[g]; !exists {
			writeJSON(w, http.StatusBadRequest, membershipResp{
				OK:    false,
				Error: fmt.Sprintf("gid %d not present in latest config (num %d)", g, cfg.Num),
			})
			return
		}
	}
	// 派发：TryLeave 在服务端确定性拒绝（ErrInvalid，即「校验通过后配置被并发变更」的
	// 竞态窗口）时立即返回错误而非无限重试，以 409 快速失败，不再泄漏 handler 与槽位。
	if err := ck.TryLeave(gids); err != shardmaster.OK {
		writeJSON(w, dispatchErrStatus(err), membershipResp{
			OK:    false,
			GIDs:  gids,
			Error: fmt.Sprintf("dispatch rejected by shardmaster: %v (config may have changed concurrently; re-query and retry)", err),
		})
		return
	}
	writeJSON(w, http.StatusOK, membershipResp{OK: true, GIDs: gids})
}

// handleMove 实现 POST /move：把某个分片迁往指定副本组（对应 shardmaster.Move），
// 即控制台「重新平衡」的单步触发。shard 必须在 [0, NShards)、gid>0 且目标 gid 存在
// 于当前配置，否则 400；校验通过但派发时配置被并发变更（TryMove 返回 ErrInvalid）
// 返回 409。
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
	// 语义校验：目标 gid 不在当前配置中会被 validateMove 确定性拒绝（ErrInvalid），
	// 先行快速 400，避免派发后 Clerk 无限重试挂死 handler。
	cfg := ck.Query(-1)
	if _, exists := cfg.Groups[req.GID]; !exists {
		writeJSON(w, http.StatusBadRequest, membershipResp{
			OK:    false,
			Error: fmt.Sprintf("gid %d not present in latest config (num %d); join it first", req.GID, cfg.Num),
		})
		return
	}
	// 派发：TryMove 在服务端确定性拒绝（ErrInvalid，即「校验通过后配置被并发变更」的
	// 竞态窗口）时立即返回错误而非无限重试，以 409 快速失败，不再泄漏 handler 与槽位。
	if err := ck.TryMove(req.Shard, req.GID); err != shardmaster.OK {
		writeJSON(w, dispatchErrStatus(err), membershipResp{
			OK:     false,
			Shard:  req.Shard,
			GID:    req.GID,
			Error: fmt.Sprintf("dispatch rejected by shardmaster: %v (config may have changed concurrently; re-query and retry)", err),
		})
		return
	}
	writeJSON(w, http.StatusOK, membershipResp{OK: true, Shard: req.Shard, GID: req.GID})
}
