package main

import (
	"encoding/json"
	"net/http"
	"raftkv/src/shardmaster"
)

// migratePlanReq 是 /debug/migrate-plan 的请求体：当前配置 + 拟议变更。
// admin 把线上当前配置（可来自 /debug/configs 的 latest）与一次拟议的
// Join/Leave/Move 一并提交，即可在「不触碰 Raft」的前提下预览迁移结果。
type migratePlanReq struct {
	Current shardmaster.Config `json:"current"`
	Op      shardmaster.PlanOp `json:"op"`
}

// migratePlanResp 是 /debug/migrate-plan 的响应体（与 shardmaster.PlanResult 对齐）。
type migratePlanResp struct {
	Planned       *shardmaster.Config   `json:"planned"`
	Errors        []string              `json:"errors"`
	TransitionErr string                `json:"transition_err"`
	Moves         []shardmaster.ShardMove `json:"moves"`
}

// handleDebugMigratePlan 提供「配置变更 dry-run」预览（不触碰 Raft）：给定当前配置
// 与一次拟议的 Join/Leave/Move，返回应用 + 再平衡后的目标配置、结构合法性错误、
// 演进合法性错误与分片迁移步骤。供运维在真实提交前安全评估迁移代价与风险，
// 避免直接提交非法配置导致 rebalance 卡死（与 #202 的纯函数 Plan 同源）。
func (s *Server) handleDebugMigratePlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req migratePlanReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	res := shardmaster.Plan(&req.Current, req.Op)
	out := migratePlanResp{
		Planned:       res.Planned,
		Errors:        res.Errors,
		TransitionErr: res.TransitionErr,
		Moves:         res.Moves,
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
