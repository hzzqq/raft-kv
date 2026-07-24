package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"raftkv/src/shardmaster"
)

func migratePlanServer() *Server {
	return &Server{
		sem:            make(chan struct{}, maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
		startedAt:      time.Now(),
	}
}

func validCurrentConfig() shardmaster.Config {
	c := shardmaster.Config{
		Num:    1,
		Groups: map[int][]string{1: {"s1", "s2"}},
	}
	for i := 0; i < shardmaster.NShards; i++ {
		c.Shards[i] = 1
	}
	return c
}

// TestGatewayMigratePlanJoin 验证：当前单组全量持有，Join 新组后应触发再平衡，
// 返回 planned 配置含两个组、moves 非空、无 errors / transition_err。
func TestGatewayMigratePlanJoin(t *testing.T) {
	s := migratePlanServer()
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	body, _ := json.Marshal(migratePlanReq{
		Current: validCurrentConfig(),
		Op:      shardmaster.PlanOp{Join: map[int][]string{2: {"s3", "s4"}}},
	})
	resp, err := http.Post(ts.URL+"/debug/migrate-plan", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out migratePlanResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Planned == nil {
		t.Fatalf("planned config is nil")
	}
	if len(out.Planned.Groups) != 2 {
		t.Fatalf("planned groups = %d, want 2", len(out.Planned.Groups))
	}
	if out.Planned.Num != 2 {
		t.Fatalf("planned num = %d, want 2", out.Planned.Num)
	}
	if len(out.Moves) == 0 {
		t.Fatalf("expected non-empty moves after rebalance")
	}
	if len(out.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", out.Errors)
	}
	if out.TransitionErr != "" {
		t.Fatalf("unexpected transition_err: %s", out.TransitionErr)
	}
}

// TestGatewayMigratePlanInvalidLeave 验证：Leave 掉唯一组会导致 rebalance 后空组，
// 触发结构合法性错误（errors 非空），dry-run 正确拦截非法配置。
func TestGatewayMigratePlanInvalidLeave(t *testing.T) {
	s := migratePlanServer()
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	body, _ := json.Marshal(migratePlanReq{
		Current: validCurrentConfig(),
		Op:      shardmaster.PlanOp{Leave: []int{1}},
	})
	resp, err := http.Post(ts.URL+"/debug/migrate-plan", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out migratePlanResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Errors) == 0 {
		t.Fatalf("expected structural errors when leaving the only group")
	}
}

// TestGatewayMigratePlanOrphanMove 验证：Move 分片到不存在的 gid 会产生孤儿分片，
// 经 ValidateConfig 检出并写入 errors（dry-run 拦截）。
func TestGatewayMigratePlanOrphanMove(t *testing.T) {
	s := migratePlanServer()
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	body, _ := json.Marshal(migratePlanReq{
		Current: validCurrentConfig(),
		Op:      shardmaster.PlanOp{Move: &shardmaster.PlanMove{Shard: 0, Gid: 99}},
	})
	resp, err := http.Post(ts.URL+"/debug/migrate-plan", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out migratePlanResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Errors) == 0 {
		t.Fatalf("expected orphan-shard error for move to non-existent gid")
	}
}

// TestGatewayMigratePlanMethodNotAllowed 验证：GET 该 POST-only 端点返回 404
// （Go1.22 ServeMux 对未注册方法的路由不匹配，通配兜底亦走 404），
// 即「仅 POST 可用」的语义成立。
func TestGatewayMigratePlanMethodNotAllowed(t *testing.T) {
	s := migratePlanServer()
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/debug/migrate-plan")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}
