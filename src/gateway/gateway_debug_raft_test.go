package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"raftkv/src/cluster"
	"raftkv/src/raft"
)

// TestGatewayDebugRaft 验证网关 /debug/raft 端点：汇总每个 group/副本的 Raft 只读快照
// 与 diagnostics.RaftCheck 自检结果。用真实集群底座（与 TestGatewayHTTP 一致），断言
// 200 + 合法 JSON 数组，且每个视图含有效的角色/任期与 0–100 不变量评分（I220 延伸：
// 把 cycle #220/#221 的 raft 健康能力汇聚到网关，运维无需登录各节点即可看共识健康）。
func TestGatewayDebugRaft(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)

	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/debug/raft")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/raft = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var views []struct {
		Group     int             `json:"group"`
		Replica   int             `json:"replica"`
		Raft      raft.RaftStatus `json:"raft"`
		Diagnosis struct {
			Score  int      `json:"score"`
			Issues []string `json:"issues"`
		} `json:"diagnosis"`
	}
	if err := json.Unmarshal(body, &views); err != nil {
		t.Fatalf("GET /debug/raft body is not valid JSON: %v (body=%s)", err, string(body))
	}
	if len(views) == 0 {
		t.Fatalf("GET /debug/raft returned empty array")
	}
	// 2 group * 3 副本 = 6 个视图。
	if len(views) != 6 {
		t.Fatalf("want 6 raft views (2 groups x 3 replicas), got %d", len(views))
	}
	for _, v := range views {
		if v.Raft.Role != raft.Leader && v.Raft.Role != raft.Follower && v.Raft.Role != raft.Candidate {
			t.Fatalf("view group=%d replica=%d has invalid Role %v", v.Group, v.Replica, v.Raft.Role)
		}
		if v.Diagnosis.Score < 0 || v.Diagnosis.Score > 100 {
			t.Fatalf("view group=%d replica=%d score %d out of [0,100]", v.Group, v.Replica, v.Diagnosis.Score)
		}
		if len(v.Diagnosis.Issues) == 0 {
			t.Fatalf("view group=%d replica=%d diagnosis must carry at least one issue (or 'ok')", v.Group, v.Replica)
		}
	}
}
