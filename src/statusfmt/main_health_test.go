package main

import "testing"

// 构造 group 状态。
func grp(g int, hasLeader bool, stall float64, pi, po, inc []int) groupStatus {
	return groupStatus{
		Group: g, HasLeader: hasLeader, StallSeconds: stall,
		PendingIn: pi, PendingOut: po, Incoming: inc,
	}
}

// TestClusterHealthScorePerfect 验证：全 leader、无 stall、无积压 → 100 分。
func TestClusterHealthScorePerfect(t *testing.T) {
	st := clusterStatus{Groups: []groupStatus{
		grp(1, true, 0, nil, nil, nil),
		grp(2, true, 0, nil, nil, nil),
	}}
	score, _ := clusterHealthScore(st)
	if score != 100 {
		t.Fatalf("期望 100，实际 %v", score)
	}
}

// TestClusterHealthScoreLeaderRatio 验证：一半 group 无主 → 50 分（其余维度满分）。
func TestClusterHealthScoreLeaderRatio(t *testing.T) {
	st := clusterStatus{Groups: []groupStatus{
		grp(1, true, 0, nil, nil, nil),
		grp(2, false, 0, nil, nil, nil),
	}}
	score, _ := clusterHealthScore(st)
	if score != 50 {
		t.Fatalf("期望 50（一半无主），实际 %v", score)
	}
}

// TestClusterHealthScoreStall 验证：平均 stall=50s → 扣 50（全 leader 时 100-50=50）。
func TestClusterHealthScoreStall(t *testing.T) {
	st := clusterStatus{Groups: []groupStatus{
		grp(1, true, 50, nil, nil, nil),
	}}
	score, _ := clusterHealthScore(st)
	if score != 50 {
		t.Fatalf("期望 50（stall 扣 50），实际 %v", score)
	}
}

// TestClusterHealthScoreBacklog 验证：积压 20 条（len-10 + len-10）→ 扣 10（全 leader 无 stall 时 100-10=90）。
func TestClusterHealthScoreBacklog(t *testing.T) {
	st := clusterStatus{Groups: []groupStatus{
		grp(1, true, 0, make([]int, 10), make([]int, 10), nil),
	}}
	score, _ := clusterHealthScore(st)
	if score != 90 {
		t.Fatalf("期望 90（backlog 扣 10），实际 %v", score)
	}
}

// TestClusterHealthScoreNoGroups 验证：无 group → 0 分，摘要 "no groups"。
func TestClusterHealthScoreNoGroups(t *testing.T) {
	st := clusterStatus{}
	score, summary := clusterHealthScore(st)
	if score != 0 || summary != "no groups" {
		t.Fatalf("期望 0 分 + 'no groups'，实际 %v / %q", score, summary)
	}
}
