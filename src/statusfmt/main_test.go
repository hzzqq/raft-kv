// main_test.go —— statusfmt 渲染逻辑的白盒单测（cluster-free）。
package main

import (
	"strings"
	"testing"
)

func TestFormatClusterStatus(t *testing.T) {
	st := clusterStatus{
		Healthy:      true,
		MaxConfigNum: 3,
		Groups: []groupStatus{
			{Group: 0, HasLeader: true, LeaderReplica: 1, ConfigNum: 3, OwnedCount: 5, PendingIn: nil, PendingOut: nil, Incoming: nil, StallSeconds: 0},
			{Group: 1, HasLeader: false, ConfigNum: 3, OwnedCount: 5, PendingIn: []int{2}, PendingOut: []int{}, Incoming: nil, StallSeconds: 5.3},
		},
	}
	out := formatClusterStatus(st)

	if !strings.Contains(out, "HEALTHY") {
		t.Fatalf("expected HEALTHY marker:\n%s", out)
	}
	if !strings.Contains(out, "latest_config=3") {
		t.Fatalf("expected latest_config=3:\n%s", out)
	}
	// nil 切片应渲染为 [] 而非 <nil>，保证输出可读、稳定。
	if strings.Contains(out, "<nil>") {
		t.Fatalf("nil slice should render as [], got <nil>:\n%s", out)
	}
	if !strings.Contains(out, "pendingIn=[]") {
		t.Fatalf("nil pendingIn should render as []:\n%s", out)
	}
	// 无 leader 的 group 应显示 leader=none。
	if !strings.Contains(out, "group 1 leader=none") {
		t.Fatalf("group 1 should show leader=none:\n%s", out)
	}
	// stall 超过阈值应标注。
	if !strings.Contains(out, "STALL 5.3s") {
		t.Fatalf("expected STALL annotation:\n%s", out)
	}
}

func TestFormatClusterStatusStalled(t *testing.T) {
	st := clusterStatus{Healthy: false, MaxConfigNum: 1, Groups: []groupStatus{}}
	out := formatClusterStatus(st)
	if !strings.Contains(out, "STALLED") {
		t.Fatalf("expected STALLED marker:\n%s", out)
	}
}
