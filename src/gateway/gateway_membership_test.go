package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"raftkv/src/cluster"
	"raftkv/src/shardmaster"
)

// ---- fake shardmaster 客户端：cluster-free 下验证「底层客户端调用被正确触发」 ----

type fakeSMClient struct {
	mu     sync.Mutex
	joins  []map[int][]string
	leaves [][]int
	moves  [][2]int // [shard, gid]
	// base 是语义校验护栏 Query(-1) 时看到的初始集群组成（nil = 空集群）；
	// 当前配置按已派发的 Join/Leave 动态推导，模拟真实 shardmaster。
	base map[int][]string
}

// Query 动态推导当前配置：base ∪ joins − leaves，供网关语义校验护栏消费。
func (f *fakeSMClient) Query(num int) shardmaster.Config {
	f.mu.Lock()
	defer f.mu.Unlock()
	groups := make(map[int][]string, len(f.base))
	for g, s := range f.base {
		groups[g] = append([]string(nil), s...)
	}
	for _, j := range f.joins {
		for g, s := range j {
			groups[g] = append([]string(nil), s...)
		}
	}
	for _, ls := range f.leaves {
		for _, g := range ls {
			delete(groups, g)
		}
	}
	return shardmaster.Config{Groups: groups}
}

func (f *fakeSMClient) Join(servers map[int][]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// 拷贝，避免调用方后续修改影响断言
	cp := make(map[int][]string, len(servers))
	for g, s := range servers {
		cp[g] = append([]string(nil), s...)
	}
	f.joins = append(f.joins, cp)
}

func (f *fakeSMClient) Leave(gids []int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leaves = append(f.leaves, append([]int(nil), gids...))
}

func (f *fakeSMClient) Move(shard, gid int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.moves = append(f.moves, [2]int{shard, gid})
}

func (f *fakeSMClient) snapshot() (joins []map[int][]string, leaves [][]int, moves [][2]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.joins, f.leaves, f.moves
}

// postJSON 给测试用的 JSON POST 辅助。
func postJSON(t *testing.T, ts *httptest.Server, path string, body interface{}) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+path, &buf)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func decodeMembership(t *testing.T, resp *http.Response) membershipResp {
	t.Helper()
	defer resp.Body.Close()
	var out membershipResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode resp (status=%d): %v", resp.StatusCode, err)
	}
	return out
}

// TestGatewayMembership_FakeClient 在 cluster-free 下用 fake 客户端验证：
// 三个端点正确解析请求、把调用派发到底层 shardmaster 客户端、并对非法参数返回 4xx。
func TestGatewayMembership_FakeClient(t *testing.T) {
	s := NewServer(nil)
	// base 模拟 Init 后已存在的组 1、2：语义校验护栏据此判定 Leave/Move 的 gid 有效性。
	fake := &fakeSMClient{base: map[int][]string{1: {"g1s1"}, 2: {"g2s1"}}}
	s.sm = fake // 注入 fake，绕过真实集群
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// --- Join 正常 ---
	resp := postJSON(t, ts, "/join", joinReq{GID: 10, Servers: []string{"s1", "s2", "s3"}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("join ok status = %d, want 200", resp.StatusCode)
	}
	out := decodeMembership(t, resp)
	if !out.OK || out.GID != 10 {
		t.Fatalf("join resp = %+v, want ok=true gid=10", out)
	}
	joins, _, _ := fake.snapshot()
	if len(joins) != 1 || len(joins[0][10]) != 3 {
		t.Fatalf("fake.Join not triggered correctly: %+v", joins)
	}

	// --- Join 缺 gid（body 为空对象）-> 400 ---
	resp = postJSON(t, ts, "/join", joinReq{})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("join missing gid status = %d, want 400", resp.StatusCode)
	}
	decodeMembership(t, resp) // 解不出错即可

	// --- Join gid<=0 -> 400 ---
	resp = postJSON(t, ts, "/join", joinReq{GID: -1, Servers: []string{"x"}})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("join gid<=0 status = %d, want 400", resp.StatusCode)
	}

	// --- Join servers 空 -> 400 ---
	resp = postJSON(t, ts, "/join", joinReq{GID: 11, Servers: nil})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("join empty servers status = %d, want 400", resp.StatusCode)
	}

	// --- Join 非法 JSON -> 400 ---
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/join", bytes.NewBufferString("{not json"))
	req.Header.Set("Content-Type", "application/json")
	badResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if badResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("join bad json status = %d, want 400", badResp.StatusCode)
	}
	badResp.Body.Close()

	// --- Leave 正常（批量）：移除初始已存在的组 1、2 ---
	resp = postJSON(t, ts, "/leave", leaveReq{GIDs: []int{1, 2}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("leave ok status = %d, want 200", resp.StatusCode)
	}
	out = decodeMembership(t, resp)
	if !out.OK || len(out.GIDs) != 2 {
		t.Fatalf("leave resp = %+v, want ok=true gids=[1,2]", out)
	}
	_, leaves, _ := fake.snapshot()
	if len(leaves) != 1 || len(leaves[0]) != 2 {
		t.Fatalf("fake.Leave not triggered correctly: %+v", leaves)
	}

	// --- Leave 单值 gid 兼容：先 Join 30 再 Leave 30（保持语义有效）---
	resp = postJSON(t, ts, "/join", joinReq{GID: 30, Servers: []string{"g30s1"}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("join gid 30 status = %d, want 200", resp.StatusCode)
	}
	resp = postJSON(t, ts, "/leave", leaveReq{GID: 30})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("leave single gid status = %d, want 200", resp.StatusCode)
	}

	// --- Leave 两者皆空 -> 400 ---
	resp = postJSON(t, ts, "/leave", leaveReq{})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("leave empty status = %d, want 400", resp.StatusCode)
	}

	// --- Move 正常 ---
	resp = postJSON(t, ts, "/move", moveReq{Shard: 0, GID: 10})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("move ok status = %d, want 200", resp.StatusCode)
	}
	out = decodeMembership(t, resp)
	if !out.OK || out.Shard != 0 || out.GID != 10 {
		t.Fatalf("move resp = %+v, want ok=true shard=0 gid=10", out)
	}
	_, _, moves := fake.snapshot()
	if len(moves) != 1 || moves[0] != [2]int{0, 10} {
		t.Fatalf("fake.Move not triggered correctly: %+v", moves)
	}

	// --- Move shard 越界 -> 400 ---
	resp = postJSON(t, ts, "/move", moveReq{Shard: 999, GID: 10})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("move shard out of range status = %d, want 400", resp.StatusCode)
	}

	// --- Move gid<=0 -> 400 ---
	resp = postJSON(t, ts, "/move", moveReq{Shard: 1, GID: 0})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("move gid<=0 status = %d, want 400", resp.StatusCode)
	}

	// --- 非 POST 方法：网关路由为 POST 专有，GET 命中兜底 /{path...} -> 404 ---
	// （与既有 POST /debug/migrate-plan 等行为一致：method+path 模式不匹配即 NotFound）
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/join", nil)
	g, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if g.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /join status = %d, want 404", g.StatusCode)
	}
	g.Body.Close()
}

// TestGatewayMembership_NoCluster 验证未挂载集群时端点返回 503（不 panic）。
func TestGatewayMembership_NoCluster(t *testing.T) {
	s := NewServer(nil) // sm 为 nil
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	resp := postJSON(t, ts, "/join", joinReq{GID: 1, Servers: []string{"x"}})
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("no-cluster join status = %d, want 503", resp.StatusCode)
	}
	decodeMembership(t, resp)
}

// TestGatewayMembership_RealCluster 用真实内存集群做端到端验证：
// 三个端点确实推进了 shardmaster 的配置（Join 新增组、Move 改分片归属、Leave 移除组）。
func TestGatewayMembership_RealCluster(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	cfgs := c.Configs()
	if len(cfgs) == 0 {
		t.Fatal("no configs after Init")
	}
	base := cfgs[len(cfgs)-1]
	if _, ok := base.Groups[10]; ok {
		t.Fatal("gid 10 should not exist before join")
	}

	// Join gid=10（servers 用占位地址，shardmaster 只存配置，不要求可达）
	resp := postJSON(t, ts, "/join", joinReq{GID: 10, Servers: []string{"g10-r0", "g10-r1", "g10-r2"}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("join status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	cfgs = c.Configs()
	afterJoin := cfgs[len(cfgs)-1]
	if servers, ok := afterJoin.Groups[10]; !ok || len(servers) != 3 {
		t.Fatalf("after join: group 10 not present with 3 servers: %+v", afterJoin.Groups)
	}

	// Move shard 0 -> gid 10
	resp = postJSON(t, ts, "/move", moveReq{Shard: 0, GID: 10})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("move status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	cfgs = c.Configs()
	afterMove := cfgs[len(cfgs)-1]
	if afterMove.Shards[0] != 10 {
		t.Fatalf("after move: shard 0 should belong to gid 10, got %d", afterMove.Shards[0])
	}

	// Leave gid 10
	resp = postJSON(t, ts, "/leave", leaveReq{GIDs: []int{10}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("leave status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	cfgs = c.Configs()
	afterLeave := cfgs[len(cfgs)-1]
	if _, ok := afterLeave.Groups[10]; ok {
		t.Fatalf("after leave: group 10 should be gone, groups=%+v", afterLeave.Groups)
	}
	// 离开后分片 0 应被重新分配回已有组（不属于 10）
	if afterLeave.Shards[0] == 10 {
		t.Fatalf("after leave: shard 0 still points to removed gid 10")
	}
}

// TestGatewayMembership_SemanticInvalidGuard 在 cluster-free 下验证语义校验护栏：
// shardmaster.Clerk 的 Join/Leave/Move 对 ErrInvalid（服务端确定性拒绝）无限重试，
// 语义无效请求若被派发将永久挂死 handler 并占用并发信号量槽位。护栏要求这类请求
// 在派发前被快速 400，且不触达底层客户端。
func TestGatewayMembership_SemanticInvalidGuard(t *testing.T) {
	s := NewServer(nil)
	fake := &fakeSMClient{base: map[int][]string{1: {"g1s1", "g1s2"}, 2: {"g2s1", "g2s2"}}}
	s.sm = fake
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// --- Join 已存在的 gid 1 -> 400，且不触达底层 Join ---
	resp := postJSON(t, ts, "/join", joinReq{GID: 1, Servers: []string{"dup"}})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("join existing gid status = %d, want 400", resp.StatusCode)
	}
	out := decodeMembership(t, resp)
	if out.OK || out.Error == "" {
		t.Fatalf("join existing gid resp = %+v, want ok=false with error", out)
	}
	if joins, _, _ := fake.snapshot(); len(joins) != 0 {
		t.Fatalf("guard must not dispatch Join for existing gid, got %d dispatches", len(joins))
	}

	// --- Leave 不存在的 gid 777 -> 400，且不触达底层 Leave ---
	resp = postJSON(t, ts, "/leave", leaveReq{GIDs: []int{777}})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("leave nonexistent gid status = %d, want 400", resp.StatusCode)
	}
	if _, leaves, _ := fake.snapshot(); len(leaves) != 0 {
		t.Fatalf("guard must not dispatch Leave for nonexistent gid, got %d dispatches", len(leaves))
	}

	// --- Leave 重复 gid -> 400（validateLeave 确定性拒绝重复项）---
	resp = postJSON(t, ts, "/leave", leaveReq{GIDs: []int{1, 1}})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("leave duplicate gids status = %d, want 400", resp.StatusCode)
	}

	// --- Move 目标 gid 99 不在当前配置 -> 400，且不触达底层 Move ---
	resp = postJSON(t, ts, "/move", moveReq{Shard: 0, GID: 99})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("move to nonexistent gid status = %d, want 400", resp.StatusCode)
	}
	if _, _, moves := fake.snapshot(); len(moves) != 0 {
		t.Fatalf("guard must not dispatch Move to nonexistent gid, got %d dispatches", len(moves))
	}

	// --- 语义有效请求不受护栏影响 ---
	resp = postJSON(t, ts, "/join", joinReq{GID: 5, Servers: []string{"new"}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("join new gid status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
	resp = postJSON(t, ts, "/leave", leaveReq{GIDs: []int{2}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("leave existing gid status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
	resp = postJSON(t, ts, "/move", moveReq{Shard: 0, GID: 1})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("move to existing gid status = %d, want 200", resp.StatusCode)
	}
	decodeMembership(t, resp)
}

// TestGatewayMembership_RealCluster_InvalidFastReject 用真实集群验证语义无效请求
// 快速返回 400（修复前：Clerk 对 ErrInvalid 无限重试，请求永久挂起）。客户端带
// 10s 超时——若护栏回归为挂起，本测试以超时失败而非卡死整轮 go test。
func TestGatewayMembership_RealCluster_InvalidFastReject(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	s := NewServer(c)
	s.Init(2)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	fast := &http.Client{Timeout: 10 * time.Second}
	post := func(path string, body interface{}) *http.Response {
		t.Helper()
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
		req, err := http.NewRequest(http.MethodPost, ts.URL+path, &buf)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		resp, err := fast.Do(req)
		if err != nil {
			t.Fatalf("POST %s failed (likely hung instead of fast-rejecting): %v", path, err)
		}
		return resp
	}

	// re-join 已存在的 gid 1（Init 已加入 gid 1、2）-> 400
	resp := post("/join", joinReq{GID: 1, Servers: []string{"dup-r0"}})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("re-join existing gid status = %d, want 400", resp.StatusCode)
	}
	out := decodeMembership(t, resp)
	if out.OK || out.Error == "" {
		t.Fatalf("re-join resp = %+v, want ok=false with error", out)
	}

	// Leave 不存在的 gid -> 400
	resp = post("/leave", leaveReq{GIDs: []int{777}})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("leave nonexistent gid status = %d, want 400", resp.StatusCode)
	}

	// Move 目标 gid 不在当前配置 -> 400
	resp = post("/move", moveReq{Shard: 0, GID: 99})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("move to nonexistent gid status = %d, want 400", resp.StatusCode)
	}
	decodeMembership(t, resp)
}
