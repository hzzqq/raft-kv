package shardkv

import "testing"

// TestClerkOrderServers 验证副本排序辅助：preferred 为空时原样返回；非空时前置且
// 不重复、其余保持相对顺序。这是 Clerk leader 缓存（先试缓存 leader）的底层不变量。
func TestClerkOrderServers(t *testing.T) {
	servers := []string{"s1", "s2", "s3"}
	if got := orderServers(servers, ""); len(got) != 3 || got[0] != "s1" {
		t.Fatalf("empty preferred should return servers unchanged, got %v", got)
	}
	got := orderServers(servers, "s3")
	want := []string{"s3", "s1", "s2"}
	if len(got) != 3 {
		t.Fatalf("expected 3 servers, got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("orderServers(s3) = %v, want %v", got, want)
		}
	}
	// preferred 不在 servers 中时视为无缓存，原样返回
	if got := orderServers(servers, "ghost"); len(got) != 3 || got[0] != "s1" {
		t.Fatalf("unknown preferred should be ignored, got %v", got)
	}
}

// TestClerkLeaderCache 验证成功读写后 Clerk 的 per-gid leader 缓存被填充：
// 后续请求优先直连该副本，减少迁移/leader 切换期的盲目广播。
func TestClerkLeaderCache(t *testing.T) {
	cfg := makeSKVConfig(t, 1, 3, 100, -1)
	defer cfg.cleanup()
	ck := cfg.makeClerk()
	ck.Put("x", "hello")
	if v := ck.Get("x"); v != "hello" {
		t.Fatalf("Get(x)=%q want hello", v)
	}
	if len(ck.leaderOf) == 0 {
		t.Fatalf("leader cache (ck.leaderOf) not populated after successful Put/Get")
	}
}
