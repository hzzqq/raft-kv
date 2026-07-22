package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseRouteLimits(t *testing.T) {
	cfg, err := ParseGatewayConfig([]byte(`
listen_addr: :8080
route_limits: [POST /kv/*=5/10, GET /status=20/5]
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.RouteLimits) != 2 {
		t.Fatalf("got %d route limits, want 2", len(cfg.RouteLimits))
	}
	rl := cfg.RouteLimits
	if rl[0].Route != "POST /kv/*" || rl[0].RPS != 5 || rl[0].Burst != 10 {
		t.Fatalf("entry0 wrong: %+v", rl[0])
	}
	if rl[1].Route != "GET /status" || rl[1].RPS != 20 || rl[1].Burst != 5 {
		t.Fatalf("entry1 wrong: %+v", rl[1])
	}
}

func TestValidateRouteLimits(t *testing.T) {
	cfg, _ := ParseGatewayConfig([]byte(`route_limits: [GET /x=-5/1]`))
	probs := cfg.Validate()
	found := false
	for _, p := range probs {
		if containsStrLocal(p, "route_limits") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected route_limits validation problem, got %v", probs)
	}
}

func TestApplyRouteLimitEffect(t *testing.T) {
	resetCounter("gateway_ratelimit_route_total")
	cfg, _ := ParseGatewayConfig([]byte(`route_limits: [GET /kv/*=1/1]`))
	s := NewServer(nil)
	cfg.Apply(s)

	h := s.Wrap(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	// 同客户端连续 3 次 GET /kv/x：route 令牌桶 burst=1 → 后两次 429
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest("GET", "/kv/x", nil)
		req.Header.Set("X-Client-ID", "c-r")
		h(recorder(), req)
	}
	if got := Metrics.Counter("gateway_ratelimit_route_total").Value(); got <= 0 {
		t.Fatalf("route ratelimit counter = %d, want > 0", got)
	}
}

// containsStrLocal 是测试内小写子串判定（避免与项目其他 helper 命名冲突）。
func containsStrLocal(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
