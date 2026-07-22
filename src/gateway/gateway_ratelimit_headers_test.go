package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// TestGatewayRateLimitHeaders 验证限流开启时下发 X-RateLimit-* 头且 remaining<=limit。
// 全程 cluster-free（直接构造 Server 字面量，不经 raft 集群）。
func TestGatewayRateLimitHeaders(t *testing.T) {
	s := &Server{
		clientRate:     200,
		clientBurst:    40,
		clientLimiters: make(map[string]*tokenBucket),
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/kv/x", nil)
	s.setRateLimitHeaders(rec, req)
	limit := rec.Header().Get("X-RateLimit-Limit")
	rem := rec.Header().Get("X-RateLimit-Remaining")
	reset := rec.Header().Get("X-RateLimit-Reset")
	if limit != "40" {
		t.Fatalf("X-RateLimit-Limit = %q, want 40", limit)
	}
	if rem == "" {
		t.Fatalf("X-RateLimit-Remaining missing")
	}
	if reset == "" {
		t.Fatalf("X-RateLimit-Reset missing")
	}
	rl, _ := strconv.Atoi(rem)
	ll, _ := strconv.Atoi(limit)
	if rl > ll {
		t.Fatalf("remaining %d > limit %d", rl, ll)
	}
}

// TestGatewayRateLimitHeadersDisabled 验证限流关闭时不下发这些头（零行为影响）。
func TestGatewayRateLimitHeadersDisabled(t *testing.T) {
	s := &Server{clientRate: 0, clientLimiters: make(map[string]*tokenBucket)}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/kv/x", nil)
	s.setRateLimitHeaders(rec, req)
	if rec.Header().Get("X-RateLimit-Limit") != "" {
		t.Fatalf("headers should be absent when rate limiting disabled")
	}
}

// TestGatewayShutdownIdempotent 验证无在途、无监听时 Shutdown 幂等返回 nil。
func TestGatewayShutdownIdempotent(t *testing.T) {
	s := &Server{}
	ctx := context.Background()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("first shutdown should succeed: %v", err)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("second shutdown should be idempotent: %v", err)
	}
}

// TestGatewayShutdownClosesGRPC 验证 Shutdown 会关闭已登记的 gRPC 监听器（I86 补齐）。
func TestGatewayShutdownClosesGRPC(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{grpcLis: lis}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown should succeed: %v", err)
	}
	if _, err := lis.Accept(); err == nil {
		t.Fatalf("gRPC listener should be closed by Shutdown")
	}
}
