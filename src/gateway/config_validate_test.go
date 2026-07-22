package main

import (
	"strings"
	"testing"
)

// TestGatewayConfigValidateOK 验证：默认配置通过校验（无问题）。
func TestGatewayConfigValidateOK(t *testing.T) {
	if probs := DefaultGatewayConfig().Validate(); len(probs) != 0 {
		t.Fatalf("默认配置应通过校验，实际问题 %v", probs)
	}
}

// TestGatewayConfigValidateListenBad 验证：非法 listen_addr 被报告。
func TestGatewayConfigValidateListenBad(t *testing.T) {
	c := DefaultGatewayConfig()
	c.ListenAddr = "not-a-hostport"
	if probs := c.Validate(); !has(probs, "not host:port") {
		t.Fatalf("期望报告 listen_addr 格式错误，实际 %v", probs)
	}

	c2 := DefaultGatewayConfig()
	c2.ListenAddr = ":0" // 端口 0 非法
	if probs := c2.Validate(); !has(probs, "out of range") {
		t.Fatalf("期望报告端口越界，实际 %v", probs)
	}

	c3 := DefaultGatewayConfig()
	c3.ListenAddr = ":70000" // 端口超界
	if probs := c3.Validate(); !has(probs, "out of range") {
		t.Fatalf("期望报告端口越界，实际 %v", probs)
	}
}

// TestGatewayConfigValidateEmpty 验证：空 listen_addr 被报告。
func TestGatewayConfigValidateEmpty(t *testing.T) {
	c := DefaultGatewayConfig()
	c.ListenAddr = ""
	if probs := c.Validate(); !has(probs, "listen_addr empty") {
		t.Fatalf("期望报告空 listen_addr，实际 %v", probs)
	}
}

// TestGatewayConfigValidateRanges 验证：非正的超时/并发/体量被报告。
func TestGatewayConfigValidateRanges(t *testing.T) {
	c := DefaultGatewayConfig()
	c.RequestTimeout = 0
	if probs := c.Validate(); !has(probs, "request_timeout_sec must be > 0") {
		t.Fatalf("期望报告超时非正，实际 %v", probs)
	}

	c = DefaultGatewayConfig()
	c.MaxConcurrent = 0
	if probs := c.Validate(); !has(probs, "max_concurrent must be >= 1") {
		t.Fatalf("期望报告并发<1，实际 %v", probs)
	}

	c = DefaultGatewayConfig()
	c.MaxBodySize = 0
	if probs := c.Validate(); !has(probs, "max_body_size must be > 0") {
		t.Fatalf("期望报告体量非正，实际 %v", probs)
	}
}

// TestGatewayConfigValidateRateBurst 验证：限流参数矛盾被报告。
func TestGatewayConfigValidateRateBurst(t *testing.T) {
	c := DefaultGatewayConfig()
	c.ClientRate = 0
	c.ClientBurst = 10
	if probs := c.Validate(); !has(probs, "client_rate <= 0") {
		t.Fatalf("期望报告限流矛盾，实际 %v", probs)
	}
}

// TestGatewayConfigValidateCIDR 验证：非法 CIDR 被报告，合法不报。
func TestGatewayConfigValidateCIDR(t *testing.T) {
	c := DefaultGatewayConfig()
	c.AllowCIDRs = []string{"10.0.0.0/8"} // 合法
	if probs := c.Validate(); has(probs, "invalid CIDR") {
		t.Fatalf("合法 CIDR 不应报错，实际 %v", probs)
	}

	c2 := DefaultGatewayConfig()
	c2.AllowCIDRs = []string{"not-a-cidr"}
	if probs := c2.Validate(); !has(probs, "invalid CIDR") {
		t.Fatalf("期望报告非法 CIDR，实际 %v", probs)
	}
}

// has 是测试辅助：判断切片中是否有含 substr 的项。
func has(ss []string, substr string) bool {
	for _, s := range ss {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}
