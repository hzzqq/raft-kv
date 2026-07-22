package main

import (
	"strings"
	"testing"
	"time"
)

// TestFormatStartupReportClean 验证：无告警时渲染 "all passed" 且含关键字段行。
func TestFormatStartupReportClean(t *testing.T) {
	r := StartupReport{
		Time:       timeForTest(),
		GoVersion:  "go1.22",
		OS:         "windows",
		Arch:       "amd64",
		NumCPU:     8,
		GOMAXPROCS: 8,
		CWD:        "/work",
		Mode:       "normal",
	}
	out := FormatStartupReport(r)
	if !strings.Contains(out, "[demo-diag]") {
		t.Fatalf("缺少 [demo-diag] 前缀：%q", out)
	}
	if !strings.Contains(out, "go=go1.22") || !strings.Contains(out, "os=windows/amd64") {
		t.Fatalf("缺少版本/平台标注：%q", out)
	}
	if !strings.Contains(out, "cwd=/work") || !strings.Contains(out, "mode=normal") {
		t.Fatalf("缺少 cwd/mode：%q", out)
	}
	if !strings.Contains(out, "checks: all passed") {
		t.Fatalf("期望 all passed：%q", out)
	}
}

// TestFormatStartupReportWarnings 验证：有告警时列出条数与每条内容。
func TestFormatStartupReportWarnings(t *testing.T) {
	r := StartupReport{
		Time:     timeForTest(),
		Mode:     "quiet",
		Warnings: []string{"temp dir unwritable: x", "invalid NShards <= 0"},
	}
	out := FormatStartupReport(r)
	if !strings.Contains(out, "checks: 2 warning(s)") {
		t.Fatalf("期望 2 条告警：%q", out)
	}
	if !strings.Contains(out, "temp dir unwritable: x") || !strings.Contains(out, "invalid NShards <= 0") {
		t.Fatalf("期望逐条列出告警：%q", out)
	}
}

// TestCollectStartupReport 验证：采集到的运行期信息合理、无 panic、GoVersion 非空、CPU>0。
// 此函数 cluster-free，不启动集群、不发网络，仅做本地自检。
func TestCollectStartupReport(t *testing.T) {
	// 确保不处于 quiet 模式，避免断言 Mode 时受环境变量干扰。
	t.Setenv("RAFT_KV_DEMO_QUIET", "")
	r := CollectStartupReport()
	if r.GoVersion == "" {
		t.Fatal("GoVersion 不应为空")
	}
	if r.NumCPU <= 0 {
		t.Fatalf("NumCPU 应 >0，实际 %d", r.NumCPU)
	}
	if r.GOMAXPROCS <= 0 {
		t.Fatalf("GOMAXPROCS 应 >0，实际 %d", r.GOMAXPROCS)
	}
	if r.OS == "" || r.Arch == "" {
		t.Fatal("OS/Arch 不应为空")
	}
	// 正常环境自检应通过（临时目录可写、NShards>0）。
	if len(r.Warnings) != 0 {
		t.Fatalf("正常环境下不应有告警，实际 %v", r.Warnings)
	}
}

// TestCollectStartupReportQuiet 验证：环境变量可切换 quiet 模式。
func TestCollectStartupReportQuiet(t *testing.T) {
	t.Setenv("RAFT_KV_DEMO_QUIET", "1")
	r := CollectStartupReport()
	if r.Mode != "quiet" {
		t.Fatalf("期望 mode=quiet，实际 %q", r.Mode)
	}
}

// timeForTest 返回固定时间，避免 FormatStartupReport 依赖 wall clock，便于稳定断言。
func timeForTest() time.Time {
	// 用可解析的固定时刻（2026-07-22T10:55:31+08:00）。
	tm, _ := time.Parse(time.RFC3339, "2026-07-22T10:55:31+08:00")
	return tm
}
