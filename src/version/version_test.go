package version

import (
	"runtime/debug"
	"testing"
)

func TestVersion(t *testing.T) {
	info := Get()
	if info.Version == "" || info.Commit == "" || info.BuildTime == "" {
		t.Fatalf("版本信息不应为空：%+v", info)
	}
	s := String()
	if s == "" {
		t.Fatal("String() 不应返回空串")
	}
	// 默认 dev 构建也应能被解析（不 panic、字段可达）
	if info.Version != "dev" {
		t.Fatalf("未注入 ldflags 时应回退到 dev，实际 %q", info.Version)
	}
}

// TestIsDev 验证未注入 ldflags 时为开发构建。
func TestIsDev(t *testing.T) {
	if !IsDev() {
		t.Fatalf("默认构建应判定为 dev（BuildVersion=%q）", BuildVersion)
	}
}

// TestShort 验证紧凑标识格式与 commit 截断。
func TestShort(t *testing.T) {
	old := Commit
	defer func() { Commit = old }()

	Commit = "abcdef0123456789"
	if got := Short(); got != BuildVersion+"@abcdef0" {
		t.Fatalf("Short() = %q，期望 %q", got, BuildVersion+"@abcdef0")
	}
	Commit = "short"
	if got := Short(); got != BuildVersion+"@short" {
		t.Fatalf("短 commit 不应截断，得到 %q", got)
	}
}

// TestApplyBuildInfo 是 #210 的核心纯函数单测：applyBuildInfo 不修改入参，
// 仅在字段仍为默认值（"dev"/"unknown"）时用 VCS 信息补全；显式注入的非默认值不被覆盖。
func TestApplyBuildInfo(t *testing.T) {
	bi := &debug.BuildInfo{
		Main: debug.Module{Version: "v1.2.3"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "abc123def456"},
			{Key: "vcs.time", Value: "2026-07-24T10:00:00Z"},
		},
	}

	// 1) 默认 cur：三个字段都应被 VCS 补全。
	out := applyBuildInfo(Info{Version: "dev", Commit: "unknown", BuildTime: "unknown"}, bi)
	if out.Version != "v1.2.3" {
		t.Fatalf("version 未补全：%q", out.Version)
	}
	if out.Commit != "abc123def456" {
		t.Fatalf("commit 未补全：%q", out.Commit)
	}
	if out.BuildTime != "2026-07-24T10:00:00Z" {
		t.Fatalf("buildTime 未补全：%q", out.BuildTime)
	}

	// 2) 显式 -ldflags 注入的非默认值绝不被覆盖（纯函数只填默认值）。
	out2 := applyBuildInfo(Info{Version: "1.0.0", Commit: "customsha", BuildTime: "x"}, bi)
	if out2.Version != "1.0.0" || out2.Commit != "customsha" || out2.BuildTime != "x" {
		t.Fatalf("显式注入被错误覆盖：%+v", out2)
	}

	// 3) 空 VCS（如 go run 未带模块信息）：保持默认，不做任何填充。
	out3 := applyBuildInfo(Info{Version: "dev", Commit: "unknown", BuildTime: "unknown"}, &debug.BuildInfo{})
	if out3.Version != "dev" || out3.Commit != "unknown" || out3.BuildTime != "unknown" {
		t.Fatalf("空 VCS 不应填充：%+v", out3)
	}

	// 4) (devel) 版本号视为无效，不覆盖 dev。
	out4 := applyBuildInfo(Info{Version: "dev", Commit: "unknown", BuildTime: "unknown"},
		&debug.BuildInfo{Main: debug.Module{Version: "(devel)"}, Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "r"}}})
	if out4.Version != "dev" {
		t.Fatalf("(devel) 版本号应被忽略，实际 %q", out4.Version)
	}
	if out4.Commit != "r" {
		t.Fatalf("commit 仍应填充，实际 %q", out4.Commit)
	}
}

// TestApplyBuildInfoNoMutation 验证 applyBuildInfo 不修改入参（纯度，便于安全复用）。
func TestApplyBuildInfoNoMutation(t *testing.T) {
	cur := Info{Version: "dev", Commit: "unknown", BuildTime: "unknown"}
	bi := &debug.BuildInfo{Main: debug.Module{Version: "v9"}, Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "z"}}}
	_ = applyBuildInfo(cur, bi)
	if cur.Version != "dev" || cur.Commit != "unknown" || cur.BuildTime != "unknown" {
		t.Fatalf("applyBuildInfo 不应修改入参：%+v", cur)
	}
}

// TestLoadFromBuildInfoIdempotent 验证全局填充幂等：重复调用不 panic、Get() 稳定。
func TestLoadFromBuildInfoIdempotent(t *testing.T) {
	first := LoadFromBuildInfo()
	second := LoadFromBuildInfo()
	// 第二次调用被 sync.Once 短路，必为 false（无副作用）。
	if second {
		t.Fatal("第二次 LoadFromBuildInfo 应被 once 短路返回 false")
	}
	_ = first // 首次可能填充也可能不填充（取决于测试二进制是否带 VCS 信息），均合法
	a, b := Get(), Get()
	if a != b {
		t.Fatalf("Get() 应稳定：%v != %v", a, b)
	}
}
