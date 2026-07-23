package version

import "testing"

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
