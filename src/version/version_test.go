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
