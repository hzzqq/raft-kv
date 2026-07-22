// version —— 构建信息包。变量可由链接器在编译期注入（如
// -ldflags "-X raftkv/src/version.Commit=abc123"），便于二进制自报版本、排障时
// 快速确认跑的是哪次构建。未注入时回退到默认值，保证测试与裸 go build 也能编译运行。
package version

import "fmt"

// 以下变量允许通过 -ldflags 覆盖；默认值用于本地开发/测试。
var (
	BuildVersion = "dev"
	Commit       = "unknown"
	BuildTime    = "unknown"
)

// Info 返回结构化的版本信息，便于网关 /version 端点或日志打印。
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
}

// Get 返回当前构建信息快照。
func Get() Info {
	return Info{Version: BuildVersion, Commit: Commit, BuildTime: BuildTime}
}

// String 返回单行可读版本串，如 "raft-kv dev (commit abc123, built 2026-07-22T10:00Z)"。
func String() string {
	return fmt.Sprintf("raft-kv %s (commit %s, built %s)", BuildVersion, Commit, BuildTime)
}
