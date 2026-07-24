// version —— 构建信息包。变量可由链接器在编译期注入（如
// -ldflags "-X raftkv/src/version.Commit=abc123"），便于二进制自报版本、排障时
// 快速确认跑的是哪次构建。未注入时回退到默认值，保证测试与裸 go build 也能编译运行。
package version

import (
	"fmt"
	"runtime/debug"
	"sync"
)

// 以下变量允许通过 -ldflags 覆盖；默认值用于本地开发/测试。
var (
	BuildVersion = "dev"
	Commit       = "unknown"
	BuildTime    = "unknown"
)

// once 保证 LoadFromBuildInfo 的全局填充只执行一次（幂等，重复调用无害但避免重复 I/O）。
var once sync.Once

// applyBuildInfo 是 LoadFromBuildInfo 的纯核：当字段仍为默认值（"dev"/"unknown"）时，
// 用 bi 中的 VCS 信息补全，返回新 Info（不修改入参）。显式 -ldflags 注入的非默认值
// 不会被覆盖。便于无副作用单测（#210）。
func applyBuildInfo(cur Info, bi *debug.BuildInfo) Info {
	out := cur
	if out.Version == "dev" && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		out.Version = bi.Main.Version
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			if out.Commit == "unknown" && s.Value != "" {
				out.Commit = s.Value
			}
		case "vcs.time":
			if out.BuildTime == "unknown" && s.Value != "" {
				out.BuildTime = s.Value
			}
		}
	}
	return out
}

// LoadFromBuildInfo 尝试从 runtime/debug.ReadBuildInfo() 自动补全未注入的版本字段
// （未传 -ldflags 的 go build / go run，可执行文件仍携带 VCS 信息，Go 1.18+ 默认记录
// vcs.revision / vcs.time）。仅当字段仍为默认值时覆盖，避免冲掉显式 -ldflags 注入。
// 应在进程启动期调用一次（幂等）；返回是否做了任何填充。R2 隐性：此前无 -ldflags 的
// 二进制 /version 端点恒报 "unknown" commit，排障时无法定位构建来源。
func LoadFromBuildInfo() bool {
	var changed bool
	once.Do(func() {
		bi, ok := debug.ReadBuildInfo()
		if !ok {
			return
		}
		cur := Get()
		next := applyBuildInfo(cur, bi)
		if next.Version != cur.Version {
			BuildVersion = next.Version
			changed = true
		}
		if next.Commit != cur.Commit {
			Commit = next.Commit
			changed = true
		}
		if next.BuildTime != cur.BuildTime {
			BuildTime = next.BuildTime
			changed = true
		}
	})
	return changed
}

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

// IsDev 报告当前是否开发构建（未经 -ldflags 注入正式版本号）。
// 网关/日志可据此差异化行为（如 dev 才开 verbose 调试端点）。
func IsDev() bool { return BuildVersion == "dev" }

// Short 返回紧凑版本标识："<version>@<commit前7位>"，适合放响应头或日志前缀。
func Short() string {
	c := Commit
	if len(c) > 7 {
		c = c[:7]
	}
	return BuildVersion + "@" + c
}
