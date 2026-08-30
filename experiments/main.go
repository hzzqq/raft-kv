// experiments/main.go —— raft-kv 容错与性能可展示实验入口。
//
// 用法（在 experiments/ 目录下，需 Go 工具链）：
//   go run . -scenario leader     # 场景 A：leader 故障切换
//   go run . -scenario partition  # 场景 B：网络分区脑裂（3+2 分裂）
//   go run . -scenario perf       # 场景 C：ShardKV 分片 vs 单组吞吐曲线
//
// 本目录是独立 Go 模块（go.mod + replace raftkv=>..），不计入 raft-kv 父模块的
// 测试覆盖门禁，因此新增实验不会动摇父项目已冻结的 100 分。
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	scenario := flag.String("scenario", "leader", "leader | partition | perf")
	flag.Parse()
	resetClock()
	switch *scenario {
	case "leader":
		runLeader()
	case "partition":
		runPartition()
	case "perf":
		runPerf()
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q (leader|partition|perf)\n", *scenario)
		os.Exit(2)
	}
}
