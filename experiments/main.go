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
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	scenario := flag.String("scenario", "leader", "leader | partition | perf")
	assert := flag.Bool("assert", false, "perf 场景：扩展比/错误数不达标则非零退出（CI 性能回归护栏）")
	flag.Parse()
	resetClock()
	// 生成时刻标记：被重定向进 results/scene_*.log，供控制台解析「生成于 <时间>」，
	// 与场景 C 的 perf JSON generated_at 语义对齐（R9b）。
	fmt.Printf("generated_at=%s\n", time.Now().Format(time.RFC3339))
	switch *scenario {
	case "leader":
		runLeader()
	case "partition":
		runPartition()
	case "perf":
		runPerf(*assert)
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q (leader|partition|perf)\n", *scenario)
		os.Exit(2)
	}
	// R9b 加深：把各产物生成时刻汇总进 results/generated_at.json，供控制台一次性读取
	// （单一真相源，比逐文件解析更快更稳）。
	writeArtifactManifest()
}

// writeArtifactManifest 扫描 results/ 目录，把每个产物的生成时刻（.log 首行
// generated_at= 标记、.json 的 generated_at 字段）汇总写入 results/generated_at.json。
// 单进程跑单个场景时也会把既有产物一并纳入，保证清单反映目录下全部可用产物。
func writeArtifactManifest() {
	dir := "results"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	manifest := map[string]interface{}{
		"written_at": time.Now().Format(time.RFC3339),
		"files":      map[string]interface{}{},
	}
	files := manifest["files"].(map[string]interface{})
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		fp := filepath.Join(dir, name)
		var ga string
		if strings.HasSuffix(name, ".json") {
			if b, rerr := os.ReadFile(fp); rerr == nil {
				var d map[string]interface{}
				if json.Unmarshal(b, &d) == nil {
					if v, ok := d["generated_at"].(string); ok {
						ga = v
					}
				}
			}
		} else if strings.HasSuffix(name, ".log") {
			if f, oerr := os.Open(fp); oerr == nil {
				sc := bufio.NewScanner(f)
				if sc.Scan() {
					line := strings.TrimSpace(sc.Text())
					if strings.HasPrefix(line, "generated_at=") {
						ga = strings.TrimSpace(line[len("generated_at="):])
					}
				}
				f.Close()
			}
		}
		if ga != "" {
			files[name] = ga
		}
	}
	if b, merr := json.MarshalIndent(manifest, "", "  "); merr == nil {
		_ = os.WriteFile(filepath.Join(dir, "generated_at.json"), b, 0o644)
	}
}
