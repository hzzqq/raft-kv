// experiments/main.go —— raft-kv 容错与性能可展示实验入口。
//
// 用法（在 experiments/ 目录下，需 Go 工具链）：
//   go run . -scenario leader     # 场景 A：leader 故障切换
//   go run . -scenario partition  # 场景 B：网络分区脑裂（3+2 分裂）
//   go run . -scenario perf       # 场景 C：ShardKV 分片 vs 单组吞吐曲线
//   go run . -scenario migration  # 场景 D：多组（n_groups>1）分片迁移故障（跨组 rebalance 期间零丢失写）
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
	scenario := flag.String("scenario", "leader", "leader | partition | perf | migration")
	assert := flag.Bool("assert", false, "断言模式：不变量不达标则非零退出（CI 回归护栏；perf 查扩展比，leader/partition 查客户端视角结构化报告 ok）")
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
	case "migration":
		runMigration()
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q (leader|partition|perf)\n", *scenario)
		os.Exit(2)
	}
	// I175：断言模式——把客户端视角结构化报告（I174 落盘）变成 CI 可强制校验的不变量。
	// 场景 A/B 此前只在 stdout 打印 ✗ 后 return 0，最强正确性保证（无脑裂 / 零丢失写）
	// 从未被任何门禁守住；此处复用其 client_view_*.json 的 ok 字段，任一不通过即非零退出。
	if *assert {
		assertClientViewScenarios(*scenario)
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

// assertClientViewScenarios 在断言模式下，校验指定场景落盘的客户端视角结构化报告的
// 关键不变量（I174 产出，带 ok 字段）。场景 A/B 的最强正确性保证（无脑裂双写 / 零丢失写）
// 此前只是 stdout 文本，从未被门禁强制；这里把它们变成机器可校验的 exit code。
//
// 为什么复用 JSON 而非在场景内直接 os.Exit：场景已把探头结果量化进 client_view_*.json，
// 断言逻辑与「展示逻辑」共用同一份真值，避免两套口径漂移；且早期失败（如未选出 leader）
// 会导致 JSON 未落盘 → 文件缺失即判失败，覆盖所有 ✗ 路径。
func assertClientViewScenarios(scenario string) {
	var files []string
	switch scenario {
	case "leader":
		files = []string{"client_view_leader.json"}
	case "partition":
		// 多数派客户端视角 + 少数派(危险路径)客户端视角，两者都必须 ok。
		files = []string{"client_view_partition.json", "client_view_minority.json"}
	case "migration":
		// 多组跨组迁移 + 副本崩溃期间，已确认写零丢失（I176 新场景）。
		files = []string{"client_view_migration.json"}
	default:
		return // perf 自行 --assert，不在此校验
	}
	allOK := true
	for _, name := range files {
		if !assertClientViewOK(name) {
			allOK = false
		}
	}
	if !allOK {
		os.Exit(1)
	}
}

// assertClientViewOK 校验单个客户端视角报告：文件必须存在、可解析、且 ok==true。
// 少数派的 ok 等价于 split_brain==false（绝未拿到一次成功确认）；多数派/leader 的 ok
// 等价于已确认写零丢失。任一不通过即打印 ✗ 并返回 false。
func assertClientViewOK(name string) bool {
	fp := filepath.Join("results", name)
	b, err := os.ReadFile(fp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "✗ [断言] 未找到客户端视角产物 %s（场景未产出结构化报告，或提前失败未落盘）\n", fp)
		return false
	}
	var r struct {
		Scenario   string `json:"scenario"`
		Ok         bool   `json:"ok"`
		SplitBrain bool   `json:"split_brain"`
	}
	if err := json.Unmarshal(b, &r); err != nil {
		fmt.Fprintf(os.Stderr, "✗ [断言] 产物 %s 解析失败: %v\n", fp, err)
		return false
	}
	if !r.Ok {
		fmt.Fprintf(os.Stderr, "✗ [断言] %s 不变量未通过（ok=false；少数派 split_brain=%v）\n", r.Scenario, r.SplitBrain)
		return false
	}
	fmt.Printf("✓ [断言] %s 客户端视角不变量通过（ok=true）\n", r.Scenario)
	return true
}
