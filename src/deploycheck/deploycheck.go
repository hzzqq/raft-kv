// Package deploycheck 把 deploy/ 下的部署编排资产与**代码真实行为**绑在一起做机器校验。
//
// 为什么需要它：docker-compose / prometheus.yml / Grafana 看板都是「字符串配置」，
// 编译器不看、单测不碰。于是最典型的两类事故完全不会被发现：
//
//	① 拓扑漂移：cluster.json 里节点 addr 写 kv0:7100，compose 里服务却叫 kvnode-0，
//	   容器互相 dial 不通；或某个副本忘了起容器 —— 集群跑起来是「少数派可用」的假象。
//	② 看板幻觉：Grafana / 告警规则里写了 raftkv_raft_leader_changes_total 这种
//	   看似合理但代码从没暴露过的指标名，面板永远空着、告警永远不触发 —— 而这恰恰是
//	   「以为自己有可观测性」最危险的形态。
//
// 本包用**只读解析**把这些配置还原成结构化事实，交给 deploycheck_test.go 做不变量断言。
// 不需要 Docker、不需要起集群，`go test ./src/deploycheck/` 秒级跑完。
//
// 注意：这里的 YAML 处理是**针对本仓库固定写法的定向扫描器**，不是通用 YAML 解析器
// （本仓库零第三方依赖，不引入 yaml 库）。若有人把 compose 改成扫描器不认识的写法，
// 测试会**失败而不是静默放过** —— 这正是期望行为：配置形态变了就该有人重新确认。
package deploycheck

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// ---------------------------------------------------------------- compose

// ComposeService 是从 docker-compose.yml 还原出的单个服务事实。
type ComposeService struct {
	Name       string   // 服务名，同时也是容器在 compose 网络里的 DNS 名
	Raw        string   // 该服务的原始文本块（供兜底断言）
	NodeName   string   // kvnode 的 -name 值（如 m0 / g0-0）；非节点服务为空
	DiagAddr   string   // kvnode 的 -http 值（如 :9100）
	ListenAddr string   // gateway 的 -addr 值（如 :8080）
	ConfigPath string   // -config / -connect 指向的集群配置路径
	HostPorts  []string // ports 映射的宿主机端口（"9101:9100" 取左值）
	MergesBase bool     // 是否合并了 x-node-base 锚点（<<: *node-base）
}

var (
	reServiceHead = regexp.MustCompile(`^  ([A-Za-z0-9_-]+):\s*$`)
	rePortPair    = regexp.MustCompile(`"(\d+):(\d+)"`)
	reQuoted      = regexp.MustCompile(`"([^"]*)"`)
)

// ParseCompose 提取 services: 段下的全部服务。
func ParseCompose(path string) (map[string]ComposeService, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")

	inServices := false
	cur := ""
	blocks := map[string][]string{}
	order := []string{}
	for _, ln := range lines {
		trimmed := strings.TrimRight(ln, " \t")
		if trimmed == "" || strings.HasPrefix(strings.TrimSpace(trimmed), "#") {
			continue
		}
		// 顶层键（indent 0）：进入/离开 services 段
		if !strings.HasPrefix(trimmed, " ") {
			inServices = strings.HasPrefix(trimmed, "services:")
			cur = ""
			continue
		}
		if !inServices {
			continue
		}
		if m := reServiceHead.FindStringSubmatch(trimmed); m != nil {
			cur = m[1]
			order = append(order, cur)
			blocks[cur] = nil
			continue
		}
		if cur != "" {
			blocks[cur] = append(blocks[cur], trimmed)
		}
	}

	out := make(map[string]ComposeService, len(order))
	for _, name := range order {
		raw := strings.Join(blocks[name], "\n")
		svc := ComposeService{Name: name, Raw: raw}
		svc.MergesBase = strings.Contains(raw, "<<: *node-base")
		svc.NodeName = flagValue(raw, "-name")
		svc.DiagAddr = flagValue(raw, "-http")
		svc.ListenAddr = flagValue(raw, "-addr")
		if v := flagValue(raw, "-config"); v != "" {
			svc.ConfigPath = v
		} else {
			svc.ConfigPath = flagValue(raw, "-connect")
		}
		for _, m := range rePortPair.FindAllStringSubmatch(portsBlock(blocks[name]), -1) {
			svc.HostPorts = append(svc.HostPorts, m[1]+":"+m[2])
		}
		out[name] = svc
	}
	return out, nil
}

// flagValue 在服务块里找 `"<flag>", "<value>"` 形态的命令行参数取值。
// 兼容流式序列（["kvnode","-name","m0"]）与块式序列（- "-name"\n- "m0"）：
// 两者拼接后，紧随该 flag 的下一个双引号 token 就是它的取值。
func flagValue(raw, flag string) string {
	toks := reQuoted.FindAllStringSubmatch(raw, -1)
	for i, m := range toks {
		if m[1] == flag && i+1 < len(toks) {
			return toks[i+1][1]
		}
	}
	return ""
}

// portsBlock 抽出 ports: 的取值区（流式同行 or 块式后续缩进行），避免把别处的
// "host:port" 误当端口映射。
func portsBlock(block []string) string {
	var sb strings.Builder
	capturing := false
	baseIndent := 0
	for _, ln := range block {
		indent := len(ln) - len(strings.TrimLeft(ln, " "))
		t := strings.TrimSpace(ln)
		if strings.HasPrefix(t, "ports:") {
			sb.WriteString(t)
			sb.WriteString("\n")
			capturing = !strings.Contains(t, "[") // 流式已在同行给全
			baseIndent = indent
			continue
		}
		if capturing {
			if indent <= baseIndent {
				capturing = false
				continue
			}
			sb.WriteString(t)
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

// ---------------------------------------------------------------- prometheus

var (
	reJobName = regexp.MustCompile(`^\s*-\s*job_name:\s*(\S+)\s*$`)
	reTargets = regexp.MustCompile(`targets:\s*\[([^\]]*)\]`)
)

// ParsePrometheusTargets 返回 job_name -> targets 列表。
func ParsePrometheusTargets(path string) (map[string][]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	job := ""
	for _, ln := range strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n") {
		if m := reJobName.FindStringSubmatch(ln); m != nil {
			job = strings.Trim(m[1], `"'`)
			if _, ok := out[job]; !ok {
				out[job] = nil
			}
			continue
		}
		if job == "" {
			continue
		}
		if m := reTargets.FindStringSubmatch(ln); m != nil {
			for _, t := range strings.Split(m[1], ",") {
				t = strings.TrimSpace(strings.Trim(strings.TrimSpace(t), `"'`))
				if t != "" {
					out[job] = append(out[job], t)
				}
			}
		}
	}
	return out, nil
}

var reExprLine = regexp.MustCompile(`^\s*expr:\s*(.+?)\s*$`)

// ParseAlertExprs 返回告警规则文件里的 alert 名 -> PromQL 表达式。
func ParseAlertExprs(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	alert := ""
	for _, ln := range strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n") {
		if i := strings.Index(ln, "- alert:"); i >= 0 {
			alert = strings.TrimSpace(ln[i+len("- alert:"):])
			continue
		}
		if m := reExprLine.FindStringSubmatch(ln); m != nil && alert != "" {
			out[alert] = m[1]
		}
	}
	return out, nil
}

// ---------------------------------------------------------------- grafana

// DashboardPanel 是看板里一个面板的可校验部分。
type DashboardPanel struct {
	Title string
	Exprs []string
}

// ParseDashboard 读取 Grafana 看板 JSON，返回标题与全部 PromQL 表达式（含面板归属）。
func ParseDashboard(path string) (title string, panels []DashboardPanel, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", nil, err
	}
	var doc struct {
		Title  string `json:"title"`
		Panels []struct {
			Title   string `json:"title"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return "", nil, fmt.Errorf("%s 不是合法 JSON: %w", filepath.Base(path), err)
	}
	for _, p := range doc.Panels {
		dp := DashboardPanel{Title: p.Title}
		for _, t := range p.Targets {
			if strings.TrimSpace(t.Expr) != "" {
				dp.Exprs = append(dp.Exprs, t.Expr)
			}
		}
		panels = append(panels, dp)
	}
	return doc.Title, panels, nil
}

// ---------------------------------------------------------------- promql

var (
	reString    = regexp.MustCompile(`"[^"]*"|'[^']*'`)
	reBraces    = regexp.MustCompile(`\{[^{}]*\}`)
	reBrackets  = regexp.MustCompile(`\[[^\[\]]*\]`)
	reGrouping  = regexp.MustCompile(`\b(by|without|on|ignoring|group_left|group_right)\s*\([^()]*\)`)
	reIdentNext = regexp.MustCompile(`[a-zA-Z_:][a-zA-Z0-9_:]*`)
	// 指标名后紧跟的标签匹配器：用于抽取 {label=...} 里被引用的 label。
	reMetricBrace  = regexp.MustCompile(`([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^{}]*)\}`)
	reLabelMatcher = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=|!=|=~|!~)\s*"`)
)

// promQLKeywords 是不该被当成指标名的保留字/操作符（函数名靠「后随 (」判定，无需枚举）。
var promQLKeywords = map[string]bool{
	"and": true, "or": true, "unless": true, "bool": true, "offset": true,
	"by": true, "without": true, "on": true, "ignoring": true,
	"group_left": true, "group_right": true, "inf": true, "nan": true,
	"start": true, "end": true, "atan2": true,
}

// MetricNamesFromPromQL 从一条 PromQL 表达式里抽出被引用的**指标名**。
//
// 做法（按顺序剥离非指标成分）：字符串字面量 → 标签匹配器 {} → 区间选择器 []
// → by/without/on/ignoring 的标签列表 → 紧随 "(" 的标识符（函数名）。
// 剩下的标识符即指标名。
func MetricNamesFromPromQL(expr string) []string {
	s := reString.ReplaceAllString(expr, `""`)
	// 标签匹配器可能嵌套在函数里，反复剥离直到稳定
	for {
		next := reBraces.ReplaceAllString(s, " ")
		if next == s {
			break
		}
		s = next
	}
	s = reBrackets.ReplaceAllString(s, " ")
	for {
		next := reGrouping.ReplaceAllString(s, " ")
		if next == s {
			break
		}
		s = next
	}

	seen := map[string]bool{}
	var out []string
	for _, loc := range reIdentNext.FindAllStringIndex(s, -1) {
		id := s[loc[0]:loc[1]]
		// 紧随（允许空格）左括号 → 函数调用，不是指标
		rest := strings.TrimLeft(s[loc[1]:], " \t")
		if strings.HasPrefix(rest, "(") {
			continue
		}
		if promQLKeywords[strings.ToLower(id)] {
			continue
		}
		if _, err := strconv.ParseFloat(id, 64); err == nil {
			continue
		}
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------- 代码侧指标名

// registrarMethods 是 metrics.Registry 上「声明一个指标」的方法名集合：
// 首个字符串实参即指标名。
var registrarMethods = map[string]bool{
	"Counter": true, "CounterWithHelp": true,
	"CounterVec": true, "CounterVecWithHelp": true,
	"Gauge": true, "GaugeWithHelp": true,
	"GaugeVec": true, "GaugeVecWithHelp": true,
	"Histogram": true, "HistWithHelp": true,
	"FuncGauge": true, "FuncGaugeWithHelp": true,
}

// histogramMethods 单独记：直方图在 Prometheus 文本里派生成 _count/_sum/_p50/_p95/_p99
// 五条序列（见 metrics.Registry.WritePrometheus），看板引用的是派生名。
var histogramMethods = map[string]bool{"Histogram": true, "HistWithHelp": true}

// vecMethods 是「带标签向量」的注册方法：首个字符串实参是指标名，其后的字符串实参
// 即标签名（CounterVecWithHelp 还需跳过 help 文本，故标签从实参[2:] 起；CounterVec
// 无 help，标签从实参[1:] 起）。
var vecHelpMethods = map[string]bool{"CounterVecWithHelp": true, "GaugeVecWithHelp": true}
var vecNoHelpMethods = map[string]bool{"CounterVec": true, "GaugeVec": true}

var histSuffixes = []string{"_count", "_sum", "_p50", "_p95", "_p99"}

// DeclaredMetrics 用 go/ast 扫描给定 Go 源文件，返回代码**真实暴露**的指标名集合。
//
// 两种来源：
//   - Registry 注册调用（Counter/Gauge/Histogram/... 的首个字符串实参），
//     直方图额外展开五个派生后缀；
//   - 以 "raftkv_" 开头的裸字符串字面量（kvnode/metrics.go 是手写 Prometheus 文本，
//     指标名以复合字面量的形式出现，不经 Registry）。
func DeclaredMetrics(files ...string) (map[string]bool, error) {
	out := map[string]bool{
		// Prometheus 内建：抓取成功与否，非本项目暴露但可被告警/看板合法引用
		"up": true, "scrape_duration_seconds": true,
	}
	fset := token.NewFileSet()
	for _, f := range files {
		file, err := parser.ParseFile(fset, f, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("解析 %s: %w", f, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CallExpr:
				sel, ok := node.Fun.(*ast.SelectorExpr)
				if !ok || !registrarMethods[sel.Sel.Name] || len(node.Args) == 0 {
					return true
				}
				name, ok := stringLit(node.Args[0])
				if !ok {
					return true
				}
				out[name] = true
				if histogramMethods[sel.Sel.Name] {
					for _, suf := range histSuffixes {
						out[name+suf] = true
					}
				}
			case *ast.BasicLit:
				if s, ok := stringLit(node); ok && strings.HasPrefix(s, "raftkv_") {
					out[s] = true
				}
			}
			return true
		})
	}
	return out, nil
}

func stringLit(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return s, true
}

// metricSourceFiles 返回代码侧「真实暴露指标」的源文件集合（kvnode 手写 Prometheus
// 文本 + gateway 的 Registry 注册 + 其余非测试 Go 源）。供 DeclaredMetrics 与
// DeclaredMetricLabels 共用，避免两处各自硬编码文件清单而漂移。
func metricSourceFiles() []string {
	files := []string{"../kvnode/metrics.go", "../gateway/gateway.go"}
	for _, dir := range []string{"../shardkv", "../shardmaster", "../raft"} {
		matches, _ := filepath.Glob(filepath.Join(dir, "*.go"))
		for _, m := range matches {
			if !strings.HasSuffix(m, "_test.go") {
				files = append(files, m)
			}
		}
	}
	return files
}

// LabelSelectorsFromPromQL 抽出每条查询里「指标名 {label=...}」形式的标签选择器，
// 返回 metric -> 被引用的 label 列表。用于校验看板/告警引用的 label 是否真实存在：
// 引用一个代码没暴露的 label（如 raftkv_raft_is_leader{role="leader"} 的 role），
// 查询会恒空、面板静默空白——这正是「看板幻觉」的运行时版，而 DeclaredMetrics 只查
// 指标名存在、查不到这种维度错误。
func LabelSelectorsFromPromQL(expr string) map[string][]string {
	s := reString.ReplaceAllString(expr, `""`)
	out := map[string][]string{}
	for _, m := range reMetricBrace.FindAllStringSubmatch(s, -1) {
		metric, inner := m[1], m[2]
		if promQLKeywords[strings.ToLower(metric)] {
			continue
		}
		var labs []string
		for _, lm := range reLabelMatcher.FindAllStringSubmatch(inner, -1) {
			labs = append(labs, lm[1])
		}
		if len(labs) > 0 {
			out[metric] = append(out[metric], labs...)
		}
	}
	return out
}

// DeclaredMetricLabels 返回代码真实暴露的指标名 -> 该指标运行时携带的标签集合。
// 两个来源：
//   - kvnode 手写 Prometheus 文本（raftkv_*）：所有指标经 nodeLabels 统一带
//     {node,kind,group,replica}；
//   - gateway 的 Registry Vec 注册：标签名是注册调用的实参。
//
// Prometheus 内建指标（up / scrape_duration_seconds）自动带 job/instance，不在此枚举，
// 调用方应跳过其 label 校验。
func DeclaredMetricLabels(files ...string) (map[string]map[string]bool, error) {
	labels := map[string]map[string]bool{
		"up":                      {}, // prometheus 内建，自动带 job/instance
		"scrape_duration_seconds": {},
	}
	fset := token.NewFileSet()
	for _, f := range files {
		file, err := parser.ParseFile(fset, f, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("解析 %s: %w", f, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CallExpr:
				sel, ok := node.Fun.(*ast.SelectorExpr)
				if !ok || len(node.Args) == 0 {
					return true
				}
				name, ok := stringLit(node.Args[0])
				if !ok {
					return true
				}
				switch {
				case vecHelpMethods[sel.Sel.Name]:
					labels[name] = labelSetFromArgs(node.Args[2:])
				case vecNoHelpMethods[sel.Sel.Name]:
					labels[name] = labelSetFromArgs(node.Args[1:])
				default:
					if histogramMethods[sel.Sel.Name] || registrarMethods[sel.Sel.Name] {
						labels[name] = map[string]bool{} // 非向量指标无标签
					}
				}
			case *ast.BasicLit:
				if s, ok := stringLit(node); ok && strings.HasPrefix(s, "raftkv_") {
					labels[s] = map[string]bool{"node": true, "kind": true, "group": true, "replica": true}
				}
			}
			return true
		})
	}
	return labels, nil
}

// labelSetFromArgs 把一批字符串实参（标签名）收成集合。
func labelSetFromArgs(args []ast.Expr) map[string]bool {
	m := map[string]bool{}
	for _, a := range args {
		if s, ok := stringLit(a); ok && s != "" {
			m[s] = true
		}
	}
	return m
}

// ---------------------------------------------------------------- 工具

// SortedKeys 返回 map 的有序键，便于测试失败信息稳定可读。
func SortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// HostOf 取 "host:port" 的 host 部分（无端口时原样返回）。
func HostOf(addr string) string {
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[:i]
	}
	return addr
}

// PortOf 取 "host:port" / ":port" 的 port 部分（无端口时返回空串）。
func PortOf(addr string) string {
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[i+1:]
	}
	return ""
}
