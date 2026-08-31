// deploycheck_test.go —— 部署编排资产的不变量测试（I4 真部署化）。
//
// 这些断言的价值在于：它们是**唯一**能在不起 Docker 的前提下，发现「compose/Prometheus/
// Grafana 与代码已经对不上」的手段。CI 里跑一次，拓扑漂移与看板幻觉当场暴露。
package deploycheck

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"raftkv/src/cluster"
)

const deployDir = "../../deploy"

func p(name string) string { return filepath.Join(deployDir, name) }

// loadAll 一次性载入四份资产，任何一份缺失都直接 Fatal（部署目录不完整本身就是缺陷）。
func loadAll(t *testing.T) (cluster.ClusterTCPConfig, map[string]ComposeService) {
	t.Helper()
	cfg, err := cluster.LoadTCPConfig(p("cluster.json"))
	if err != nil {
		t.Fatalf("加载 deploy/cluster.json: %v", err)
	}
	svcs, err := ParseCompose(p("docker-compose.yml"))
	if err != nil {
		t.Fatalf("解析 deploy/docker-compose.yml: %v", err)
	}
	if len(svcs) == 0 {
		t.Fatal("docker-compose.yml 未解析出任何服务（扫描器与文件写法不匹配？）")
	}
	return cfg, svcs
}

// nodeServices 返回「承载 Raft 节点」的服务：服务名 -> 节点名。
func nodeServices(svcs map[string]ComposeService) map[string]string {
	out := map[string]string{}
	for name, s := range svcs {
		if s.NodeName != "" {
			out[name] = s.NodeName
		}
	}
	return out
}

// TestClusterConfigSelfConsistent：cluster.json 自身必须自洽。
// 节点清单是全部部署形态的唯一真源，写错一个名字整个集群就是「少数派可用」的假象。
func TestClusterConfigSelfConsistent(t *testing.T) {
	cfg, _ := loadAll(t)

	want := map[string]bool{}
	for j := 0; j < cfg.NSM; j++ {
		want[fmt.Sprintf("m%d", j)] = true
	}
	for g := 0; g < cfg.NGroups; g++ {
		for r := 0; r < cfg.NReplicas; r++ {
			want[fmt.Sprintf("g%d-%d", g, r)] = true
		}
	}
	got := map[string]bool{}
	for _, nd := range cfg.Nodes {
		if got[nd.Name] {
			t.Errorf("节点名重复: %s", nd.Name)
		}
		got[nd.Name] = true
		if nd.Addr == "" || PortOf(nd.Addr) == "" {
			t.Errorf("节点 %s 的 addr=%q 缺少端口", nd.Name, nd.Addr)
		}
	}
	if len(cfg.Nodes) != cfg.NSM+cfg.NGroups*cfg.NReplicas {
		t.Errorf("节点数 %d != n_sm(%d) + n_groups(%d)*n_replicas(%d)",
			len(cfg.Nodes), cfg.NSM, cfg.NGroups, cfg.NReplicas)
	}
	for name := range want {
		if !got[name] {
			t.Errorf("cluster.json 缺少节点 %s（该副本不会被启动）", name)
		}
	}
	for name := range got {
		if !want[name] {
			t.Errorf("cluster.json 多出未知节点 %s（名字不符合 m<j>/g<g>-<r> 或超出规模）", name)
		}
	}

	// 副本数为偶数时多数派不明确（4 副本的多数派仍是 3，白付一份成本），部署配置不该这么写。
	if cfg.NReplicas%2 == 0 || cfg.NSM%2 == 0 {
		t.Errorf("副本数应为奇数以明确多数派：n_replicas=%d n_sm=%d", cfg.NReplicas, cfg.NSM)
	}
	// 真部署必须落盘，否则容器重启即丢状态，「崩溃恢复」无从验证。
	if strings.TrimSpace(cfg.DataDir) == "" {
		t.Error("data_dir 为空：容器重启后状态全丢，真部署形态必须落盘")
	}
}

// TestComposeCoversEveryNodeExactlyOnce：每个节点必须恰好被一个容器承载。
// 漏一个 → 该组只剩 2/3 副本（仍可写，但容错度悄悄降级）；重复一个 → 两个进程
// 抢同一个 -name，行为未定义。
func TestComposeCoversEveryNodeExactlyOnce(t *testing.T) {
	cfg, svcs := loadAll(t)

	hostedBy := map[string][]string{} // 节点名 -> 承载它的服务名
	for name, s := range svcs {
		if s.NodeName != "" {
			hostedBy[s.NodeName] = append(hostedBy[s.NodeName], name)
		}
	}
	for _, nd := range cfg.Nodes {
		switch len(hostedBy[nd.Name]) {
		case 1: // ok
		case 0:
			t.Errorf("节点 %s 没有对应的 compose 服务（副本不会被启动）", nd.Name)
		default:
			sort.Strings(hostedBy[nd.Name])
			t.Errorf("节点 %s 被多个服务承载: %v", nd.Name, hostedBy[nd.Name])
		}
	}
	for nodeName := range hostedBy {
		found := false
		for _, nd := range cfg.Nodes {
			if nd.Name == nodeName {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("compose 启动了 cluster.json 里不存在的节点 %q（该进程会启动失败）", nodeName)
		}
	}
}

// TestServiceNameMatchesNodeAddrHost：容器 DNS 名必须与 cluster.json 里该节点 addr 的
// host 完全一致。不一致的后果最隐蔽——节点自己能 Listen（Docker 把服务名解析到自身 IP
// 会失败/或监听到错网卡），别的副本按 addr 去 dial 也连不上，表现为「永远选不出 leader」。
func TestServiceNameMatchesNodeAddrHost(t *testing.T) {
	cfg, svcs := loadAll(t)

	addrOf := map[string]string{}
	for _, nd := range cfg.Nodes {
		addrOf[nd.Name] = nd.Addr
	}
	for svcName, nodeName := range nodeServices(svcs) {
		addr, ok := addrOf[nodeName]
		if !ok {
			continue // 已由 TestComposeCoversEveryNodeExactlyOnce 报告
		}
		if host := HostOf(addr); host != svcName {
			t.Errorf("服务 %q 承载节点 %s，但其 addr=%q 的 host 是 %q —— 容器间无法互相 dial",
				svcName, nodeName, addr, host)
		}
	}
}

// TestNodeServicesExposeDiagPort：每个节点容器都必须开诊断 HTTP，且端口统一。
// 端口不统一会让 Prometheus 配置退化成逐个特例，最容易漏抓某个副本。
func TestNodeServicesExposeDiagPort(t *testing.T) {
	_, svcs := loadAll(t)

	ports := map[string][]string{}
	for svcName, nodeName := range nodeServices(svcs) {
		s := svcs[svcName]
		if s.DiagAddr == "" {
			t.Errorf("服务 %q（节点 %s）没有 -http 诊断端口 —— Prometheus 抓不到它的共识状态",
				svcName, nodeName)
			continue
		}
		port := PortOf(s.DiagAddr)
		ports[port] = append(ports[port], svcName)
	}
	if len(ports) > 1 {
		t.Errorf("节点诊断端口不统一: %v（应全部一致，便于 Prometheus 统一抓取）", ports)
	}
}

// TestPrometheusScrapesEveryNodeAndGateway：Prometheus 的抓取目标必须与 compose 拓扑
// 逐一对齐。少抓一个副本 = 那台机器上的 leader 切换/apply 落后永远看不到。
func TestPrometheusScrapesEveryNodeAndGateway(t *testing.T) {
	_, svcs := loadAll(t)
	jobs, err := ParsePrometheusTargets(p("prometheus.yml"))
	if err != nil {
		t.Fatalf("解析 prometheus.yml: %v", err)
	}

	const nodeJob, gwJob = "raftkv-nodes", "raftkv-gateway"
	if _, ok := jobs[nodeJob]; !ok {
		t.Fatalf("prometheus.yml 缺少 job %q，现有 %v", nodeJob, SortedKeys(jobs))
	}

	want := map[string]bool{}
	for svcName := range nodeServices(svcs) {
		want[svcName+":"+PortOf(svcs[svcName].DiagAddr)] = true
	}
	got := map[string]bool{}
	for _, tg := range jobs[nodeJob] {
		if got[tg] {
			t.Errorf("job %s 中目标重复: %s", nodeJob, tg)
		}
		got[tg] = true
	}
	for tg := range want {
		if !got[tg] {
			t.Errorf("Prometheus 漏抓节点目标 %s（该副本的共识指标不可观测）", tg)
		}
	}
	for tg := range got {
		if !want[tg] {
			t.Errorf("Prometheus 抓取了不存在的节点目标 %s（scrape 永远失败）", tg)
		}
	}

	// 网关目标端口必须与 gateway 服务的 -addr 一致
	gw, ok := svcs["gateway"]
	if !ok {
		t.Fatal("compose 缺少 gateway 服务")
	}
	wantGW := "gateway:" + PortOf(gw.ListenAddr)
	if tgs := jobs[gwJob]; len(tgs) != 1 || tgs[0] != wantGW {
		t.Errorf("job %s 目标 %v，期望 [%s]（与 gateway -addr %q 对齐）",
			gwJob, tgs, wantGW, gw.ListenAddr)
	}
}

// TestGatewayConnectsSameConfigAsNodes：网关 -connect 与节点 -config 必须是同一份文件。
// 两份不同的清单会让网关按错误地址找节点，症状是 /readyz 永久 503。
func TestGatewayConnectsSameConfigAsNodes(t *testing.T) {
	_, svcs := loadAll(t)
	gw, ok := svcs["gateway"]
	if !ok {
		t.Fatal("compose 缺少 gateway 服务")
	}
	if gw.ConfigPath == "" {
		t.Fatal("gateway 未指定 -connect 配置")
	}
	for svcName := range nodeServices(svcs) {
		if got := svcs[svcName].ConfigPath; got != gw.ConfigPath {
			t.Errorf("服务 %q 的 -config=%q 与 gateway -connect=%q 不同（拓扑视图不一致）",
				svcName, got, gw.ConfigPath)
		}
	}
}

// TestHostPortMappingsUnique：宿主机端口不能撞车，否则 compose up 直接失败。
func TestHostPortMappingsUnique(t *testing.T) {
	_, svcs := loadAll(t)
	owner := map[string]string{}
	for _, name := range SortedKeys(svcs) {
		for _, pair := range svcs[name].HostPorts {
			hostPort := strings.SplitN(pair, ":", 2)[0]
			if prev, dup := owner[hostPort]; dup {
				t.Errorf("宿主机端口 %s 被 %s 与 %s 同时占用", hostPort, prev, name)
				continue
			}
			owner[hostPort] = name
		}
	}
}

// TestComposeMountedFilesExist：compose 以只读卷挂载的配置文件必须真实存在。
// 缺文件时 Docker 会把它当成目录创建，容器行为诡异且难查。
func TestComposeMountedFilesExist(t *testing.T) {
	data, err := os.ReadFile(p("docker-compose.yml"))
	if err != nil {
		t.Fatalf("读取 compose: %v", err)
	}
	checked := 0
	for _, ln := range strings.Split(string(data), "\n") {
		ln = strings.TrimSpace(ln)
		if !strings.HasPrefix(ln, "- ./") {
			continue
		}
		local := strings.TrimPrefix(ln, "- ")
		if i := strings.Index(local, ":"); i > 0 {
			local = local[:i]
		}
		if _, err := os.Stat(filepath.Join(deployDir, strings.TrimPrefix(local, "./"))); err != nil {
			t.Errorf("compose 挂载了不存在的路径 %s: %v", local, err)
		}
		checked++
	}
	if checked == 0 {
		t.Error("未发现任何本地卷挂载（扫描器失效？）")
	}
}

// declaredMetrics 汇总代码真实暴露的全部指标名。
func declaredMetrics(t *testing.T) map[string]bool {
	t.Helper()
	set, err := DeclaredMetrics(metricSourceFiles()...)
	if err != nil {
		t.Fatalf("解析代码指标名: %v", err)
	}
	return set
}

// TestDashboardMetricsAreRealMetrics 是本包最有价值的一条：
// Grafana 看板里的每个指标名都必须是代码真实暴露的。否则面板永远空白，
// 而「看板上一片绿」会被误读成「系统健康」——比没有看板更危险。
func TestDashboardMetricsAreRealMetrics(t *testing.T) {
	declared := declaredMetrics(t)
	title, panels, err := ParseDashboard(p("grafana/dashboards/raftkv-overview.json"))
	if err != nil {
		t.Fatalf("解析看板: %v", err)
	}
	if title == "" || len(panels) == 0 {
		t.Fatalf("看板标题=%q 面板数=%d，内容不完整", title, len(panels))
	}
	totalExprs := 0
	for _, pan := range panels {
		if len(pan.Exprs) == 0 {
			t.Errorf("面板 %q 没有任何查询（空面板）", pan.Title)
		}
		for _, expr := range pan.Exprs {
			totalExprs++
			names := MetricNamesFromPromQL(expr)
			if len(names) == 0 {
				t.Errorf("面板 %q 的查询未解析出指标名: %s", pan.Title, expr)
			}
			for _, n := range names {
				if !declared[n] {
					t.Errorf("面板 %q 引用了代码未暴露的指标 %q\n  查询: %s",
						pan.Title, n, expr)
				}
			}
		}
	}
	if totalExprs < 10 {
		t.Errorf("看板查询仅 %d 条，覆盖面偏薄（至少应覆盖 QPS/延迟/leader/term/lag/分片）", totalExprs)
	}
}

// TestAlertMetricsAreRealMetrics：告警规则同理——引用了不存在的指标，告警永不触发，
// 而「从来没告警过」会被误读成「一直很稳」。
func TestAlertMetricsAreRealMetrics(t *testing.T) {
	declared := declaredMetrics(t)
	exprs, err := ParseAlertExprs(p("alerts.yml"))
	if err != nil {
		t.Fatalf("解析 alerts.yml: %v", err)
	}
	if len(exprs) == 0 {
		t.Fatal("alerts.yml 未解析出任何告警规则")
	}
	for _, alert := range SortedKeys(exprs) {
		expr := exprs[alert]
		names := MetricNamesFromPromQL(expr)
		if len(names) == 0 {
			t.Errorf("告警 %s 未解析出指标名: %s", alert, expr)
		}
		for _, n := range names {
			if !declared[n] {
				t.Errorf("告警 %s 引用了代码未暴露的指标 %q\n  表达式: %s", alert, n, expr)
			}
		}
	}
	// 必须覆盖最关键的失效模式，否则「有告警」只是摆设。
	mustCover := []string{"RaftNoLeader", "RaftSplitBrainSuspect", "ShardMigrationStalled"}
	for _, want := range mustCover {
		if _, ok := exprs[want]; !ok {
			t.Errorf("缺少关键告警 %s（现有: %v）", want, SortedKeys(exprs))
		}
	}
}

// TestPrometheusRuleFileMountMatches：prometheus.yml 里 rule_files 指向的容器内路径，
// 必须与 compose 的挂载目标一致，否则规则静默不加载。
func TestPrometheusRuleFileMountMatches(t *testing.T) {
	promCfg, err := os.ReadFile(p("prometheus.yml"))
	if err != nil {
		t.Fatalf("读取 prometheus.yml: %v", err)
	}
	compose, err := os.ReadFile(p("docker-compose.yml"))
	if err != nil {
		t.Fatalf("读取 compose: %v", err)
	}
	var ruleTargets []string
	inRules := false
	for _, ln := range strings.Split(string(promCfg), "\n") {
		trimmed := strings.TrimSpace(ln)
		if strings.HasPrefix(trimmed, "rule_files:") {
			inRules = true
			continue
		}
		if inRules {
			if strings.HasPrefix(trimmed, "- ") {
				ruleTargets = append(ruleTargets, strings.TrimSpace(trimmed[2:]))
				continue
			}
			if trimmed != "" {
				break
			}
		}
	}
	if len(ruleTargets) == 0 {
		t.Fatal("prometheus.yml 未配置 rule_files（告警规则不会被加载）")
	}
	for _, target := range ruleTargets {
		if !strings.Contains(string(compose), ":"+target+":ro") {
			t.Errorf("rule_files 指向 %s，但 compose 未把任何文件挂到该路径", target)
		}
	}
}

// isPromBuiltin 是 Prometheus 内建指标，自动带 job/instance 等标签，不查其 label 维度。
func isPromBuiltin(metric string) bool {
	return metric == "up" || metric == "scrape_duration_seconds"
}

// TestDashboardLabelSelectorsExist：看板查询里 {label=...} 引用的标签必须真实存在。
// 这是 TestDashboardMetricsAreRealMetrics 的维度升级——指标名存在还不够，引用的 label
// 不存在同样会让面板恒空（看板幻觉运行时版）。例如 raftkv_raft_is_leader{role="leader"}
// 的 role 标签根本没暴露，查询永远返回空，而「一片绿」会被误读成系统健康。
func TestDashboardLabelSelectorsExist(t *testing.T) {
	labels, err := DeclaredMetricLabels(metricSourceFiles()...)
	if err != nil {
		t.Fatalf("解析代码指标标签: %v", err)
	}
	_, panels, err := ParseDashboard(p("grafana/dashboards/raftkv-overview.json"))
	if err != nil {
		t.Fatalf("解析看板: %v", err)
	}
	bad := 0
	for _, pan := range panels {
		for _, expr := range pan.Exprs {
			for metric, labs := range LabelSelectorsFromPromQL(expr) {
				if isPromBuiltin(metric) {
					continue
				}
				got, ok := labels[metric]
				if !ok {
					continue // 指标名不存在由 TestDashboardMetricsAreRealMetrics 报告
				}
				for _, l := range labs {
					if !got[l] {
						t.Errorf("面板 %q 查询 %s 引用指标 %s 不存在的标签 %q（查询会恒空/看板幻觉）",
							pan.Title, expr, metric, l)
						bad++
					}
				}
			}
		}
	}
	if bad == 0 {
		t.Logf("看板 label 选择器全部真实存在（共 %d 面板）", len(panels))
	}
}

// TestAlertLabelSelectorsExist：告警规则同理，引用的 label 必须真实存在。
func TestAlertLabelSelectorsExist(t *testing.T) {
	labels, err := DeclaredMetricLabels(metricSourceFiles()...)
	if err != nil {
		t.Fatalf("解析代码指标标签: %v", err)
	}
	exprs, err := ParseAlertExprs(p("alerts.yml"))
	if err != nil {
		t.Fatalf("解析 alerts.yml: %v", err)
	}
	bad := 0
	for _, alert := range SortedKeys(exprs) {
		for metric, labs := range LabelSelectorsFromPromQL(exprs[alert]) {
			if isPromBuiltin(metric) {
				continue
			}
			got, ok := labels[metric]
			if !ok {
				continue
			}
			for _, l := range labs {
				if !got[l] {
					t.Errorf("告警 %s 查询 %s 引用指标 %s 不存在的标签 %q（查询会恒空/告警永不触发）",
						alert, exprs[alert], metric, l)
					bad++
				}
			}
		}
	}
	if bad == 0 {
		t.Logf("告警 label 选择器全部真实存在（共 %d 条规则）", len(exprs))
	}
}

// ---------------------------------------------------------------- 扫描器自身的测试
// 守护者本身必须可信：下面几条测试直接给 PromQL 抽取器喂已知输入。

func TestMetricNamesFromPromQL(t *testing.T) {
	cases := []struct {
		expr string
		want []string
	}{
		{"sum(raftkv_node_up)", []string{"raftkv_node_up"}},
		{`sum by (method) (rate(http_requests_total[1m]))`, []string{"http_requests_total"}},
		{`sum(increase(raftkv_raft_leader_elections_total{group="0"}[5m]))`,
			[]string{"raftkv_raft_leader_elections_total"}},
		{`up{job="raftkv-nodes"} == 0`, []string{"up"}},
		{"http_request_latency_ms_p99 > 500", []string{"http_request_latency_ms_p99"}},
		{`sum(rate(http_responses_total{code=~"5.."}[1m]))`, []string{"http_responses_total"}},
		{`max by (group) (raftkv_shard_config_num) - min by (group) (raftkv_shard_config_num) > 0`,
			[]string{"raftkv_shard_config_num"}},
		{"min_over_time(raftkv_raft_apply_lag[2m]) > 0", []string{"raftkv_raft_apply_lag"}},
		{`sum by (group) (raftkv_raft_is_leader) > 1`, []string{"raftkv_raft_is_leader"}},
		// 二元运算两侧都是指标时都要抽出来
		{"a_total / b_total", []string{"a_total", "b_total"}},
		// 标签值里的伪指标名不能被误抽
		{`x_total{other="y_total"}`, []string{"x_total"}},
	}
	for _, c := range cases {
		got := MetricNamesFromPromQL(c.expr)
		if strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Errorf("MetricNamesFromPromQL(%q) = %v, want %v", c.expr, got, c.want)
		}
	}
}

func TestDeclaredMetricsFindsHistogramDerivatives(t *testing.T) {
	set, err := DeclaredMetrics("../gateway/gateway.go")
	if err != nil {
		t.Fatalf("DeclaredMetrics: %v", err)
	}
	// 直方图必须展开成 Prometheus 实际暴露的五条派生序列
	for _, name := range []string{
		"http_request_latency_ms_count", "http_request_latency_ms_sum",
		"http_request_latency_ms_p50", "http_request_latency_ms_p95", "http_request_latency_ms_p99",
	} {
		if !set[name] {
			t.Errorf("缺少直方图派生指标 %s", name)
		}
	}
	// 直接注册的向量/仪表也要在册
	for _, name := range []string{"http_requests_total", "http_responses_total", "raft_min_health_score"} {
		if !set[name] {
			t.Errorf("缺少注册指标 %s", name)
		}
	}
	// 不该把随便一个字符串当指标
	if set["Total HTTP requests served, labeled by method."] {
		t.Error("把 help 文本误当成指标名")
	}
}

func TestDeclaredMetricsFindsHandwrittenNodeMetrics(t *testing.T) {
	set, err := DeclaredMetrics("../kvnode/metrics.go")
	if err != nil {
		t.Fatalf("DeclaredMetrics: %v", err)
	}
	for _, name := range []string{
		"raftkv_node_up", "raftkv_raft_term", "raftkv_raft_is_leader",
		"raftkv_raft_leader_elections_total", "raftkv_raft_apply_lag",
		"raftkv_shard_stall_seconds", "raftkv_shard_health_score",
	} {
		if !set[name] {
			t.Errorf("kvnode 手写指标 %s 未被识别", name)
		}
	}
}

func TestParseComposeExtractsFlags(t *testing.T) {
	_, svcs := loadAll(t)
	sm0, ok := svcs["sm0"]
	if !ok {
		t.Fatalf("未解析出 sm0 服务，现有: %v", SortedKeys(svcs))
	}
	if sm0.NodeName != "m0" {
		t.Errorf("sm0.NodeName = %q, want m0", sm0.NodeName)
	}
	if PortOf(sm0.DiagAddr) != "9100" {
		t.Errorf("sm0.DiagAddr = %q, want :9100", sm0.DiagAddr)
	}
	if len(sm0.HostPorts) != 1 || !strings.HasSuffix(sm0.HostPorts[0], ":9100") {
		t.Errorf("sm0.HostPorts = %v, want 单条 *:9100 映射", sm0.HostPorts)
	}
	if !sm0.MergesBase {
		t.Error("sm0 未合并 x-node-base 锚点（healthcheck/image 会缺失）")
	}
	// loadgen 是压测客户端，不该被当成 Raft 节点
	if lg, ok := svcs["loadgen"]; ok && lg.NodeName != "" {
		t.Errorf("loadgen 被误判为节点服务（NodeName=%q）", lg.NodeName)
	}
}
