package linearizability

import "testing"

// TestCheckHistorySynthetic 证明检查器本身正确：抓得到违规、不误杀正确历史。
func TestCheckHistorySynthetic(t *testing.T) {
	// 写者：Put(k,"a")@[100,200]，Append(k,"b")@[300,400]。
	w := []Op{
		{Put, "k", "a", 100, 200},
		{Append, "k", "b", 300, 400},
	}
	cases := []struct {
		name string
		read Op
		want bool
	}{
		{"覆盖 append 的读(重叠)", Op{Get, "k", "ab", 350, 450}, true},
		{"两写之间读旧值(a)", Op{Get, "k", "a", 250, 280}, true},
		{"append 前读空", Op{Get, "k", "", 50, 80}, true},
		{"append 提交后 stale 读旧值(a)", Op{Get, "k", "a", 450, 550}, false}, // 非线一致：应见 "ab"
		{"凭空多出的 append(读到 ab 但区间在全 before)", Op{Get, "k", "ab", 50, 80}, false},
	}
	for _, c := range cases {
		ops := append([]Op{}, w...)
		ops = append(ops, c.read)
		ok, reason := CheckSingleWriterPerKey(History{Ops: ops})
		if ok != c.want {
			t.Fatalf("[%s] 期望 linearizable=%v，实际=%v（%s）", c.name, c.want, ok, reason)
		}
	}

	// Put 覆盖语义 + 多键独立。
	h := History{Ops: []Op{
		{Put, "x", "v1", 100, 200},
		{Put, "x", "v2", 300, 400},
		{Get, "x", "v2", 450, 500}, // 覆盖后读到 v2
		{Get, "y", "", 100, 200},   // 不同键互不影响，y 初值空
	}}
	if ok, reason := CheckSingleWriterPerKey(h); !ok {
		t.Fatalf("多键正确历史应线一致：%s", reason)
	}

	// 金标准正例 / 反例必须分别通过 / 失败。
	if ok, _ := CheckHistory(GoldenCorrect()).OK, false; !ok {
		t.Fatalf("GoldenCorrect 应判为线性一致")
	}
	if ok := CheckHistory(GoldenViolating()).OK; ok {
		t.Fatalf("GoldenViolating 应判为非线性一致")
	}
}
