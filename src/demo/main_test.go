// main_test.go —— 运行 RunDemo 验证"进程内 KV 路径 + 全栈 HTTP 路径"均正常。
package main

import (
	"strings"
	"testing"
)

func TestRunDemo(t *testing.T) {
	out := RunDemo()

	// 1) 进程内 KV 路径：Put/Get/Append + 跨 group 迁移后数据仍在。
	if !strings.Contains(out, `inproc Put/Get="world"`) {
		t.Fatalf("inproc Put/Get missing: %s", out)
	}
	if !strings.Contains(out, `Append/Get="world!"`) {
		t.Fatalf("inproc Append/Get missing: %s", out)
	}
	if !strings.Contains(out, `after-move Get="world!"`) {
		t.Fatalf("after-move Get missing: %s", out)
	}

	// 2) 全栈 HTTP 路径：经真实 HTTP 读写 + /metrics 快照。
	if !strings.Contains(out, "http put=true") {
		t.Fatalf("http put not ok: %s", out)
	}
	if !strings.Contains(out, `get="dval"`) {
		t.Fatalf("http get dval missing: %s", out)
	}
	if !strings.Contains(out, `append get="dval-http"`) {
		t.Fatalf("http append get missing: %s", out)
	}
	if !strings.Contains(out, "metrics-ok=true") {
		t.Fatalf("http metrics not ok: %s", out)
	}
}
