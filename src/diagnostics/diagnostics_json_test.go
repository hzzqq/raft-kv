package diagnostics

import (
	"encoding/json"
	"testing"
)

// TestDiagnosisLevel 验证健康分到严重度等级的映射阈值。
func TestDiagnosisLevel(t *testing.T) {
	cases := []struct {
		score int
		want  string
	}{
		{100, "ok"},
		{80, "ok"},
		{79, "warn"},
		{50, "warn"},
		{49, "critical"},
		{0, "critical"},
	}
	for _, c := range cases {
		if got := (Diagnosis{Score: c.score}).Level(); got != c.want {
			t.Fatalf("Level(%d) = %q, want %q", c.score, got, c.want)
		}
	}
}

// TestDiagnosisJSON 验证机器可读导出：JSON 含 score/level/issues，且可被反序列化。
func TestDiagnosisJSON(t *testing.T) {
	d := Diagnosis{Score: 42, Issues: []string{"invalid: x", "unbalanced: gap is 2"}}
	b, err := d.JSON()
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}
	var out struct {
		Score  int      `json:"score"`
		Level  string   `json:"level"`
		Issues []string `json:"issues"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Score != 42 || out.Level != "critical" || len(out.Issues) != 2 {
		t.Fatalf("unexpected JSON: %+v", out)
	}
}
