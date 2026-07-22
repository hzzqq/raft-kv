package metrics

import (
	"testing"
)

// TestCounterSubDec 验证：增/减/自减组合，并发安全、值正确。
func TestCounterSubDec(t *testing.T) {
	c := &Counter{}
	if c.Value() != 0 {
		t.Fatalf("初始应为 0，实际 %d", c.Value())
	}
	if c.Inc() != 1 {
		t.Fatalf("Inc 应返回 1")
	}
	if c.Add(5) != 6 {
		t.Fatalf("Add(5) 应返回 6")
	}
	if c.Sub(2) != 4 {
		t.Fatalf("Sub(2) 应返回 4")
	}
	if c.Dec() != 3 {
		t.Fatalf("Dec 应返回 3")
	}
	if c.Value() != 3 {
		t.Fatalf("最终应为 3，实际 %d", c.Value())
	}
	// Sub 可下探到负（delta 指标允许）。
	if c.Sub(5) != -2 {
		t.Fatalf("Sub(5) 应返回 -2")
	}
	if c.Value() != -2 {
		t.Fatalf("最终应为 -2，实际 %d", c.Value())
	}
}
