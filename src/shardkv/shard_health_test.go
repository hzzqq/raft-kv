package shardkv

import "testing"

func TestShardHealth(t *testing.T) {
	cases := []struct {
		cur, target int
		migrating   bool
		wantScore   int
		wantDetail  string
	}{
		{5, 5, false, 100, "健康"},
		{3, 5, false, 80, "配置版本滞后"},  // lag=2 扣 20
		{0, 5, false, 50, "配置版本滞后"},  // lag=5 扣 50（封顶）
		{0, 10, false, 50, "配置版本滞后"}, // lag=10 仍封顶 50
		{5, 5, true, 80, "迁移中"},      // 健康-20
		{3, 5, true, 60, "迁移中"},      // 滞后20+迁移20
	}
	for _, c := range cases {
		score, detail := ShardHealth(c.cur, c.target, c.migrating)
		if score != c.wantScore {
			t.Fatalf("ShardHealth(%d,%d,%v) score=%d 期望 %d", c.cur, c.target, c.migrating, score, c.wantScore)
		}
		if detail != c.wantDetail {
			t.Fatalf("ShardHealth(%d,%d,%v) detail=%q 期望 %q", c.cur, c.target, c.migrating, detail, c.wantDetail)
		}
	}
}

// TestShardHealthNegativeLag 验证：异常负滞后按对齐处理（不放大惩罚）。
func TestShardHealthNegativeLag(t *testing.T) {
	score, detail := ShardHealth(10, 5, false) // cur>target，异常
	if score != 100 {
		t.Fatalf("负滞后应视为对齐 score=100，实际 %d", score)
	}
	if detail != "健康" {
		t.Fatalf("负滞后 detail 应为 健康，实际 %q", detail)
	}
}
