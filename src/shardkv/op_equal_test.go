package shardkv

import (
	"testing"

	"raftkv/src/shardmaster"
)

func TestOpEqual(t *testing.T) {
	base := Op{Kind: "Cmd", ClientId: 7, Seq: 3, Shard: 2, Key: "k", OpType: "Put", Value: "v"}
	// 完全相同的副本（含不同 NotifyId）应判等
	same := base
	same.NotifyId = 999 // NotifyId 不参与比较
	if !OpEqual(base, same) {
		t.Fatal("仅 NotifyId 不同的 op 应判等")
	}
	// 改 Value → 不等
	diffVal := base
	diffVal.Value = "other"
	if OpEqual(base, diffVal) {
		t.Fatal("Value 不同应判不等")
	}
	// 改 Seq → 不等
	diffSeq := base
	diffSeq.Seq = 4
	if OpEqual(base, diffSeq) {
		t.Fatal("Seq 不同应判不等")
	}
	// 配置号不同 → 不等（NewConfig 类身份核心）
	diffCfg := base
	diffCfg.Kind = "NewConfig"
	diffCfg.Config = shardmaster.Config{Num: 2}
	if OpEqual(base, diffCfg) {
		t.Fatal("Config.Num 不同应判不等")
	}
	// 同 Num 不同 Groups（仅比 Num） → 判等（按设计约定）
	sameNum := base
	sameNum.Config = shardmaster.Config{Num: 0, Groups: map[int][]string{1: {"x"}}} // base.Config.Num 也是 0
	if !OpEqual(base, sameNum) {
		t.Fatal("同 Config.Num 应判等（不论 Groups 内容）")
	}
}
