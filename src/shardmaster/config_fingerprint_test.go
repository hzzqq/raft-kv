package shardmaster

import (
	"testing"
)

func TestConfigFingerprint(t *testing.T) {
	base := &Config{
		Num:    1,
		Shards: [NShards]int{1, 1, 1, 1, 1, 2, 2, 2, 2, 2},
		Groups: map[int][]string{1: {"s1a", "s1b"}, 2: {"s2"}},
	}
	fp := ConfigFingerprint(base)
	if fp == "" || fp == "<nil>" {
		t.Fatalf("有效配置指纹不应为空/<nil>，实际 %q", fp)
	}
	// 相同逻辑配置（server 顺序不同）→ 同一指纹
	reordered := &Config{
		Num:    1,
		Shards: [NShards]int{1, 1, 1, 1, 1, 2, 2, 2, 2, 2},
		Groups: map[int][]string{1: {"s1b", "s1a"}, 2: {"s2"}},
	}
	if ConfigFingerprint(reordered) != fp {
		t.Fatalf("server 顺序不同应产生相同指纹：\n%q\n%q", fp, ConfigFingerprint(reordered))
	}
	// 改一个分片 → 不同指纹
	changed := &Config{
		Num:    1,
		Shards: [NShards]int{2, 1, 1, 1, 1, 2, 2, 2, 2, 2},
		Groups: map[int][]string{1: {"s1a", "s1b"}, 2: {"s2"}},
	}
	if ConfigFingerprint(changed) == fp {
		t.Fatal("分片分布变化应改变指纹")
	}
	// 改配置号 → 不同指纹
	numChg := &Config{
		Num:    2,
		Shards: [NShards]int{1, 1, 1, 1, 1, 2, 2, 2, 2, 2},
		Groups: map[int][]string{1: {"s1a", "s1b"}, 2: {"s2"}},
	}
	if ConfigFingerprint(numChg) == fp {
		t.Fatal("配置号变化应改变指纹")
	}
	if ConfigFingerprint(nil) != "<nil>" {
		t.Fatal("nil 应返回 \"<nil>\"")
	}
}
