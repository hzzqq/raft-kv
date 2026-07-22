package shardkv

import (
	"strings"
	"testing"
)

func TestOpValid(t *testing.T) {
	// 合法 Get
	if probs := OpValid(Op{OpType: "Get", Key: "k"}); len(probs) != 0 {
		t.Fatalf("合法 Get 应通过，实际 %v", probs)
	}
	// 合法 Put
	if probs := OpValid(Op{OpType: "Put", Key: "k", Value: "v"}); len(probs) != 0 {
		t.Fatalf("合法 Put 应通过，实际 %v", probs)
	}
	// 合法 Append
	if probs := OpValid(Op{OpType: "Append", Key: "k", Value: "v"}); len(probs) != 0 {
		t.Fatalf("合法 Append 应通过，实际 %v", probs)
	}
	// 空 Key
	if probs := OpValid(Op{OpType: "Get", Key: ""}); !containsStr(probs, "key empty") {
		t.Fatalf("期望空 Key 报错，实际 %v", probs)
	}
	// 未知 OpType
	if probs := OpValid(Op{OpType: "Delete", Key: "k"}); !containsStr(probs, "unknown op type") {
		t.Fatalf("期望未知 OpType 报错，实际 %v", probs)
	}
	// 超大 Value
	big := make([]byte, MaxValueLen+1)
	if probs := OpValid(Op{OpType: "Put", Key: "k", Value: string(big)}); !containsStr(probs, "value too large") {
		t.Fatalf("期望超大 Value 报错，实际 %v", probs)
	}
	// 多问题：空 Key + 未知 OpType
	multi := OpValid(Op{OpType: "X", Key: ""})
	if len(multi) != 2 {
		t.Fatalf("期望 2 个问题，实际 %v", multi)
	}
}

func containsStr(ss []string, sub string) bool {
	for _, s := range ss {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
