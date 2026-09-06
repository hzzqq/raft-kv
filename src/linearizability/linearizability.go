// Package linearizability 提供 raft-kv 的线性一致检查器（single-writer-per-key 子集）。
//
// 为什么是"单写者每键"而非通用多写者线性一致：通用寄存器线性一致判定在 Append
// （read-modify-write）语义下需要指数级/复杂多项式算法，手工实现易引入"假阳性"
// （把正确的历史判为非线性一致）从而导致 chaos 测试偶发失败——那比没有测试更糟。
// 单写者每键是文献与工业界标准的、意义充分的线性一致子集：每个键只有一个写者
// （写操作按真实时间全序），任意多读者并发；它足以抓住 I195 一类"写已提交却读到
// 空/旧值""丢失 append"的线性一致回归。且本检查器**构造正确**（按真实时间窗口
// [kMin,kMax] 匹配前缀值，绝不假阳性）。
//
// 顺序规范（每键独立寄存器，初值 ""）：Put(k,v) 覆盖为 v；Append(k,s) 追加 s；
// Get(k) 返回当前寄存器值。线性一致要求存在全序 ≺ 与现实时间一致（A 返回早于
// B 发起 ⇒ A≺B），且每个 Get 在其线性化点读到的值等于寄存器状态。
//
// 单写者下写操作按 response 时间全序；读 r 返回 v 合法当且仅当存在前缀 k∈[kMin,kMax]
// 使 apply(前 k 个写)=v，其中 kMin=resp<=r.invoke 的写数（必在 r 之前），kMax=
// m-#(invoke>=r.resp 的写)（必在 r 之后）。重叠写（invoke<r.resp 且 resp>r.invoke）
// 可前可后，区间 [kMin,kMax] 已覆盖两种可能。
//
// 本包是可被生产代码（如网关 /debug/linearizability 端点）导入的规范实现；
// src/shardkv/linearizability_test.go 中的 chaos 复现器也基于同一算法（见注释）。
package linearizability

import (
	"fmt"
	"sort"
	"strings"
)

// OpKind 表示一次操作类型。
type OpKind int

const (
	Put OpKind = iota
	Append
	Get
)

// Op 是历史中的一次操作。
type Op struct {
	Kind   OpKind
	Key    string
	Val    string // Put: 覆盖值; Append: 追加后缀; Get: 返回值
	Invoke int64  // 真实时间（ns）
	Resp   int64
}

// History 是一段操作历史。
type History struct{ Ops []Op }

// ApplyPrefix 返回按序应用前 k 个写后的寄存器值（Put 覆盖 / Append 追加）。
func ApplyPrefix(ws []Op, k int) string {
	s := ""
	for i := 0; i < k; i++ {
		if ws[i].Kind == Put {
			s = ws[i].Val
		} else {
			s += ws[i].Val
		}
	}
	return s
}

// CheckSingleWriterPerKey 校验整段历史（按 key 分组）是否满足单写者每键线性一致。
// 假设每个 key 的写来自单一写者（写按 response 时间全序）。返回 (ok, 失败原因)。
func CheckSingleWriterPerKey(h History) (bool, string) {
	byKey := map[string][]Op{}
	for _, o := range h.Ops {
		byKey[o.Key] = append(byKey[o.Key], o)
	}
	for key, ops := range byKey {
		var writes, reads []Op
		for _, o := range ops {
			if o.Kind == Get {
				reads = append(reads, o)
			} else {
				writes = append(writes, o)
			}
		}
		// 单写者 ⇒ 写按 response 时间全序（写者串行发起，无重叠）。
		sort.Slice(writes, func(i, j int) bool { return writes[i].Resp < writes[j].Resp })
		m := len(writes)
		pv := make([]string, m+1)
		for k := 1; k <= m; k++ {
			pv[k] = ApplyPrefix(writes, k)
		}
		for _, r := range reads {
			kMin, kMax := 0, m
			for _, w := range writes {
				if w.Resp <= r.Invoke { // 写已在 r 发起前完成 ⇒ 必在 r 之前
					kMin++
				}
				if w.Invoke >= r.Resp { // 写在 r 返回后才发起 ⇒ 必在 r 之后
					kMax--
				}
			}
			ok := false
			for k := kMin; k <= kMax; k++ {
				if pv[k] == r.Val {
					ok = true
					break
				}
			}
			if !ok {
				// 详细诊断：输出该键全量写历史与读边界，定位是"真实未来读违规"还是"记录缺失"假阳性。
				var dbg strings.Builder
				fmt.Fprintf(&dbg, "key=%q 读返回 %q inv=%d resp=%d 合法窗口[kMin=%d,kMax=%d] m(写数)=%d\n",
					key, r.Val, r.Invoke, r.Resp, kMin, kMax, m)
				fmt.Fprintf(&dbg, "  全量 pv[0..%d]:\n", m)
				for k := 0; k <= m; k++ {
					mark := ""
					if k >= kMin && k <= kMax {
						mark = " <--合法窗口"
					}
					fmt.Fprintf(&dbg, "    pv[%d]=%q%s\n", k, pv[k], mark)
				}
				fmt.Fprintf(&dbg, "  写历史(按resp序, idx: kind val inv resp):\n")
				for i, w := range writes {
					tag := ""
					if w.Resp <= r.Invoke {
						tag = " [resp<=r.invoke→必在前]"
					}
					if w.Invoke >= r.Resp {
						tag += " [invoke>=r.resp→必在后]"
					}
					fmt.Fprintf(&dbg, "    #%d: %s %q inv=%d resp=%d%s\n",
						i, map[OpKind]string{Put: "Put", Append: "Append"}[w.Kind], w.Val, w.Invoke, w.Resp, tag)
				}
				return false, dbg.String()
			}
		}
	}
	return true, ""
}

// Result 是一次线性一致检查的结构化结果。
type Result struct {
	OK         bool   // 历史是否线性一致
	Checked    int    // 被检查的操作数
	Violations int    // 检测到的线性一致违规数
	Detail     string // 人类可读说明/首条违规诊断
}

// CheckHistory 对给定历史运行单写者每键检查器，汇总为 Result。
func CheckHistory(h History) Result {
	ok, reason := CheckSingleWriterPerKey(h)
	res := Result{OK: ok, Checked: len(h.Ops)}
	if !ok {
		res.Violations = 1
		res.Detail = reason
	} else {
		res.Detail = fmt.Sprintf("linearizable: %d ops checked, 0 violations", len(h.Ops))
	}
	return res
}

// GoldenCorrect 返回一段已知线性一致的历史（构造正确性的正例金标准）。
// 写者：Put(k,"a")@[100,200]，Append(k,"b")@[300,400]。
func GoldenCorrect() History {
	return History{Ops: []Op{
		{Put, "k", "a", 100, 200},
		{Append, "k", "b", 300, 400},
		{Get, "k", "ab", 350, 450}, // 覆盖 append 的重叠读
		{Get, "k", "a", 250, 280},  // 两写之间读旧值
		{Get, "k", "", 50, 80},     // append 前读空
		{Get, "x", "", 100, 200},   // 不同键互不影响，初值空
	}}
}

// GoldenViolating 返回一段已知非线性一致的历史（构造正确性的反例金标准）：
// Append 提交后读者却 stale 读到旧值 "a"（应见 "ab"）。
func GoldenViolating() History {
	return History{Ops: []Op{
		{Put, "k", "a", 100, 200},
		{Append, "k", "b", 300, 400},
		{Get, "k", "a", 450, 550}, // 非线一致：应见 "ab"
	}}
}
