package main

import (
	"encoding/json"
	"net/http"

	"raftkv/src/linearizability"
)

// handleLinearizability 暴露 GET /debug/linearizability：对一段（嵌入的）操作历史运行
// 单写者每键线性一致检查器，返回结构化结果 {ok, checked, violations, detail}。
//
// 当前网关不记录实时客户端操作历史（shardkv 的 chaos 复现器在测试侧记录），因此端点
// 以嵌入的金标准历史做"集成自检"：先校验正例（证明检查器正确运行），再校验已知反例
// （证明检查器确实会抓违规、非死代码）。两项都通过才认为集成健康；若反例未被抓到
// （检查器实现回归）端点降级返回 500，避免假阳性地报告系统健康。
//
// 复用 I197 的现成检查器（src/linearizability，单写者每键线性一致，构造正确、绝不假阳性）。
func (s *Server) handleLinearizability(w http.ResponseWriter, r *http.Request) {
	// 主检查对象：金标准正例历史。
	res := linearizability.CheckHistory(linearizability.GoldenCorrect())

	detail := res.Detail
	// 集成自检：检查器必须能抓到已知反例，否则视为检查器未真正接入（死代码）。
	bad := linearizability.CheckHistory(linearizability.GoldenViolating())
	if !bad.OK {
		detail += " | integration-check: checker correctly flagged a known-violating history (not dead code)"
	} else {
		// 反例未被抓到 ⇒ 检查器实现回归，端点降级报错。
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"ok":         false,
			"checked":    res.Checked,
			"violations": bad.Violations,
			"detail":     "linearizability checker regression: known-violating history was NOT flagged",
		})
		return
	}

	out := map[string]interface{}{
		"ok":         res.OK,
		"checked":    res.Checked,
		"violations": res.Violations,
		"detail":     detail,
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(out); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
