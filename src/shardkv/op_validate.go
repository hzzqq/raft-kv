package shardkv

// MaxValueLen 是单个 KV 值允许的最大字节数（1MB），防止异常大值压垮状态机/快照。
const MaxValueLen = 1 << 20

// OpValid 校验单个 Op 的字段合法性，返回问题列表（空=合法）。cluster-free 纯函数，
// 用于请求入口护栏：非法 Op 不应进入 Raft 日志。检查维度：
//   - OpType 须为 Get/Put/Append 之一
//   - Key 不可为空
//   - Value 长度不超过 MaxValueLen（仅对写入类 Put/Append 有意义，但统一检查无害）
func OpValid(op Op) []string {
	var problems []string
	switch op.OpType {
	case "Get", "Put", "Append":
		// 合法类型
	default:
		problems = append(problems, "unknown op type: "+op.OpType)
	}
	if op.Key == "" {
		problems = append(problems, "key empty")
	}
	if len(op.Value) > MaxValueLen {
		problems = append(problems, "value too large")
	}
	return problems
}
