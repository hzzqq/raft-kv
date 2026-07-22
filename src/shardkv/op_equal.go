package shardkv

// OpEqual 判断两个 op 是否表示「同一个逻辑命令」，用于 Clerk 幂等去重、apply 去重、
// 以及测试断言「同一请求不会被执行两次」。比较维度：命令种类 Kind、客户端标识
// ClientId、序列号 Seq、分片 Shard、Key、OpType、Value，以及配置号 Config.Num
// （对 NewConfig 类 op，配置号是命令身份的核心；仅比 Num 而非深比较 Groups map，
// 同 Num 即视为同一配置命令，实践中足够）。不比较 NotifyId（每次 Start 重新分配，
// 不属于命令身份）。
func OpEqual(a, b Op) bool {
	return a.Kind == b.Kind &&
		a.ClientId == b.ClientId &&
		a.Seq == b.Seq &&
		a.Shard == b.Shard &&
		a.Key == b.Key &&
		a.OpType == b.OpType &&
		a.Value == b.Value &&
		a.Config.Num == b.Config.Num
}
