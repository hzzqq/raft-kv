package shardkv

// ShardHealth 计算单个分片的健康度（0–100，纯函数、cluster-free）：
//   - 当前配置版本与目标版本对齐且无迁移 → 100（健康）
//   - 版本滞后（targetVer-currentVer）：每差 1 扣 10，封顶扣 50
//   - 迁移中（migrating=true）：额外扣 20
//
// 便于在状态页/指标中逐分片标记健康度，定位「版本落后」或「卡在迁移」的异常分片。
// 调用方传入的 currentVer/targetVer 应为同一分片的本地配置号与集群最新配置号。
func ShardHealth(currentVer, targetVer int, migrating bool) (int, string) {
	score := 100
	lag := targetVer - currentVer
	if lag < 0 {
		lag = 0 // 不允许负滞后（异常情况按对齐处理）
	}
	if lag > 0 {
		penalty := lag * 10
		if penalty > 50 {
			penalty = 50
		}
		score -= penalty
	}
	if migrating {
		score -= 20
	}
	if score < 0 {
		score = 0
	}

	var detail string
	switch {
	case migrating:
		detail = "迁移中"
	case lag > 0:
		detail = "配置版本滞后"
	default:
		detail = "健康"
	}
	return score, detail
}
