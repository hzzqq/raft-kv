package shardmaster

// PlanMove 是 PlanOp 中定向分片迁移的输入（Shard 迁到 Gid）。
type PlanMove struct {
	Shard int
	Gid   int
}

// PlanOp 描述一次拟议的配置变更（与 Join/Leave/Move 三种 RPC 对齐）。
// 字段可组合：Join 与 Leave 可同时给出（先 Join 后 Leave 再 rebalance）；
// Move 为可选的定向分片迁移——一旦设定则跳过自动 rebalance（与 applyOp 的
// Move 分支语义一致，只改一个分片、其余分片映射保持继承）。
type PlanOp struct {
	Join  map[int][]string
	Leave []int
	Move  *PlanMove
}

// PlanResult 是变更预览：
//   - Planned      应用变更 + 再平衡后的目标配置（不写 Raft）；
//   - Errors       目标配置自身的结构合法性问题（孤儿分片 / 空组等），Valid 失败才有；
//   - TransitionErr 相对当前配置的演进合法性问题（配置号不连续 / 结构非法）；
//   - Moves        本次变更涉及的分片属主迁移步骤（供 dry-run 展示迁移代价）。
type PlanResult struct {
	Planned       *Config
	Errors        []string
	TransitionErr string
	Moves         []ShardMove
}

// Plan 在内存中模拟一次配置变更（不触碰 Raft），供管理面 / 迁移预览与安全校验。
// 严格复刻 applyOp 的语义：继承上一版分片映射 → 应用 Join/Leave/Move → Join/Leave
// 触发 rebalance、Move 单分片覆盖 → 用既有 Valid / IsValidTransition 双校验。
// 任何校验失败都不会修改入参 current（纯函数，cluster-free 可直接单测）。
func Plan(current *Config, op PlanOp) PlanResult {
	next := Config{
		Num:    current.Num + 1,
		Groups: copyGroups(current.Groups),
		Shards: current.Shards, // [NShards]int 值类型，赋值即拷贝
	}
	if op.Join != nil {
		for gid, srvs := range op.Join {
			next.Groups[gid] = append([]string{}, srvs...)
		}
	}
	if op.Leave != nil {
		for _, gid := range op.Leave {
			delete(next.Groups, gid)
		}
	}
	if op.Move != nil {
		// Move 分支：只覆盖单个分片，跳过 rebalance（与 applyOp 一致）。
		next.Shards[op.Move.Shard] = op.Move.Gid
	} else {
		rebalance(&next)
	}
	res := PlanResult{Planned: &next}
	if !next.Valid() {
		res.Errors = ValidateConfig(&next)
	}
	if ok, why := current.IsValidTransition(&next); !ok {
		res.TransitionErr = why
	}
	res.Moves = ShardMoves(current, &next)
	return res
}
