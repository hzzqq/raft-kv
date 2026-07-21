package shardkv

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// leaderOf 返回第 g 组当前 Raft leader 副本下标；若暂未选出 leader 返回 -1。
// rf 字段在同包内可见，GetState 自带锁，可安全并发读取。
func (cfg *skvConfig) leaderOf(g int) int {
	for r := 0; r < cfg.nReplicas; r++ {
		if _, isLeader := cfg.groups[g][r].rf.GetState(); isLeader {
			return r
		}
	}
	return -1
}

// mustWaitGroupConfig 与 waitGroupConfig 类似，但在超时后 真正判定失败
// （waitGroupConfig 超时仅静默返回，无法暴露"配置冻结"）。用于混沌测试中
// 明确断言"迁移进行中杀主"后配置仍能推进，不残留孤儿 incoming / pendingIn。
func (cfg *skvConfig) mustWaitGroupConfig(g, r, num int, d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cfg.groupConfigNum(g, r) >= num {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	cfg.t.Fatalf("group %d replica %d config stuck at %d, want >= %d (freeze under leader-kill?)\n  leader=%d\n  states: %v",
		g, r, cfg.groupConfigNum(g, r), num, cfg.leaderOf(g),
		[]string{
			cfg.groups[g][0].debugState(),
			cfg.groups[g][1].debugState(),
			cfg.groups[g][2].debugState(),
		})
}

// I16：迁移进行中反复杀掉源组/目的组 leader，验证系统最终仍能收敛，
// 数据不丢、配置推进不冻结（无孤儿 incoming / 无 pendingIn 死锁）。
//
// 采用与 TestSKVConfigProgress 相同的"单分片 Move"可控 churn 路径——该路径
// 已被既有测试守住（3+ group 整体再平衡的脆弱性在 docs 已知风险中记录、此处刻意
// 避开）。叠加"迁移中杀主"后，本测试专门守护 cycle 48 根因（incoming 孤儿 /
// pendingIn 残留）在 leader 崩溃 + 重新选举场景下不被重新触发。
//
// 判定逻辑：每轮把某分片在两组间来回 Move（制造持续迁移流），并在迁移窗口内
// 杀掉两组当前 leader；随后 mustWaitGroupConfig 断言配置推进到最新——若某次迁移
// 因杀主而残留孤儿分片 / pendingIn，配置将无法推进到 want，mustWaitGroupConfig
// 超时即判失败（明确的"迁移冻结"回归信号）。最后校验 churn 期间写入的数据仍完整可读。
func TestChaosLeaderKillDuringMigration(t *testing.T) {
	const nGroups = 2
	cfg := makeSKVConfig(t, nGroups, 3, 3, 0)
	defer cfg.cleanup()
	ck := cfg.makeClerk()
	cfg.joinGroup(0)
	cfg.joinGroup(1)
	cfg.waitGroupConfig(1, 0, 2)

	// 预先写入数据（分散到多个分片），记录期望值用于收尾校验。
	const nKeys = 12
	expected := map[string]string{}
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("ck-%d", i)
		v := fmt.Sprintf("ckv-%d", i)
		ck.Put(k, v)
		expected[k] = v
	}

	const rounds = 8
	hardDeadline := time.After(160 * time.Second)
	for i := 0; i < rounds; i++ {
		select {
		case <-hardDeadline:
			t.Fatalf("chaos I16 hard deadline reached at round %d/%d (possible live-lock)", i, rounds)
		default:
		}
		shard := i % NShards
		// 把该分片在两组间来回移动，制造持续迁移流。
		cfg.moveShard(shard, i%2)
		// 迁移进行中杀掉两组当前 leader，迫使重新选举并继续未完成的迁移。
		for g := 0; g < nGroups; g++ {
			if lr := cfg.leaderOf(g); lr >= 0 {
				cfg.restartReplica(g, lr)
			}
		}
		want := 3 + i // 初始 config=2 + 每轮一次 Move
		for g := 0; g < nGroups; g++ {
			cfg.mustWaitGroupConfig(g, 0, want, 40*time.Second)
		}
		t.Logf("chaos I16 round %d/%d ok (config=%d)", i+1, rounds, want)
	}

	// churn 结束后数据仍完整可读且正确（无冻结 / 无丢更新 / 无陈旧读）。
	for k, want := range expected {
		if v := ck.Get(k); v != want {
			t.Fatalf("after chaos Get(%q)=%q want %q", k, v, want)
		}
	}

	// 额外守护：配置稳定后迁移中间态必须全部清空，否则即便未冻结也已破坏一致性
	// （孤儿 pendingIn/incoming 会在后续配置下指向错误 owner）。
	cfg.assertNoMigrationOrphans(t)
}

// assertNoMigrationOrphans 在 churn 结束后轮询一小段时间，确认所有副本的
// pendingIn/pendingOut/incoming 都归零。若残留则判失败——这是比"配置冻结"更隐蔽的
// 状态机泄漏信号（迁移卡在半途但不阻断配置推进）。
func (cfg *skvConfig) assertNoMigrationOrphans(t *testing.T) {
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		clean := true
		for g := 0; g < cfg.nGroups; g++ {
			for r := 0; r < cfg.nReplicas; r++ {
				pin, pout, inc := cfg.groups[g][r].orphanCounts()
				if pin != 0 || pout != 0 || inc != 0 {
					clean = false
				}
			}
		}
		if clean {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	for g := 0; g < cfg.nGroups; g++ {
		for r := 0; r < cfg.nReplicas; r++ {
			pin, pout, inc := cfg.groups[g][r].orphanCounts()
			if pin != 0 || pout != 0 || inc != 0 {
				t.Fatalf("group %d replica %d has migration orphans after churn: pendingIn=%d pendingOut=%d incoming=%d",
					g, r, pin, pout, inc)
			}
		}
	}
}

// LeaderKillRecover 最小复现：多次（无配置 churn）杀主后应在每轮数秒内重新选出
// leader。用于区分"产品反复崩溃恢复 bug"与"chaos 测试杀主+配置 churn 组合"问题。
func TestLeaderKillRecover(t *testing.T) {
	const nGroups = 1
	const kills = 6
	cfg := makeSKVConfig(t, nGroups, 3, 3, 0)
	defer cfg.cleanup()
	cfg.joinGroup(0)
	cfg.waitGroupConfig(0, 0, 1)
	ck := cfg.makeClerk()
	ck.Put("k", "v") // 写点数据，验证恢复后可服务

	for n := 0; n < kills; n++ {
		if lr := cfg.leaderOf(0); lr >= 0 {
			cfg.restartReplica(0, lr)
		}
		// 等新一轮 leader 选出
		deadline := time.Now().Add(12 * time.Second)
		got := -1
		for time.Now().Before(deadline) {
			if l := cfg.leaderOf(0); l >= 0 {
				got = l
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if got < 0 {
			t.Fatalf("no leader after kill #%d/%d (product crash-recovery bug?)\n  states: %v",
				n+1, kills,
				[]string{cfg.groups[0][0].debugState(), cfg.groups[0][1].debugState(), cfg.groups[0][2].debugState()})
		}
		// 恢复后数据仍可服务
		if v := ck.Get("k"); v != "v" {
			t.Fatalf("after kill #%d data lost: Get(k)=%q want v", n+1, v)
		}
		t.Logf("kill #%d/%d recovered, leader=%d", n+1, kills, got)
	}
}

// I18-本地变体：更长时间、更多轮的"迁移中杀主"混沌，作为 CI chaos 长时 job 的
// 主体用例（相比 I16 的 8 轮，这里跑 20 轮并穿插并发纯读，放大崩溃-重选窗口）。
func TestChaosLongRun(t *testing.T) {
	const nGroups = 2
	cfg := makeSKVConfig(t, nGroups, 3, 3, 0)
	defer cfg.cleanup()
	ck := cfg.makeClerk()
	cfg.joinGroup(0)
	cfg.joinGroup(1)
	cfg.waitGroupConfig(1, 0, 2)

	const nKeys = 16
	expected := sync.Map{}
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("lk-%d", i)
		v := fmt.Sprintf("lkv-%d", i)
		ck.Put(k, v)
		expected.Store(k, v)
	}

	// 后台并发纯读，制造 ReadIndex 快读与迁移/杀主的并发压力。
	done := make(chan struct{})
	var readers sync.WaitGroup
	for c := 0; c < 2; c++ {
		readers.Add(1)
		go func(c int) {
			defer readers.Done()
			localCk := cfg.makeClerk()
			for {
				select {
				case <-done:
					return
				default:
					for i := 0; i < nKeys; i++ {
						localCk.Get(fmt.Sprintf("lk-%d", i))
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(c)
	}

	const rounds = 20
	hardDeadline := time.After(380 * time.Second)
	for i := 0; i < rounds; i++ {
		select {
		case <-hardDeadline:
			close(done)
			readers.Wait()
			t.Fatalf("chaos I18 hard deadline reached at round %d/%d (possible live-lock)", i, rounds)
		default:
		}
		shard := (i * 3) % NShards
		cfg.moveShard(shard, i%2)
		for g := 0; g < nGroups; g++ {
			if lr := cfg.leaderOf(g); lr >= 0 {
				cfg.restartReplica(g, lr)
			}
		}
		want := 3 + i
		for g := 0; g < nGroups; g++ {
			cfg.mustWaitGroupConfig(g, 0, want, 60*time.Second)
		}
		t.Logf("chaos I18 round %d/%d ok (config=%d)", i+1, rounds, want)
	}
	close(done)
	readers.Wait()

	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("lk-%d", i)
		want, _ := expected.Load(k)
		if v := ck.Get(k); v != want.(string) {
			t.Fatalf("after long chaos Get(%q)=%q want %q", k, v, want)
		}
	}

	// 配置稳定后迁移中间态必须归零（同 TestChaosLeaderKillDuringMigration 的守护）。
	cfg.assertNoMigrationOrphans(t)
}
