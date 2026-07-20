// cluster_test.go —— cluster 包冒烟测试
package cluster

import (
	"fmt"
	"hash/fnv"
	"testing"
	"time"

	"raftkv/src/shardmaster"
)

func key2shard(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % shardmaster.NShards)
}

// TestClusterSmoke：启动 2 group 集群，做基本读写 + 跨 group 迁移后可读。
func TestClusterSmoke(t *testing.T) {
	c := StartCluster(2, 3, 3, 0)
	defer c.Cleanup()

	ck := c.Clerk()
	c.Join(0)
	c.WaitConfig(0, 0, 1)

	ck.Put("k1", "v1")
	if v := ck.Get("k1"); v != "v1" {
		t.Fatalf("after Put got %q want v1", v)
	}
	ck.Append("k1", "x")
	if v := ck.Get("k1"); v != "v1x" {
		t.Fatalf("after Append got %q want v1x", v)
	}

	// 跨 group 迁移：把 k1 所在分片迁到 group1，验证数据随之迁移且可读。
	c.Join(1)
	c.WaitConfig(1, 0, 2)
	shard := key2shard("k1")
	c.Move(shard, 1)
	c.WaitConfig(0, 0, 3)
	c.WaitConfig(1, 0, 3)
	time.Sleep(500 * time.Millisecond)

	if v := ck.Get("k1"); v != "v1x" {
		t.Fatalf("after Move shard %d: got %q want \"v1x\"", shard, v)
	}
}

// TestClusterChurn：2 group 集群下做可控的多 group 分片漂移（Move 式，安全路径），
// 断言配置持续推进到最新且 churn 结束后数据仍完整可读。验证新增的 Churn/WaitAllConfigs
// helper 可在 cluster 包内直接复用。3+ group 整体再平衡（Join/Leave）式 churn 的脆弱性
// 见 docs/lab4-shardkv-design.md §7，不在此用例（安全路径）覆盖。
func TestClusterChurn(t *testing.T) {
	const nGroups = 2
	c := StartCluster(nGroups, 3, 3, 0)
	defer c.Cleanup()
	ck := c.Clerk()
	for g := 0; g < nGroups; g++ {
		c.Join(g)
		c.WaitConfig(g, 0, g+1)
	}
	for i := 0; i < 10; i++ {
		ck.Put(fmt.Sprintf("cc-%d", i), fmt.Sprintf("ccv-%d", i))
	}

	c.Churn(12, 80*time.Millisecond, 1)
	c.WaitAllConfigs(2 + 12)

	for i := 0; i < 10; i++ {
		if v := ck.Get(fmt.Sprintf("cc-%d", i)); v != fmt.Sprintf("ccv-%d", i) {
			t.Fatalf("after churn Get(cc-%d)=%q want ccv-%d", i, v, i)
		}
	}
}
