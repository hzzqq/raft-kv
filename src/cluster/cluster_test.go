// cluster_test.go —— cluster 包冒烟测试
package cluster

import (
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
