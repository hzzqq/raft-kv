// demo/main.go —— raft-kv 端到端演示
//
// 在进程内启动一个多 replica group 的 ShardKV 集群（基于可复用的 cluster 包），
// 演示基本 KV 操作以及分片在 group 之间的迁移，适合作为"开箱即跑"的示例。
// 注意：本演示依赖内存 labrpc 网络（与测试同一套），因此集群是进程内的；
// 生产部署需替换为真实网络传输层（gRPC / TCP）。
package main

import (
	"fmt"
	"hash/fnv"
	"time"

	"raftkv/src/cluster"
	"raftkv/src/shardmaster"
)

func key2shard(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % shardmaster.NShards)
}

// RunDemo 启动一个内存集群并跑一遍演示流程，返回结果摘要（便于单测断言）。
func RunDemo() string {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()

	ck := c.Clerk()
	c.Join(0)
	c.WaitConfig(0, 0, 1)
	c.Join(1)
	c.WaitConfig(1, 0, 2)

	ck.Put("hello", "world")
	afterPut := ck.Get("hello")

	ck.Append("hello", "!")
	afterAppend := ck.Get("hello")

	// 跨 group 迁移演示：把 "hello" 所在分片迁到 group1，验证数据随之迁移且可读。
	shard := key2shard("hello")
	c.Move(shard, 1)
	c.WaitConfig(0, 0, 3)
	c.WaitConfig(1, 0, 3)
	time.Sleep(500 * time.Millisecond)
	afterMove := ck.Get("hello")

	return fmt.Sprintf("Put/Get=%q Append/Get=%q after-move Get=%q",
		afterPut, afterAppend, afterMove)
}

func main() {
	fmt.Println("raft-kv demo starting...")
	out := RunDemo()
	fmt.Println("demo result:", out)
	fmt.Println("raft-kv demo done.")
}
