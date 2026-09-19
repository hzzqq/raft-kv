// gateway_cluster_bench_test.go —— 网关端到端基准：真实进程内集群 + loopback HTTP。
//
// 与 gateway_bench_test.go 的分工：那边在 cluster-free 层量化网关自身管线开销；
// 这里量化「HTTP 全栈 + 共识」的端到端吞吐（单客户端串行口径），并与
// BenchmarkMemoryMapPut（无共识内存地板）、src/shardkv 的 BenchmarkSKVPutGet
// （shardkv Clerk 直连、无 HTTP 层）构成三层对照，用于定位开销大头。
//
// 口径说明（docs/benchmarks.md 有完整基线与对比方法，数字绑定机器环境，跨机器不可直接比）：
//   - StartCluster(1,3,3,0)：1 个 replica group × 3 副本 + 3 节点 shardmaster，
//     labrpc 内存网络（与全仓单测同构，非真实 TCP 传输层）；
//   - 关闭每客户端限流（单客户端串行压测必打爆令牌桶，非被测对象）；
//   - PUT 保持生产口径（缓存 + ETag 双开，含写失效护栏）；GET 关闭缓存与 ETag
//     以隔离共识读路径（网关缓存命中开销已在 cluster-free 基准量化）。
//
// 运行：go test ./src/gateway/ -run '^$' -bench 'BenchmarkGatewayCluster|BenchmarkMemoryMap' -benchtime 1s
package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"raftkv/src/cluster"
)

// benchClusterServer 构造 1×3 集群 + 生产口径网关（缓存 + ETag + 压缩，与 main.go
// 一致），返回集群/网关/httptest 服务器；清理顺序由调用方 defer（ts 先于 c）。
func benchClusterServer(b *testing.B) (*cluster.Cluster, *Server, *httptest.Server) {
	b.Helper()
	c := cluster.StartCluster(1, 3, 3, 0)
	s := NewServer(c)
	s.Init(1) // 加入唯一 replica group，使分片可写
	s.SetClientRateLimit(0, 0)
	s.SetCache(2*time.Second, 1024) // 与 main.go 生产口径一致
	s.SetETag(true)
	ts := httptest.NewServer(s.Handler())
	return c, s, ts
}

// benchDrain 排干并关闭响应体（连接才可复用；kvcli Del 连接泄漏的教训）。
func benchDrain(resp *http.Response) {
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// benchHTTPPut 发一次 PUT，返回状态码（0 = 传输错误，由调用方 Fatal）。
func benchHTTPPut(client *http.Client, base, path, val string) int {
	req, err := http.NewRequest(http.MethodPut, base+path, strings.NewReader(val))
	if err != nil {
		return 0
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0
	}
	defer benchDrain(resp)
	return resp.StatusCode
}

// benchHTTPGet 发一次 GET，返回状态码（0 = 传输错误，由调用方 Fatal）。
func benchHTTPGet(client *http.Client, base, path string) int {
	resp, err := client.Get(base + path)
	if err != nil {
		return 0
	}
	defer benchDrain(resp)
	return resp.StatusCode
}

// BenchmarkGatewayClusterPut 端到端 PUT 吞吐：loopback HTTP → 网关（含写失效护栏）→
// shardkv → raft 3 副本共识提交 → 应答。ns/op 即单客户端「写达成共识并收到应答」
// 的端到端成本；与 BenchmarkMemoryMapPut 对照即「共识 + 全栈」的放大倍数。
func BenchmarkGatewayClusterPut(b *testing.B) {
	c, _, ts := benchClusterServer(b)
	defer c.Cleanup()
	defer ts.Close()
	client := ts.Client()
	// 预热一次写（计时外）：确保 leader 就绪、HTTP 连接已建，避免把首轮选举抖动计入。
	if code := benchHTTPPut(client, ts.URL, "/kv/warm", "warm"); code != http.StatusOK {
		b.Fatalf("warm-up PUT = %d, want 200", code)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "/kv/bk-" + strconv.Itoa(i%256)
		if code := benchHTTPPut(client, ts.URL, key, "v"); code != http.StatusOK {
			b.Fatalf("PUT %s = %d, want 200", key, code)
		}
	}
}

// BenchmarkGatewayClusterGet 端到端 GET：缓存与 ETag 已关，走完整共识读路径
//（shardkv Get → raft 读守卫线性一致读）+ HTTP 往返。预写 64 个 key（计时外）轮转读。
func BenchmarkGatewayClusterGet(b *testing.B) {
	c, s, ts := benchClusterServer(b)
	defer c.Cleanup()
	defer ts.Close()
	s.SetCacheEnabled(false) // 隔离共识读路径：缓存命中开销已在 cluster-free 基准量化
	s.SetETag(false)
	client := ts.Client()
	const keys = 64
	for i := 0; i < keys; i++ { // 预写数据（计时外）
		if code := benchHTTPPut(client, ts.URL, "/kv/gk-"+strconv.Itoa(i), "v"); code != http.StatusOK {
			b.Fatalf("seed PUT %d = %d, want 200", i, code)
		}
	}
	if code := benchHTTPGet(client, ts.URL, "/kv/gk-0"); code != http.StatusOK {
		b.Fatalf("warm-up GET = %d, want 200", code)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "/kv/gk-" + strconv.Itoa(i%keys)
		if code := benchHTTPGet(client, ts.URL, key); code != http.StatusOK {
			b.Fatalf("GET %s = %d, want 200", key, code)
		}
	}
}

// BenchmarkMemoryMapPut 无共识、无 HTTP 的对照地板：单机内存 map 写（互斥锁保护）。
// 与 BenchmarkGatewayClusterPut 的 ns/op 相除即「共识 + 全栈」放大倍数；与 util 的
// BenchmarkLRUPut 互补：这里是 KV 语义（string→string）直写。
func BenchmarkMemoryMapPut(b *testing.B) {
	var mu sync.Mutex
	m := make(map[string]string)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		m[strconv.Itoa(i%256)] = "v"
		mu.Unlock()
	}
}
