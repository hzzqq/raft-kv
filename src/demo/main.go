// demo/main.go —— raft-kv 端到端演示
//
// 在进程内启动一个多 replica group 的 ShardKV 集群（基于可复用的 cluster 包），
// 分两段演示：
//  1. 进程内 KV 路径：直接用 Clerk 做 Put/Get/Append + 跨 group 分片迁移；
//  2. 全栈 HTTP 路径：以本进程集群的 Clerk 起一个真正的 HTTP 网关，客户端经
//     HTTP 做 Put/Get/Append，并拉取 /metrics —— 验证 cluster→HTTP→client 全栈。
//
// 适合作为"开箱即跑"的示例。注意：本演示依赖内存 labrpc 网络（与测试同一套），
// 因此集群是进程内的；生产部署需替换为真实网络传输层（gRPC / TCP）。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"raftkv/src/cluster"
	"raftkv/src/metrics"
	"raftkv/src/shardkv"
	"raftkv/src/shardmaster"
	"raftkv/src/util"
)

func key2shard(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % shardmaster.NShards)
}

// demoGatewayHandler 构造一个与 src/gateway 语义一致的 HTTP 网关 handler，
// 直接复用本进程内集群的 Clerk（演示 cluster → HTTP → client 全栈，无需跨进程）。
func demoGatewayHandler(ck *shardkv.Clerk) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, ck.Get(r.PathValue("key")))
	})
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		ck.Put(r.PathValue("key"), string(b))
	})
	mux.HandleFunc("POST /kv/{key}/append", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		ck.Append(r.PathValue("key"), string(b))
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// 可观测性：暴露进程内 ShardKV 的 Metrics 快照。
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(shardkv.Metrics.Snapshot())
	})
	return mux
}

// RunDemo 启动一个内存集群并跑一遍演示流程，返回结果摘要（便于单测断言）。
func RunDemo() string {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	ck := c.Clerk()

	// 定期把指标快照 dump 到 stderr，演示 metrics 的周期性可观测能力（cycle 26）。
	// 从集群启动起就开启，覆盖整段演示，期间会触发多次 dump。
	stopReporter := make(chan struct{})
	metrics.StartPeriodicReporter(shardkv.Metrics, 400*time.Millisecond, os.Stderr, stopReporter)
	defer close(stopReporter)

	c.Join(0)
	c.WaitConfig(0, 0, 1)
	c.Join(1)
	c.WaitConfig(1, 0, 2)

	// ---- 1) 进程内 KV 路径 ----
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

	// ---- 2) 全栈 HTTP 路径 ----
	// 整体超时兜底：集群/网关异常时不让 demo 永久挂起（防御性）。
	demoCtx, demoCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer demoCancel()

	srv := &http.Server{Handler: demoGatewayHandler(ck)}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Sprintf("HTTP demo listen error: %v", err)
	}
	go srv.Serve(ln)
	base := "http://" + ln.Addr().String()
	defer func() { _ = srv.Shutdown(context.Background()) }() // 优雅关闭，等待在途请求完成

	httpClient := &http.Client{Timeout: 5 * time.Second}
	waitHealth(base, httpClient)

	get := func(path string) (*http.Response, error) {
		req, e := http.NewRequestWithContext(demoCtx, http.MethodGet, base+path, nil)
		if e != nil {
			return nil, e
		}
		return httpClient.Do(req)
	}

	putReq, _ := http.NewRequestWithContext(demoCtx, http.MethodPut, base+"/kv/dkey", strings.NewReader("dval"))
	putResp, _ := httpClient.Do(putReq)
	putOK := putResp != nil && putResp.StatusCode == http.StatusOK
	if putResp != nil {
		putResp.Body.Close()
	}
	getResp, _ := get(base + "/kv/dkey")
	dval := ""
	if getResp != nil {
		b, _ := io.ReadAll(getResp.Body)
		dval = string(b)
		getResp.Body.Close()
	}
	appReq, _ := http.NewRequestWithContext(demoCtx, http.MethodPost, base+"/kv/dkey/append", strings.NewReader("-http"))
	appResp, _ := httpClient.Do(appReq)
	if appResp != nil {
		appResp.Body.Close()
	}
	getResp2, _ := get(base + "/kv/dkey")
	dval2 := ""
	if getResp2 != nil {
		b, _ := io.ReadAll(getResp2.Body)
		dval2 = string(b)
		getResp2.Body.Close()
	}
	metricsBody := ""
	mResp, _ := get(base + "/metrics")
	if mResp != nil {
		b, _ := io.ReadAll(mResp.Body)
		metricsBody = string(b)
		mResp.Body.Close()
	}
	var metricsJSON map[string]interface{}
	metricsOK := json.Unmarshal([]byte(metricsBody), &metricsJSON) == nil &&
		metricsJSON["counters"] != nil

	return fmt.Sprintf(
		"inproc Put/Get=%q Append/Get=%q after-move Get=%q | "+
			"http put=%v get=%q append get=%q metrics-ok=%v",
		afterPut, afterAppend, afterMove,
		putOK, dval, dval2, metricsOK,
	)
}

// waitHealth 轮询 /healthz 直到返回 200（整体上限 ~4s），避免 Serve 尚未开始接受连接。
// 采用指数退避（util.Backoff），网关/集群暂时不可达时不会以恒定高频空打，也不会无限阻塞。
func waitHealth(base string, client *http.Client) {
	deadline := time.Now().Add(4 * time.Second)
	for attempt := 1; ; attempt++ {
		if r, err := client.Get(base + "/healthz"); err == nil {
			code := r.StatusCode
			r.Body.Close()
			if code == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(util.Backoff(50*time.Millisecond, 500*time.Millisecond, attempt, 0.2))
	}
}

func main() {
	fmt.Println("raft-kv demo starting...")
	out := RunDemo()
	fmt.Println("demo result:", out)
	fmt.Println("raft-kv demo done.")
}
