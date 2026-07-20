// client_test.go —— kvcli 客户端单测（对内存集群起的网关发请求）
package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"raftkv/src/cluster"
)

// testHandler 用内存集群起一个与 gateway 语义一致的临时网关，供客户端单测。
func testHandler(c *cluster.Cluster) http.Handler {
	mux := http.NewServeMux()
	ck := c.Clerk()
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
	return mux
}

func TestClientAgainstCluster(t *testing.T) {
	c := cluster.StartCluster(2, 3, 3, 0)
	defer c.Cleanup()
	for g := 0; g < 2; g++ {
		c.Join(g)
		c.WaitConfig(g, 0, g+1)
	}

	ts := httptest.NewServer(testHandler(c))
	defer ts.Close()

	cl := NewClient(ts.URL)
	if err := cl.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	if v, _ := cl.Get("foo"); v != "bar" {
		t.Fatalf("Get(foo)=%q want bar", v)
	}
	if err := cl.Append("foo", "-baz"); err != nil {
		t.Fatal(err)
	}
	if v, _ := cl.Get("foo"); v != "bar-baz" {
		t.Fatalf("Get(foo) after append=%q want bar-baz", v)
	}
}
