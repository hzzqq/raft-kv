package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// kvStore 是网关桩的内存存储，带互斥锁以支持并发请求。
type kvStore struct {
	mu sync.Mutex
	m  map[string]string
}

func (s *kvStore) get(k string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok
}
func (s *kvStore) put(k, v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[k] = v
}
func (s *kvStore) append(k, v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[k] = s.m[k] + v
}
func (s *kvStore) del(k string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, k)
}

// newStatefulKVServer 起一个带内存存储的网关桩，支持 GET/PUT/POST.../append/DELETE，
// 行为贴近真实网关：GET 缺失 key 返回 404；写操作持久化到内存。返回 server 与
// 存储指针，便于测试预置数据与断言结果。同进程内多个客户端测试共享同一桩。
func newStatefulKVServer(t *testing.T) (*httptest.Server, *kvStore) {
	t.Helper()
	store := &kvStore{m: map[string]string{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/kv/") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		rest := r.URL.Path[len("/kv/"):]
		var key string
		var isAppend bool
		if strings.HasSuffix(rest, "/append") {
			key = rest[:len(rest)-len("/append")]
			isAppend = true
		} else {
			key = rest
		}
		switch r.Method {
		case http.MethodGet:
			v, ok := store.get(key)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(v))
		case http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			store.put(key, string(b))
			w.WriteHeader(http.StatusOK)
		case http.MethodPost:
			if !isAppend {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			b, _ := io.ReadAll(r.Body)
			store.append(key, string(b))
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			store.del(key)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	return srv, store
}
