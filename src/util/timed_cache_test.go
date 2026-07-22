package util

import (
	"sync"
	"testing"
	"time"
)

func TestTimedCacheExpiry(t *testing.T) {
	c := NewTimedCache()
	c.now = func() time.Time { return time.Unix(1000, 0) }
	c.Set("k", "v", 10*time.Second)
	// 未过期：命中
	c.now = func() time.Time { return time.Unix(1005, 0) }
	if v, ok := c.Get("k"); !ok || v.(string) != "v" {
		t.Fatalf("expected hit, got %v %v", v, ok)
	}
	// 已过期：miss 且顺手清除
	c.now = func() time.Time { return time.Unix(1011, 0) }
	if _, ok := c.Get("k"); ok {
		t.Fatalf("expected expired miss")
	}
	if _, ok := c.Get("k"); ok {
		t.Fatalf("expected entry cleaned on expiry")
	}
}

func TestTimedCacheNoTTL(t *testing.T) {
	c := NewTimedCache()
	c.Set("k", 42, 0)
	if v, ok := c.Get("k"); !ok || v.(int) != 42 {
		t.Fatalf("expected persistent hit, got %v %v", v, ok)
	}
}

func TestTimedCacheDelete(t *testing.T) {
	c := NewTimedCache()
	c.Set("k", "v", time.Hour)
	c.Delete("k")
	if _, ok := c.Get("k"); ok {
		t.Fatalf("expected miss after delete")
	}
}

func TestTimedCacheGC(t *testing.T) {
	c := NewTimedCache()
	c.now = func() time.Time { return time.Unix(1000, 0) }
	c.Set("a", 1, time.Second)
	c.Set("b", 2, time.Second)
	c.Set("c", 3, 0) // 永不过期
	c.now = func() time.Time { return time.Unix(2000, 0) }
	if n := c.GC(); n != 2 {
		t.Fatalf("expected 2 evicted, got %d", n)
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatalf("c should survive")
	}
}

func TestTimedCacheConcurrent(t *testing.T) {
	c := NewTimedCache()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := string(rune('a' + i%10))
			c.Set(key, i, time.Minute)
			c.Get(key)
			c.Delete(key)
			c.GC()
		}(i)
	}
	wg.Wait()
}
