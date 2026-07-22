package util

import (
	"testing"
	"time"
)

func TestSnowIDUniqueIncreasing(t *testing.T) {
	s, err := NewSnowID(1)
	if err != nil {
		t.Fatal(err)
	}
	var prev int64
	seen := make(map[int64]bool)
	for i := 0; i < 1000; i++ {
		id, err := s.NextID()
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if id <= prev {
			t.Fatalf("id not increasing: %d <= %d", id, prev)
		}
		if seen[id] {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = true
		prev = id
	}
}

func TestSnowIDNodeRange(t *testing.T) {
	if _, err := NewSnowID(-1); err == nil {
		t.Fatalf("expected error for negative node")
	}
	if _, err := NewSnowID(1024); err == nil {
		t.Fatalf("expected error for node 1024")
	}
	if _, err := NewSnowID(0); err != nil {
		t.Fatalf("node 0 should be valid: %v", err)
	}
	if _, err := NewSnowID(1023); err != nil {
		t.Fatalf("node 1023 should be valid: %v", err)
	}
}

func TestSnowIDComponentRoundTrip(t *testing.T) {
	s, _ := NewSnowID(7)
	id, err := s.NextID()
	if err != nil {
		t.Fatal(err)
	}
	ts, node, seq := s.Component(id)
	if node != 7 {
		t.Fatalf("node mismatch: %d", node)
	}
	if seq != 0 {
		t.Fatalf("seq mismatch: %d", seq)
	}
	if time.Since(ts) > time.Minute || ts.Unix() < time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix() {
		t.Fatalf("ts out of range: %v", ts)
	}
}

func TestSnowIDClockBackwards(t *testing.T) {
	s, _ := NewSnowID(1)
	// 注入时钟须晚于 epoch(2024-01-01)，否则会被误判为回拨
	base := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	clk := base.UnixMilli()
	s.now = func() time.Time { return time.UnixMilli(clk) }
	if _, err := s.NextID(); err != nil {
		t.Fatal(err)
	}
	// 时钟回拨 5 秒：应报错而非生成倒序 ID
	clk = base.Add(-5 * time.Second).UnixMilli()
	s.now = func() time.Time { return time.UnixMilli(clk) }
	if _, err := s.NextID(); err == nil {
		t.Fatalf("expected clock-backwards error")
	}
}
