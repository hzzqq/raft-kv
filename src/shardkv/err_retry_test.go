package shardkv

import "testing"

func TestIsRetryableErr(t *testing.T) {
	retryable := []Err{ErrWrongLeader, ErrTimeout}
	for _, e := range retryable {
		if !IsRetryableErr(e) {
			t.Fatalf("Err=%q 应可重试", e)
		}
	}
	notRetryable := []Err{OK, ErrWrongGroup, Err(""), Err("boom")}
	for _, e := range notRetryable {
		if IsRetryableErr(e) {
			t.Fatalf("Err=%q 不应可重试", e)
		}
	}
}
