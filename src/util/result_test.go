package util

import (
	"errors"
	"testing"
)

func TestResult(t *testing.T) {
	r := Ok(42)
	if !r.IsOk() || r.Val != 42 {
		t.Fatal("Ok 结果不符")
	}
	if r.Or(0) != 42 {
		t.Fatal("Ok 的 Or 应返回值")
	}
	// 成功 Must 正常
	if r.Must() != 42 {
		t.Fatal("Ok.Must 应返回值")
	}

	e := ErrResult[int](errors.New("boom"))
	if e.IsOk() {
		t.Fatal("错误结果 IsOk 应为 false")
	}
	if e.Or(7) != 7 {
		t.Fatal("错误结果 Or 应返回 fallback")
	}
	// Must 在错误时 panic
	defer func() {
		if recover() == nil {
			t.Fatal("错误结果 Must 应 panic")
		}
	}()
	e.Must()
}
