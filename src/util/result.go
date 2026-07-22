package util

// Result 是「值或错误」的泛型容器（Rust 风格 Either 的简化），用于把
// (T, error) 二元结果当单个值传递——在并发任务收集（errgroup 的泛型版）、
// 管道中间态、以及「先攒结果再统一处理错误」的场景里，避免到处写
// `var mu sync.Mutex; mu.Lock(); results[i]=...; mu.Unlock()` 样板。
type Result[T any] struct {
	Val T
	Err error
}

// Ok 构造一个成功结果。
func Ok[T any](v T) Result[T] {
	return Result[T]{Val: v}
}

// ErrResult 构造一个失败结果。
func ErrResult[T any](e error) Result[T] {
	return Result[T]{Err: e}
}

// IsOk 表示无错误。
func (r Result[T]) IsOk() bool {
	return r.Err == nil
}

// Must 返回值，若含错误则 panic。适合「已在外层保证成功」的断言式取值。
func (r Result[T]) Must() T {
	if r.Err != nil {
		panic(r.Err)
	}
	return r.Val
}

// Or 返回值，若含错误则返回 fallback。便于降级默认值。
func (r Result[T]) Or(fallback T) T {
	if r.Err != nil {
		return fallback
	}
	return r.Val
}
