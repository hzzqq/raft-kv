package util

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

// TestErrGroupAllOK 验证：全部成功时 Wait 返回 nil。
func TestErrGroupAllOK(t *testing.T) {
	g := WithErrGroup(context.Background())
	for i := 0; i < 3; i++ {
		g.Go(func(ctx context.Context) error {
			select {
			case <-time.After(10 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("全成功应返回 nil，却返回 %v", err)
	}
}

// TestErrGroupCancel 验证：任一任务出错即取消其余，Wait 返回首个错误。
func TestErrGroupCancel(t *testing.T) {
	g := WithErrGroup(context.Background())
	// 任务 1：很快出错
	g.Go(func(ctx context.Context) error {
		return errBoom
	})
	// 任务 2：监听 ctx，被取消后退出
	g.Go(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if err := g.Wait(); err == nil {
		t.Fatal("应有错误返回")
	} else if !errors.Is(err, errBoom) && err.Error() != "boom" {
		// 首个被记录的错误应为 errBoom（任务 2 返回的是 ctx.Err()，但先到的是 boom）
		t.Fatalf("期望 boom，却返回 %v", err)
	}
}

// TestErrGroupParentCancel 验证：父 ctx 取消时组内任务应感知并退出。
func TestErrGroupParentCancel(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	g := WithErrGroup(parent)
	cancel() // 立即取消父 ctx
	done := make(chan error, 1)
	g.Go(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	go func() { done <- g.Wait() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("父取消后任务应返回错误")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("父取消后任务未退出")
	}
}
