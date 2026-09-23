package async

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/core"
	"github.com/chichengyu/async/ratelimit"
)

// ============================================================
// PoolBuilder 测试
// ============================================================

func TestPoolBuilder_Basic(t *testing.T) {
	ctx := context.Background()
	p := NewPoolBuilder[int](4)

	p.WithTimeout(5 * time.Second)
	err := p.Submit(ctx, func(ctx context.Context) (int, error) { return 42, nil })
	if err != nil {
		t.Fatalf("submit error: %v", err)
	}
	results := p.Wait()

	if len(results) != 1 || results[0].Value != 42 {
		t.Fatalf("expected [42], got %v", results)
	}
}

func TestPoolBuilder_DefaultPool(t *testing.T) {
	ctx := context.Background()
	p := DefaultPoolBuilder[string]()
	err := p.Submit(ctx, func(ctx context.Context) (string, error) { return "ok", nil })
	if err != nil {
		t.Fatalf("submit error: %v", err)
	}
	results := p.Wait()
	if len(results) != 1 || results[0].Value != "ok" {
		t.Fatalf("expected [ok], got %v", results)
	}
}

func TestPoolBuilder_Error(t *testing.T) {
	ctx := context.Background()
	errTest := errors.New("pool error")

	p := NewPoolBuilder[int](2)
	_ = p.Submit(ctx, func(ctx context.Context) (int, error) { return 0, errTest })
	_ = p.Submit(ctx, func(ctx context.Context) (int, error) { return 42, nil })
	results := p.Wait()

	errFound := false
	for _, r := range results {
		if r.Err != nil {
			errFound = true
		}
	}
	if !errFound {
		t.Fatal("expected an error in results")
	}
}

// ============================================================
// GroupBuilder 测试
// ============================================================

func TestGroupBuilder_Basic(t *testing.T) {
	ctx := context.Background()
	var count atomic.Int64

	g := NewGroupBuilder[int](4)
	g.Go(ctx, func(ctx context.Context) (int, error) { count.Add(1); return 1, nil })
	g.Go(ctx, func(ctx context.Context) (int, error) { count.Add(1); return 2, nil })
	results := g.Wait()

	if count.Load() != 2 {
		t.Fatalf("expected 2 executions, got %d", count.Load())
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestGroupBuilder_DefaultGroup(t *testing.T) {
	ctx := context.Background()
	g := DefaultGroupBuilder[int]()
	g.Go(ctx, func(ctx context.Context) (int, error) { return 100, nil })
	results := g.Wait()
	if len(results) != 1 || results[0].Value != 100 {
		t.Fatalf("expected [100], got %v", results)
	}
}

func TestGroupBuilder_Error(t *testing.T) {
	ctx := context.Background()
	errTest := errors.New("group error")

	g := NewGroupBuilder[int](2)
	g.Go(ctx, func(ctx context.Context) (int, error) { return 0, errTest })
	g.Go(ctx, func(ctx context.Context) (int, error) { return 42, nil })
	results := g.Wait()

	errFound := false
	for _, r := range results {
		if r.Err != nil {
			errFound = true
		}
	}
	if !errFound {
		t.Fatal("expected an error in results")
	}
}

// ============================================================
// ShardPoolBuilder 测试
// ============================================================

func TestShardPoolBuilder_Basic(t *testing.T) {
	ctx := context.Background()
	sp := NewShardPoolBuilder[string](4, 2)

	_ = sp.Submit(ctx, func(ctx context.Context) (string, error) { return "shard-ok", nil })
	results := sp.WaitAndClose()

	if len(results) != 1 || results[0].Value != "shard-ok" {
		t.Fatalf("expected [shard-ok], got %v", results)
	}
}

func TestShardPoolBuilder_DefaultShardPool(t *testing.T) {
	ctx := context.Background()
	sp := DefaultShardPoolBuilder[string]()
	_ = sp.Submit(ctx, func(ctx context.Context) (string, error) { return "default", nil })
	results := sp.WaitAndClose()

	if len(results) != 1 || results[0].Value != "default" {
		t.Fatalf("expected [default], got %v", results)
	}
}

// ============================================================
// ShardGroupBuilder 测试
// ============================================================

func TestShardGroupBuilder_Basic(t *testing.T) {
	ctx := context.Background()
	sg := NewShardGroupBuilder[int](4, 2)

	sg.Go(ctx, func(ctx context.Context) (int, error) { return 10, nil })
	results := sg.Wait()

	if len(results) != 1 || results[0].Value != 10 {
		t.Fatalf("expected [10], got %v", results)
	}
}

func TestShardGroupBuilder_DefaultShardGroup(t *testing.T) {
	ctx := context.Background()
	sg := DefaultShardGroupBuilder[int]()
	sg.Go(ctx, func(ctx context.Context) (int, error) { return 99, nil })
	results := sg.Wait()

	if len(results) != 1 || results[0].Value != 99 {
		t.Fatalf("expected [99], got %v", results)
	}
}

// ============================================================
// PipelineBuilder 测试
// ============================================================

func TestPipelineBuilder_Basic(t *testing.T) {
	ctx := context.Background()

	p := NewPipelineBuilder[int](ctx).
		Add(func(ctx context.Context, n int) (int, error) { return n * 2, nil }).
		Add(func(ctx context.Context, n int) (int, error) { return n + 1, nil })

	result, err := p.Run(5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 11 {
		t.Fatalf("expected (5*2)+1=11, got %d", result)
	}
}

func TestPipelineBuilder_Stages(t *testing.T) {
	ctx := context.Background()

	p := NewPipelineBuilder[string](ctx).
		Add(func(ctx context.Context, s string) (string, error) { return s + "A", nil }).
		Add(func(ctx context.Context, s string) (string, error) { return s + "B", nil })

	if p.Stages() != 2 {
		t.Fatalf("expected 2 stages, got %d", p.Stages())
	}

	result, _ := p.Run("X")
	if result != "XAB" {
		t.Fatalf("expected XAB, got %s", result)
	}
}

func TestPipelineBuilder_Error(t *testing.T) {
	ctx := context.Background()

	p := NewPipelineBuilder[int](ctx).
		Add(func(ctx context.Context, n int) (int, error) { return n * 2, nil }).
		Add(func(ctx context.Context, n int) (int, error) { return 0, errors.New("stage2 failed") })

	_, err := p.Run(5)
	if err == nil {
		t.Fatal("expected error from stage2")
	}
}

func TestPipelineBuilder_Empty(t *testing.T) {
	ctx := context.Background()
	p := NewPipelineBuilder[int](ctx)

	result, err := p.Run(42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 42 {
		t.Fatalf("expected 42 with no stages, got %d", result)
	}
}

// ============================================================
// RateLimitBuilder 测试
// ============================================================

func TestRateLimitBuilder_Basic(t *testing.T) {
	rl := NewRateLimitBuilder(100, time.Second).Build()
	if rl == nil {
		t.Fatal("expected non-nil rate limiter")
	}
	rl.Close()
}

func TestRateLimitBuilder_WithBurst(t *testing.T) {
	rl := NewRateLimitBuilder(100, time.Second).
		WithBurst(200).
		BuildWithBurst()
	if rl == nil {
		t.Fatal("expected non-nil rate limiter")
	}
	rl.Close()
}

func TestRateLimitBuilder_WithStrategy(t *testing.T) {
	rl := NewRateLimitBuilder(100, time.Second).
		WithBurst(200).
		WithStrategy(ratelimit.Reject).
		BuildWithBurst()
	if rl == nil {
		t.Fatal("expected non-nil rate limiter")
	}
	rl.Close()
}

func TestRateLimitBuilder_BuildOnly(t *testing.T) {
	// Build (without burst) should ignore WithBurst
	rl := NewRateLimitBuilder(50, time.Second).
		WithBurst(100).
		Build()
	if rl == nil {
		t.Fatal("expected non-nil rate limiter")
	}
	rl.Close()
}

// ============================================================
// SlidingWindowBuilder 测试
// ============================================================

func TestSlidingWindowBuilder_Basic(t *testing.T) {
	sw := NewSlidingWindowBuilder(10, time.Second).Build()
	if sw == nil {
		t.Fatal("expected non-nil sliding window limiter")
	}
}

func TestSlidingWindowBuilder_Allow(t *testing.T) {
	sw := NewSlidingWindowBuilder(10, time.Second).Build()

	n := 5
	for i := 0; i < n; i++ {
		if !sw.Allow() {
			t.Fatalf("expected allow on request %d", i)
		}
	}
}

// ============================================================
// TokenBucketBuilder 测试
// ============================================================

func TestTokenBucketBuilder_Basic(t *testing.T) {
	tb := NewTokenBucketBuilder(10, 20).Build()
	if tb == nil {
		t.Fatal("expected non-nil token bucket")
	}
}

func TestTokenBucketBuilder_Allow(t *testing.T) {
	tb := NewTokenBucketBuilder(100, 200).Build()
	if !tb.Allow() {
		t.Fatal("expected allow with high capacity")
	}
}

// ============================================================
// AdaptiveRateLimitBuilder 测试
// ============================================================

func TestAdaptiveRateLimitBuilder_Basic(t *testing.T) {
	al := NewAdaptiveRateLimitBuilder(5, 100).Build()
	if al == nil {
		t.Fatal("expected non-nil adaptive rate limiter")
	}
}

func TestAdaptiveRateLimitBuilder_AcquireRelease(t *testing.T) {
	al := NewAdaptiveRateLimitBuilder(10, 100).Build()
	ctx := context.Background()

	err := al.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	al.Release()
}

// ============================================================
// TaskBuilder 测试
// ============================================================

func TestTaskBuilder_Run(t *testing.T) {
	ctx := context.Background()
	ar := NewTaskBuilder[int](ctx).
		Run(func(ctx context.Context) (int, error) { return 42, nil })

	val, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestTaskBuilder_RunWithTimeout(t *testing.T) {
	ctx := context.Background()
	ar := NewTaskBuilder[string](ctx).
		WithTimeout(1 * time.Second).
		Run(func(ctx context.Context) (string, error) { return "timeout-ok", nil })

	val, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "timeout-ok" {
		t.Fatalf("expected timeout-ok, got %s", val)
	}
}

func TestTaskBuilder_RunCancel(t *testing.T) {
	ctx := context.Background()
	task := NewTaskBuilder[int](ctx).
		RunCancel(func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		})

	task.Cancel()
	_, err := task.Result()
	if err == nil {
		t.Fatal("expected error from cancellation")
	}
}

func TestTaskBuilder_RunAction(t *testing.T) {
	ctx := context.Background()
	var called atomic.Bool

	ae := NewTaskBuilder[string](ctx).
		RunAction(func(ctx context.Context) error {
			called.Store(true)
			return nil
		})

	_, err := ae.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called.Load() {
		t.Fatal("action was not called")
	}
}

func TestTaskBuilder_RunActionCancel(t *testing.T) {
	ctx := context.Background()
	te := NewTaskBuilder[int](ctx).
		RunActionCancel(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})

	te.Cancel()
	_, err := te.Result()
	if err == nil {
		t.Fatal("expected error from cancellation")
	}
}

func TestTaskBuilder_RunVoid(t *testing.T) {
	ctx := context.Background()
	var called atomic.Bool

	tv := NewTaskBuilder[string](ctx).
		RunVoid(func(ctx context.Context) {
			called.Store(true)
		})

	tv.Wait()
	if !called.Load() {
		t.Fatal("void task was not called")
	}
}

func TestTaskBuilder_RunVoidWithTimeout(t *testing.T) {
	ctx := context.Background()
	var called atomic.Bool

	tv := NewTaskBuilder[int](ctx).
		WithTimeout(1 * time.Second).
		RunVoid(func(ctx context.Context) {
			called.Store(true)
		})

	tv.Wait()
	if !called.Load() {
		t.Fatal("void task with timeout was not called")
	}
}

func TestTaskBuilder_Error(t *testing.T) {
	ctx := context.Background()
	errTest := errors.New("task error")

	ar := NewTaskBuilder[int](ctx).
		Run(func(ctx context.Context) (int, error) { return 0, errTest })

	_, err := ar.Wait()
	if err == nil || err.Error() != errTest.Error() {
		t.Fatalf("expected %v, got %v", errTest, err)
	}
}

// ============================================================
// BoundedRunnerBuilder 测试
// ============================================================

func TestBoundedRunnerBuilder_Basic(t *testing.T) {
	runner := NewBoundedRunnerBuilder(10).Build()
	if runner == nil {
		t.Fatal("expected non-nil bounded runner")
	}
}

func TestBoundedRunnerBuilder_Go(t *testing.T) {
	ctx := context.Background()
	runner := NewBoundedRunnerBuilder(10).Build()

	ar := BoundedGo(runner, ctx, func(ctx context.Context) (string, error) {
		return fmt.Sprintf("ran"), nil
	})
	if ar == nil {
		t.Fatal("expected non-nil async result")
	}
	val, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "ran" {
		t.Fatalf("expected ran, got %s", val)
	}
}

func TestBoundedRunnerBuilder_Concurrent(t *testing.T) {
	ctx := context.Background()
	runner := NewBoundedRunnerBuilder(2).Build()

	var counter atomic.Int64
	for i := 0; i < 10; i++ {
		_ = BoundedGo(runner, ctx, func(ctx context.Context) (int, error) {
			counter.Add(1)
			return 0, nil
		})
	}

	time.Sleep(200 * time.Millisecond)
	if counter.Load() != 10 {
		t.Fatalf("expected 10 completions, got %d", counter.Load())
	}
}

// ============================================================
// 链式 Action 方法测试（Error 累积模式）
// ============================================================

func TestPoolBuilder_SubmitCh_Chain(t *testing.T) {
	ctx := context.Background()
	p := NewPoolBuilder[int](4)

	results := p.
		SubmitCh(ctx, func(ctx context.Context) (int, error) { return 1, nil }).
		SubmitCh(ctx, func(ctx context.Context) (int, error) { return 2, nil }).
		SubmitCh(ctx, func(ctx context.Context) (int, error) { return 3, nil }).
		Wait()

	if err := p.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
}

func TestPoolBuilder_SubmitCh_ErrorAccumulation(t *testing.T) {
	ctx := context.Background()

	p := NewPoolBuilder[int](4)
	_ = p.
		SubmitCh(ctx, func(ctx context.Context) (int, error) { return 1, nil }).
		SubmitCh(ctx, func(ctx context.Context) (int, error) { return 2, nil }).
		SubmitCh(ctx, func(ctx context.Context) (int, error) { return 3, nil }).
		Wait()

	// Submit 返回 error 仅表示提交失败（如 pool 关闭），不代表任务执行错误
	// 任务执行错误存储在 Result.Err 中
	if p.Error() != nil {
		t.Fatalf("unexpected submit error: %v", p.Error())
	}
	if len(p.Errors()) != 0 {
		t.Fatalf("expected 0 errors, got %d", len(p.Errors()))
	}
}

func TestPoolBuilder_SubmitAtCh(t *testing.T) {
	ctx := context.Background()
	p := NewPoolBuilder[int](4)

	results := p.
		SubmitAtCh(0, ctx, func(ctx context.Context) (int, error) { return 10, nil }).
		SubmitAtCh(2, ctx, func(ctx context.Context) (int, error) { return 30, nil }).
		Wait()

	if err := p.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[0].Value != 10 || results[2].Value != 30 {
		t.Fatalf("expected [10, 0, 30], got %v", results)
	}
}

func TestPoolBuilder_TrySubmitCh(t *testing.T) {
	ctx := context.Background()
	p := NewPoolBuilder[int](1)

	_ = p.
		TrySubmitCh(ctx, func(ctx context.Context) (int, error) { return 42, nil }).
		TrySubmitCh(ctx, func(ctx context.Context) (int, error) { return 99, nil }).
		Wait()

	// TrySubmit may fail if queue is full, but that's OK - error is captured
	_ = p.Error()
}

func TestGroupBuilder_GoCh_Chain(t *testing.T) {
	ctx := context.Background()
	g := NewGroupBuilder[int](4)

	results := g.
		GoCh(ctx, func(ctx context.Context) (int, error) { return 1, nil }).
		GoCh(ctx, func(ctx context.Context) (int, error) { return 2, nil }).
		GoCh(ctx, func(ctx context.Context) (int, error) { return 3, nil }).
		Wait()

	if err := g.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
}

func TestGroupBuilder_GoWithTimeoutCh(t *testing.T) {
	ctx := context.Background()
	g := NewGroupBuilder[int](4)

	results := g.
		GoWithTimeoutCh(ctx, 5*time.Second, func(ctx context.Context) (int, error) { return 42, nil }).
		Wait()

	if err := g.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 || results[0].Value != 42 {
		t.Fatalf("expected [42], got %v", results)
	}
}

func TestGroupBuilder_GoAtCh(t *testing.T) {
	ctx := context.Background()
	g := NewGroupBuilder[int](4)

	results := g.
		GoAtCh(0, ctx, func(ctx context.Context) (int, error) { return 100, nil }).
		GoAtCh(2, ctx, func(ctx context.Context) (int, error) { return 200, nil }).
		Wait()

	if err := g.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[0].Value != 100 || results[2].Value != 200 {
		t.Fatalf("expected [100, 0, 200], got %v", results)
	}
}

func TestShardPoolBuilder_SubmitCh_Chain(t *testing.T) {
	ctx := context.Background()
	sp := NewShardPoolBuilder[int](4, 2)

	sp.
		SubmitCh(ctx, func(ctx context.Context) (int, error) { return 1, nil }).
		SubmitCh(ctx, func(ctx context.Context) (int, error) { return 2, nil })
	results := sp.WaitAndClose()

	if err := sp.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestShardPoolBuilder_SubmitKeyedCh(t *testing.T) {
	ctx := context.Background()
	sp := NewShardPoolBuilder[string](4, 2)

	sp.
		SubmitKeyedCh("user-a", ctx, func(ctx context.Context) (string, error) { return "a", nil }).
		SubmitKeyedCh("user-b", ctx, func(ctx context.Context) (string, error) { return "b", nil })
	results := sp.WaitAndClose()

	if err := sp.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestShardPoolBuilder_SubmitBatchCh(t *testing.T) {
	ctx := context.Background()
	sp := NewShardPoolBuilder[int](4, 2)

	items := []int{1, 2, 3}
	sp.SubmitBatchCh(ctx, items, func(ctx context.Context, n int) (int, error) {
		return n * 10, nil
	})
	results := sp.WaitAndClose()
	batchResults := sp.BatchResults()

	if err := sp.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if len(batchResults) != 3 {
		t.Fatalf("expected 3 batch results, got %d", len(batchResults))
	}
}

func TestShardGroupBuilder_GoCh_Chain(t *testing.T) {
	ctx := context.Background()
	sg := NewShardGroupBuilder[int](4, 2)

	results := sg.
		GoCh(ctx, func(ctx context.Context) (int, error) { return 1, nil }).
		GoCh(ctx, func(ctx context.Context) (int, error) { return 2, nil }).
		Wait()

	if err := sg.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestShardGroupBuilder_GoKeyedCh(t *testing.T) {
	ctx := context.Background()
	sg := NewShardGroupBuilder[string](4, 2)

	results := sg.
		GoKeyedCh("task-1", ctx, func(ctx context.Context) (string, error) { return "ok", nil }).
		Wait()

	if err := sg.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 || results[0].Value != "ok" {
		t.Fatalf("expected [ok], got %v", results)
	}
}

func TestShardGroupBuilder_GoBatchCh(t *testing.T) {
	ctx := context.Background()
	sg := NewShardGroupBuilder[int](4, 2)

	items := []int{1, 2}
	sg.GoBatchCh(ctx, items, func(ctx context.Context, n int) (int, error) {
		return n * 100, nil
	})
	results := sg.Wait()
	batchResults := sg.BatchResults()

	if err := sg.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if len(batchResults) != 2 {
		t.Fatalf("expected 2 batch results, got %d", len(batchResults))
	}
}

func TestMultiPoolBuilder_SubmitCh_Chain(t *testing.T) {
	ctx := context.Background()
	basePool := NewPool[int](4)
	mp := NewMultiPoolBuilder(basePool, 4)

	mp.
		SubmitCh(ctx, func(ctx context.Context) (int, error) { return 8, nil }).
		SubmitCh(ctx, func(ctx context.Context) (int, error) { return 16, nil })
	results := mp.WaitAndClose()

	if err := mp.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestMultiGroupBuilder_GoCh_Chain(t *testing.T) {
	ctx := context.Background()
	baseGroup := NewGroup[int](4)
	mg := NewMultiGroupBuilder(baseGroup, 4)

	results := mg.
		GoCh(ctx, func(ctx context.Context) (int, error) { return 1, nil }).
		GoCh(ctx, func(ctx context.Context) (int, error) { return 2, nil }).
		Wait()

	if err := mg.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestNoResultPoolBuilder_SubmitCh(t *testing.T) {
	ctx := context.Background()
	p := NewNoResultPoolBuilder(4)

	var counter atomic.Int64
	p.
		SubmitCh(ctx, func(ctx context.Context) error { counter.Add(1); return nil }).
		SubmitCh(ctx, func(ctx context.Context) error { counter.Add(1); return nil })
	p.Wait()

	if err := p.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if counter.Load() != 2 {
		t.Fatalf("expected 2 completions, got %d", counter.Load())
	}
}

func TestNoResultGroupBuilder_GoCh(t *testing.T) {
	ctx := context.Background()
	nr := NewNoResultGroupBuilder(4)

	var counter atomic.Int64
	nr.
		GoCh(ctx, func(ctx context.Context) error { counter.Add(1); return nil }).
		GoCh(ctx, func(ctx context.Context) error { counter.Add(1); return nil })
	nr.Wait()

	if err := nr.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if counter.Load() != 2 {
		t.Fatalf("expected 2 completions, got %d", counter.Load())
	}
}

func TestNoResultGroupBuilder_GoAtCh(t *testing.T) {
	ctx := context.Background()
	nr := NewNoResultGroupBuilder(4)

	var counter atomic.Int64
	nr.
		GoAtCh(1, ctx, func(ctx context.Context) error { counter.Add(1); return nil }).
		GoAtCh(3, ctx, func(ctx context.Context) error { counter.Add(1); return nil })
	nr.Wait()

	if err := nr.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if counter.Load() != 2 {
		t.Fatalf("expected 2 completions, got %d", counter.Load())
	}
}

func TestNoResultGroupBuilder_GoWithTimeoutCh(t *testing.T) {
	ctx := context.Background()
	nr := NewNoResultGroupBuilder(4)

	var counter atomic.Int64
	nr.
		GoWithTimeoutCh(ctx, 5*time.Second, func(ctx context.Context) error { counter.Add(1); return nil })
	nr.Wait()

	if err := nr.Error(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if counter.Load() != 1 {
		t.Fatalf("expected 1 completion, got %d", counter.Load())
	}
}

// ============================================================
// Default 构建器测试（零参数工厂函数）
// ============================================================

func TestDefaultPoolBuilder(t *testing.T) {
	ctx := context.Background()
	p := DefaultPoolBuilder[int]()
	err := p.Submit(ctx, func(ctx context.Context) (int, error) { return 42, nil })
	if err != nil {
		t.Fatalf("submit error: %v", err)
	}
	results := p.Wait()
	if len(results) != 1 || results[0].Value != 42 {
		t.Fatalf("expected [42], got %v", results)
	}
}

func TestDefaultAutoScalePoolBuilder(t *testing.T) {
	ctx := context.Background()
	p := DefaultAutoScalePoolBuilder[int]()
	err := p.Submit(ctx, func(ctx context.Context) (int, error) { return 99, nil })
	if err != nil {
		t.Fatalf("submit error: %v", err)
	}
	results := p.Wait()
	if len(results) != 1 || results[0].Value != 99 {
		t.Fatalf("expected [99], got %v", results)
	}
}

func TestDefaultGroupBuilder(t *testing.T) {
	ctx := context.Background()
	g := DefaultGroupBuilder[int]()
	g.Go(ctx, func(ctx context.Context) (int, error) { return 7, nil })
	results := g.Wait()
	if len(results) != 1 || results[0].Value != 7 {
		t.Fatalf("expected [7], got %v", results)
	}
}

func TestDefaultShardPoolBuilder(t *testing.T) {
	ctx := context.Background()
	sp := DefaultShardPoolBuilder[int]()
	sp.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	results := sp.WaitAndClose()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func TestDefaultAutoScaleShardPoolBuilder(t *testing.T) {
	ctx := context.Background()
	sp := DefaultAutoScaleShardPoolBuilder[int]()
	sp.Submit(ctx, func(ctx context.Context) (int, error) { return 1, nil })
	results := sp.WaitAndClose()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func TestDefaultShardGroupBuilder(t *testing.T) {
	ctx := context.Background()
	sg := DefaultShardGroupBuilder[int]()
	sg.Go(ctx, func(ctx context.Context) (int, error) { return 8, nil })
	results := sg.Wait()
	if len(results) != 1 || results[0].Value != 8 {
		t.Fatalf("expected [8], got %v", results)
	}
}

func TestDefaultMultiPoolBuilder(t *testing.T) {
	ctx := context.Background()
	basePool := NewPool[int](4)
	mp := DefaultMultiPoolBuilder(basePool)
	mp.Submit(ctx, func(ctx context.Context) (int, error) { return 16, nil })
	results := mp.WaitAndClose()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func TestDefaultMultiGroupBuilder(t *testing.T) {
	ctx := context.Background()
	baseGroup := NewGroup[int](4)
	mg := DefaultMultiGroupBuilder(baseGroup)
	mg.Go(ctx, func(ctx context.Context) (int, error) { return 16, nil })
	results := mg.Wait()
	if len(results) != 1 || results[0].Value != 16 {
		t.Fatalf("expected [16], got %v", results)
	}
}

func TestDefaultNoResultPoolBuilder(t *testing.T) {
	ctx := context.Background()
	p := DefaultNoResultPoolBuilder()
	var counter atomic.Int64
	fn := func(ctx context.Context) (struct{}, error) {
		counter.Add(1)
		return struct{}{}, nil
	}
	p.Submit(ctx, fn)
	p.Wait()
	if counter.Load() != 1 {
		t.Fatalf("expected 1 completion, got %d", counter.Load())
	}
}

func TestDefaultNoResultGroupBuilder(t *testing.T) {
	ctx := context.Background()
	nr := DefaultNoResultGroupBuilder()
	var counter atomic.Int64
	nr.Go(ctx, func(ctx context.Context) error { counter.Add(1); return nil })
	nr.Wait()
	if counter.Load() != 1 {
		t.Fatalf("expected 1 completion, got %d", counter.Load())
	}
}

func TestDefaultPipelineBuilder(t *testing.T) {
	pb := DefaultPipelineBuilder[int]()
	if pb.Pipeline == nil {
		t.Fatal("expected non-nil pipeline")
	}
}

func TestDefaultRateLimitBuilder(t *testing.T) {
	rl := DefaultRateLimitBuilder().Build()
	if rl == nil {
		t.Fatal("expected non-nil rate limiter")
	}
	rl.Stop()
}

func TestDefaultSlidingWindowBuilder(t *testing.T) {
	sw := DefaultSlidingWindowBuilder().Build()
	if sw == nil {
		t.Fatal("expected non-nil sliding window")
	}
	if !sw.Allow() {
		t.Fatal("expected first Allow to return true")
	}
}

func TestDefaultTokenBucketBuilder(t *testing.T) {
	tb := DefaultTokenBucketBuilder().Build()
	if tb == nil {
		t.Fatal("expected non-nil token bucket")
	}
	if !tb.Allow() {
		t.Fatal("expected first Allow to return true")
	}
}

func TestDefaultAdaptiveRateLimitBuilder(t *testing.T) {
	al := DefaultAdaptiveRateLimitBuilder().Build()
	if al == nil {
		t.Fatal("expected non-nil adaptive rate limiter")
	}
}

func TestDefaultTaskBuilder(t *testing.T) {
	tb := DefaultTaskBuilder[int]()
	ar := tb.Run(func(ctx context.Context) (int, error) { return 42, nil })
	if ar == nil {
		t.Fatal("expected non-nil async result")
	}
	val, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestDefaultBoundedRunnerBuilder(t *testing.T) {
	runner := DefaultBoundedRunnerBuilder().Build()
	if runner == nil {
		t.Fatal("expected non-nil bounded runner")
	}
}

// ============================================================
// 日志注入链式测试
// ============================================================

// testLogger 测试用日志记录器。
type testLogger struct {
	called int
}

func (l *testLogger) Log(ctx context.Context, level LogLevel, msg string, fields ...LogField) {
	l.called++
}
func (l *testLogger) With(fields ...LogField) Logger { return l }
func (l *testLogger) WithContext(ctx context.Context) context.Context {
	return ctx
}

type nopLogger struct{}

func (l *nopLogger) Log(ctx context.Context, level LogLevel, msg string, fields ...LogField) {}
func (l *nopLogger) With(fields ...LogField) Logger                                          { return l }
func (l *nopLogger) WithContext(ctx context.Context) context.Context                         { return ctx }

func TestPoolBuilder_WithLoggerCh(t *testing.T) {
	logger := &testLogger{}
	p := NewPoolBuilder[int](4).WithLoggerCh(logger)
	if p == nil {
		t.Fatal("expected non-nil pool builder")
	}
	current := GetLogger()
	if current == nil {
		t.Fatal("expected non-nil global logger")
	}
	// 注入的 logger 应该已经被设置为全局 logger
	current.Log(context.Background(), core.LevelInfo, "test")
	if logger.called == 0 {
		t.Fatal("logger should have been called")
	}
}

func TestGroupBuilder_WithLoggerCh(t *testing.T) {
	logger := &testLogger{}
	g := NewGroupBuilder[int](8).WithLoggerCh(logger)
	if g == nil {
		t.Fatal("expected non-nil group builder")
	}
}

func TestShardPoolBuilder_WithLoggerCh(t *testing.T) {
	logger := &testLogger{}
	sp := NewShardPoolBuilder[int](8, 4).WithLoggerCh(logger)
	if sp == nil {
		t.Fatal("expected non-nil shard pool builder")
	}
}

func TestRateLimitBuilder_WithLoggerCh(t *testing.T) {
	logger := &testLogger{}
	b := NewRateLimitBuilder(10, time.Second).WithLoggerCh(logger)
	limiter := b.Build()
	if limiter == nil {
		t.Fatal("expected non-nil limiter")
	}
}

func TestSlidingWindowBuilder_WithLoggerCh(t *testing.T) {
	logger := &testLogger{}
	b := NewSlidingWindowBuilder(10, time.Second).WithLoggerCh(logger)
	sw := b.Build()
	if sw == nil {
		t.Fatal("expected non-nil sliding window")
	}
}

func TestTokenBucketBuilder_WithLoggerCh(t *testing.T) {
	logger := &testLogger{}
	b := NewTokenBucketBuilder(10, 20).WithLoggerCh(logger)
	tb := b.Build()
	if tb == nil {
		t.Fatal("expected non-nil token bucket")
	}
}

func TestAdaptiveRateLimitBuilder_WithLoggerCh(t *testing.T) {
	logger := &testLogger{}
	b := NewAdaptiveRateLimitBuilder(5, 100).WithLoggerCh(logger)
	ar := b.Build()
	if ar == nil {
		t.Fatal("expected non-nil adaptive ratelimiter")
	}
}

func TestTaskBuilder_WithLoggerCh(t *testing.T) {
	ctx := context.Background()
	logger := &testLogger{}
	b := NewTaskBuilder[int](ctx).WithLoggerCh(logger)
	ar := b.Run(func(ctx context.Context) (int, error) {
		return 1, nil
	})
	result, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 1 {
		t.Fatalf("expected 1, got %d", result)
	}
}

func TestBoundedRunnerBuilder_WithLoggerCh(t *testing.T) {
	logger := &testLogger{}
	b := NewBoundedRunnerBuilder(10).WithLoggerCh(logger)
	runner := b.Build()
	if runner == nil {
		t.Fatal("expected non-nil runner")
	}
}

func TestPipelineBuilder_WithLoggerCh(t *testing.T) {
	logger := &testLogger{}
	p := DefaultPipelineBuilder[int]().WithLoggerCh(logger)
	result, err := p.
		Add(func(ctx context.Context, n int) (int, error) { return n * 2, nil }).
		Run(10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 20 {
		t.Fatalf("expected 20, got %d", result)
	}
}
