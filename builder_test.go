package async

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/core"
)

// =========== MapBuilder ============

func TestMapBuilder_BasicRun(t *testing.T) {
	ctx := context.Background()
	results := NewMapBuilder[int, string](ctx, []int{1, 2, 3}).
		WithConcurrency(4).
		Run(func(ctx context.Context, n int) (string, error) {
			return fmt.Sprintf("x%d", n), nil
		})

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		if !r.Ok() {
			t.Fatalf("result %d has error: %v", i, r.Err)
		}
	}
}

func TestMapBuilder_WithFailFast(t *testing.T) {
	ctx := context.Background()
	errTest := errors.New("fail on 3")
	results, err := NewMapBuilder[int, string](ctx, []int{1, 2, 3, 4, 5}).
		WithConcurrency(4).
		WithFailFast().
		RunWithError(func(ctx context.Context, n int) (string, error) {
			if n == 3 {
				return "", errTest
			}
			return fmt.Sprintf("x%d", n), nil
		})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	_ = results
}

func TestMapBuilder_WithTimeout(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	results := NewMapBuilder[int, string](ctx, []int{1, 2, 3}).
		WithConcurrency(3).
		WithTimeout(50 * time.Millisecond).
		Run(func(ctx context.Context, n int) (string, error) {
			select {
			case <-time.After(200 * time.Millisecond):
				return fmt.Sprintf("x%d", n), nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		})
	elapsed := time.Since(start)

	if elapsed > 150*time.Millisecond {
		t.Fatalf("timeout did not trigger quickly enough, elapsed: %v", elapsed)
	}
	_ = results
}

func TestMapBuilder_RunSerial(t *testing.T) {
	ctx := context.Background()
	var order []int
	results := NewMapBuilder[int, string](ctx, []int{3, 2, 1}).
		RunSerial(func(ctx context.Context, n int) (string, error) {
			order = append(order, n)
			return fmt.Sprintf("x%d", n), nil
		})

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if order[0] != 3 || order[1] != 2 || order[2] != 1 {
		t.Fatalf("expected serial order [3,2,1], got %v", order)
	}
}

func TestMapBuilder_RunChunk(t *testing.T) {
	ctx := context.Background()
	var chunks [][]int
	NewMapBuilder[int, string](ctx, []int{1, 2, 3, 4, 5, 6}).
		WithConcurrency(2).
		RunChunk(2, func(ctx context.Context, chunk []int) (string, error) {
			chunks = append(chunks, chunk)
			return "ok", nil
		})

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
}

func TestMapBuilder_RunChunked(t *testing.T) {
	ctx := context.Background()
	var sum atomic.Int64
	NewMapBuilder[int, string](ctx, []int{1, 2, 3, 4, 5, 6}).
		WithConcurrency(4).
		RunChunked(2, func(ctx context.Context, n int) (string, error) {
			sum.Add(int64(n))
			return fmt.Sprintf("x%d", n), nil
		})

	if sum.Load() != 21 {
		t.Fatalf("expected sum=21, got %d", sum.Load())
	}
}

func TestMapBuilder_RunPool(t *testing.T) {
	ctx := context.Background()
	results := NewMapBuilder[int, string](ctx, []int{1, 2, 3}).
		WithConcurrency(4).
		RunPool(func(ctx context.Context, n int) (string, error) {
			return fmt.Sprintf("p%d", n), nil
		})

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
}

func TestMapBuilder_EmptyItems(t *testing.T) {
	ctx := context.Background()
	results := NewMapBuilder[int, string](ctx, nil).
		WithConcurrency(4).
		Run(func(ctx context.Context, n int) (string, error) {
			return fmt.Sprintf("x%d", n), nil
		})

	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

// ──────────────── ForEachBuilder ────────────────

func TestForEachBuilder_BasicRun(t *testing.T) {
	ctx := context.Background()
	var sum atomic.Int64
	_, err := NewForEachBuilder[int](ctx, []int{1, 2, 3, 4, 5}).
		WithConcurrency(4).
		Run(func(ctx context.Context, n int) error {
			sum.Add(int64(n))
			return nil
		})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sum.Load() != 15 {
		t.Fatalf("expected sum=15, got %d", sum.Load())
	}
}

func TestForEachBuilder_WithFailFast(t *testing.T) {
	ctx := context.Background()
	errTest := errors.New("fail")
	var count atomic.Int64
	_, err := NewForEachBuilder[int](ctx, []int{1, 2, 3, 4, 5}).
		WithConcurrency(4).
		WithFailFast().
		Run(func(ctx context.Context, n int) error {
			if n == 3 {
				return errTest
			}
			count.Add(1)
			return nil
		})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestForEachBuilder_RunSerial(t *testing.T) {
	ctx := context.Background()
	var order []int
	NewForEachBuilder[int](ctx, []int{3, 2, 1}).
		RunSerial(func(ctx context.Context, n int) error {
			order = append(order, n)
			return nil
		})

	if len(order) != 3 || order[0] != 3 || order[1] != 2 || order[2] != 1 {
		t.Fatalf("expected [3,2,1], got %v", order)
	}
}

func TestForEachBuilder_RunChunk(t *testing.T) {
	ctx := context.Background()
	var total atomic.Int64
	NewForEachBuilder[int](ctx, []int{1, 2, 3, 4, 5, 6}).
		WithConcurrency(2).
		RunChunk(2, func(ctx context.Context, chunk []int) error {
			for _, n := range chunk {
				total.Add(int64(n))
			}
			return nil
		})

	if total.Load() != 21 {
		t.Fatalf("expected total=21, got %d", total.Load())
	}
}

func TestForEachBuilder_RunChunked(t *testing.T) {
	ctx := context.Background()
	var total atomic.Int64
	NewForEachBuilder[int](ctx, []int{1, 2, 3, 4, 5, 6}).
		WithConcurrency(4).
		RunChunked(2, func(ctx context.Context, n int) error {
			total.Add(int64(n))
			return nil
		})

	if total.Load() != 21 {
		t.Fatalf("expected total=21, got %d", total.Load())
	}
}

func TestForEachBuilder_RunPool(t *testing.T) {
	ctx := context.Background()
	var count atomic.Int64
	_, err := NewForEachBuilder[int](ctx, []int{1, 2, 3, 4, 5}).
		WithConcurrency(4).
		RunPool(func(ctx context.Context, n int) error {
			count.Add(1)
			return nil
		})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count.Load() != 5 {
		t.Fatalf("expected 5 items processed, got %d", count.Load())
	}
}

func TestForEachBuilder_EmptyItems(t *testing.T) {
	ctx := context.Background()
	_, err := NewForEachBuilder[int](ctx, nil).
		WithConcurrency(4).
		Run(func(ctx context.Context, n int) error {
			return nil
		})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ──────────────── RetryBuilder ────────────────

func TestRetryBuilder_SimpleRun(t *testing.T) {
	ctx := context.Background()
	var count atomic.Int64
	val, err := NewRetryBuilder[int](ctx).
		WithMaxRetries(2).
		Run(func(ctx context.Context) (int, error) {
			count.Add(1)
			if count.Load() < 3 {
				return 0, errors.New("transient")
			}
			return 42, nil
		})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
	if count.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", count.Load())
	}
}

func TestRetryBuilder_ExponentialBackoff(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	var count atomic.Int64
	val, err := NewRetryBuilder[int](ctx).
		WithMaxRetries(3).
		WithExponentialBackoff(10*time.Millisecond, 100*time.Millisecond).
		Run(func(ctx context.Context) (int, error) {
			count.Add(1)
			if count.Load() < 3 {
				return 0, errors.New("transient")
			}
			return 99, nil
		})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 99 {
		t.Fatalf("expected 99, got %d", val)
	}
	if elapsed < 20*time.Millisecond {
		t.Fatalf("expected at least some backoff delay, elapsed: %v", elapsed)
	}
}

func TestRetryBuilder_LinearBackoff(t *testing.T) {
	ctx := context.Background()
	var count atomic.Int64
	err := NewRetryBuilder[string](ctx).
		WithMaxRetries(2).
		WithLinearBackoff(time.Millisecond).
		RunSimple(func(ctx context.Context) error {
			count.Add(1)
			return errors.New("always fail")
		})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if count.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", count.Load())
	}
}

func TestRetryBuilder_RunToResult(t *testing.T) {
	ctx := context.Background()
	result := NewRetryBuilder[int](ctx).
		WithMaxRetries(2).
		RunToResult(func(ctx context.Context) (int, error) {
			return 42, nil
		})

	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if result.Value != 42 {
		t.Fatalf("expected 42, got %d", result.Value)
	}
}

func TestRetryBuilder_RunSimple(t *testing.T) {
	ctx := context.Background()
	var count atomic.Int64
	err := NewRetryBuilder[struct{}](ctx).
		WithMaxRetries(1).
		RunSimple(func(ctx context.Context) error {
			count.Add(1)
			if count.Load() < 2 {
				return errors.New("transient")
			}
			return nil
		})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", count.Load())
	}
}

func TestRetryBuilder_Exhausted(t *testing.T) {
	ctx := context.Background()
	_, err := NewRetryBuilder[int](ctx).
		WithMaxRetries(2).
		Run(func(ctx context.Context) (int, error) {
			return 0, errors.New("persistent")
		})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ──────────────── ReduceBuilder ────────────────

func TestReduceBuilder_BasicRun(t *testing.T) {
	ctx := context.Background()
	sum, err := NewReduceBuilder[int, int](ctx, []int{1, 2, 3, 4, 5}).
		WithConcurrency(4).
		Run(
			func(ctx context.Context, n int) (int, error) { return n, nil },
			0,
			func(acc, val int) int { return acc + val },
		)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sum != 15 {
		t.Fatalf("expected sum=15, got %d", sum)
	}
}

func TestReduceBuilder_TransformAndReduce(t *testing.T) {
	ctx := context.Background()
	sum, err := NewReduceBuilder[int, int](ctx, []int{1, 2, 3, 4, 5}).
		WithConcurrency(4).
		Run(
			func(ctx context.Context, n int) (int, error) { return n * n, nil },
			0,
			func(acc, val int) int { return acc + val },
		)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sum != 55 {
		t.Fatalf("expected 1+4+9+16+25=55, got %d", sum)
	}
}

func TestReduceBuilder_WithFailFast(t *testing.T) {
	ctx := context.Background()
	errTest := errors.New("fail on 3")
	_, err := NewReduceBuilder[int, int](ctx, []int{1, 2, 3, 4, 5}).
		WithConcurrency(4).
		WithFailFast().
		Run(
			func(ctx context.Context, n int) (int, error) {
				if n == 3 {
					return 0, errTest
				}
				return n, nil
			},
			0,
			func(acc, val int) int { return acc + val },
		)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestReduceBuilder_WithTimeout(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	_, err := NewReduceBuilder[int, int](ctx, []int{1, 2, 3}).
		WithConcurrency(3).
		WithTimeout(50*time.Millisecond).
		Run(
			func(ctx context.Context, n int) (int, error) {
				select {
				case <-time.After(200 * time.Millisecond):
					return n, nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			},
			0,
			func(acc, val int) int { return acc + val },
		)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if elapsed > 150*time.Millisecond {
		t.Fatalf("timeout did not trigger quickly enough, elapsed: %v", elapsed)
	}
}

func TestReduceBuilder_EmptyItems(t *testing.T) {
	ctx := context.Background()
	sum, err := NewReduceBuilder[int, int](ctx, nil).
		Run(
			func(ctx context.Context, n int) (int, error) { return n, nil },
			100,
			func(acc, val int) int { return acc + val },
		)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sum != 100 {
		t.Fatalf("expected initial 100, got %d", sum)
	}
}

// ──────────────── 联合使用 ────────────────

func TestBuilder_ChainTogether(t *testing.T) {
	ctx := context.Background()

	mapped := NewMapBuilder[int, int](ctx, []int{1, 2, 3, 4, 5}).
		WithConcurrency(4).
		Run(func(ctx context.Context, n int) (int, error) {
			return n * 10, nil
		})

	values := ResultValues(mapped)
	for i, v := range values {
		expected := (i + 1) * 10
		if v != expected {
			t.Fatalf("at %d: expected %d, got %d", i, expected, v)
		}
	}

	NewForEachBuilder[int](ctx, values).
		WithConcurrency(4).
		Run(func(ctx context.Context, n int) error {
			if n < 10 || n > 50 {
				t.Errorf("unexpected value: %d", n)
			}
			return nil
		})
}

func TestBuilder_CompatibleWithExistingAPI(t *testing.T) {
	ctx := context.Background()

	results1 := Map(ctx, []int{1, 2, 3}, core.IO(), func(ctx context.Context, n int) (string, error) {
		return fmt.Sprintf("old-%d", n), nil
	})

	results2 := NewMapBuilder[int, string](ctx, []int{1, 2, 3}).
		WithConcurrency(core.IO()).
		Run(func(ctx context.Context, n int) (string, error) {
			return fmt.Sprintf("new-%d", n), nil
		})

	if len(results1) != len(results2) {
		t.Fatalf("result count mismatch: %d vs %d", len(results1), len(results2))
	}
	for i := range results1 {
		if results1[i].Value[0:3] != "old" || results2[i].Value[0:3] != "new" {
			t.Fatalf("unexpected values")
		}
	}
}

// ============================================================
// Default 构建器测试（builder.go）
// ============================================================

func TestDefaultMapBuilder(t *testing.T) {
	ctx := context.Background()
	_ = ctx
	items := []int{1, 2, 3}
	m := DefaultMapBuilder[int, int](items)
	results := m.Run(func(ctx context.Context, n int) (int, error) { return n * 10, nil })
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
}

func TestDefaultForEachBuilder(t *testing.T) {
	ctx := context.Background()
	_ = ctx
	items := []int{1, 2, 3}
	f := DefaultForEachBuilder[int](items)
	var sum atomic.Int64
	_, _ = f.RunSerial(func(ctx context.Context, n int) error {
		sum.Add(int64(n))
		return nil
	})
	if sum.Load() != 6 {
		t.Fatalf("expected 6, got %d", sum.Load())
	}
}

func TestDefaultRetryBuilder(t *testing.T) {
	ctx := context.Background()
	_ = ctx
	r := DefaultRetryBuilder[int]()
	val, err := r.Run(func(ctx context.Context) (int, error) { return 42, nil })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestDefaultReduceBuilder(t *testing.T) {
	ctx := context.Background()
	_ = ctx
	items := []int{1, 2, 3, 4, 5}
	r := DefaultReduceBuilder[int, int](items)
	val, err := r.Run(
		func(ctx context.Context, n int) (int, error) { return n, nil },
		0,
		func(acc, n int) int { return acc + n },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 15 {
		t.Fatalf("expected 15, got %d", val)
	}
}
