package async

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/testutil"
)

// ============================================================
// TaskBuilder 群式调用测试（万/十万/百万/千万）
// ============================================================

func TestTask_Chain_Basic(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var done atomic.Int64
			for i := 0; i < tier.Size; i++ {
				v := i
				ar := Task[int]().Context(freshCtx()).
					Go(func(ctx context.Context) (int, error) {
						return v * 2, nil
					})
				val, err := ar.Wait()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if val != v*2 {
					t.Fatalf("expected %d, got %d", v*2, val)
				}
				done.Add(1)
			}
			if done.Load() != int64(tier.Size) {
				t.Fatalf("expected %d, got %d", tier.Size, done.Load())
			}
		})
	}
}

func TestTask_Chain_WithTimeout(t *testing.T) {
	ar := Task[int]().Context(freshCtx()).
		WithTimeout(10 * time.Millisecond).
		Go(func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(2 * time.Second):
				return 42, nil
			}
		})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestTask_Chain_GoResult(t *testing.T) {
	tk := Task[string]().Context(freshCtx()).
		GoResult(func(ctx context.Context) (string, error) {
			return "hello", nil
		})
	val, err := tk.Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "hello" {
		t.Fatalf("expected 'hello', got '%s'", val)
	}
}

func TestTask_Chain_GoResultCancel(t *testing.T) {
	tk := Task[int]().Context(freshCtx()).
		GoResult(func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(time.Second):
				return 1, nil
			}
		})
	tk.Cancel()
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected error after cancel")
	}
}

func TestTask_Chain_GoResultTimeout(t *testing.T) {
	tk := Task[int]().Context(freshCtx()).
		WithTimeout(10 * time.Millisecond).
		GoResult(func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(2 * time.Second):
				return 42, nil
			}
		})
	_, err := tk.Result()
	if err == nil {
		t.Fatal("expected timeout error in GoResult")
	}
}

func TestTask_Chain_GoAct(t *testing.T) {
	var called atomic.Bool
	ar := Task[struct{}]().Context(freshCtx()).
		GoAct(func(ctx context.Context) error {
			called.Store(true)
			return nil
		})
	_, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called.Load() {
		t.Fatal("action not called")
	}
}

func TestTask_Chain_GoActError(t *testing.T) {
	sentinel := errors.New("action error")
	ar := Task[struct{}]().Context(freshCtx()).
		GoAct(func(ctx context.Context) error {
			return sentinel
		})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTask_Chain_GoResultAct(t *testing.T) {
	tk := Task[struct{}]().Context(freshCtx()).
		GoResultAct(func(ctx context.Context) error {
			return nil
		})
	_, err := tk.Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTask_Chain_Bounded(t *testing.T) {
	sizes := []int{100, 1000}
	for _, n := range sizes {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			var done atomic.Int64
			for i := 0; i < n; i++ {
				v := i
				ar := Task[int]().Context(freshCtx()).
					Bounded(10).
					Go(func(ctx context.Context) (int, error) {
						return v, nil
					})
				val, err := ar.Wait()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if val != v {
					t.Fatalf("expected %d, got %d", v, val)
				}
				done.Add(1)
			}
			if done.Load() != int64(n) {
				t.Fatalf("expected %d, got %d", n, done.Load())
			}
		})
	}
}

func TestTask_Chain_BoundedTimeout(t *testing.T) {
	ar := Task[int]().Context(freshCtx()).
		Bounded(5).
		WithTimeout(50 * time.Millisecond).
		Go(func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(2 * time.Second):
				return 1, nil
			}
		})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected timeout error with bounded runner")
	}
}

func TestTask_Chain_DefaultTimeout(t *testing.T) {
	ar := Task[int]().Context(freshCtx()).
		WithTimeout(10 * time.Millisecond).
		DefaultTimeout().
		Go(func(ctx context.Context) (int, error) {
			return 42, nil
		})
	val, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error after DefaultTimeout: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestTask_Chain_DefaultBounded(t *testing.T) {
	ar := Task[int]().Context(freshCtx()).
		Bounded(1).
		DefaultBounded().
		Go(func(ctx context.Context) (int, error) {
			return 99, nil
		})
	val, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error after DefaultBounded: %v", err)
	}
	if val != 99 {
		t.Fatalf("expected 99, got %d", val)
	}
}

func TestTask_Chain_PanicRecovery(t *testing.T) {
	ar := Task[int]().Context(freshCtx()).
		Go(func(ctx context.Context) (int, error) {
			panic("builder panic")
		})
	_, err := ar.Wait()
	if err == nil {
		t.Fatal("expected panic error")
	}
}

func TestTask_Chain_NoContext(t *testing.T) {
	ar := Task[int]().
		Go(func(ctx context.Context) (int, error) {
			return 7, nil
		})
	val, err := ar.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 7 {
		t.Fatalf("expected 7, got %d", val)
	}
}

func TestTask_Chain_Concurrent_10K(t *testing.T) {
	testutil.SkipIfTooLarge1M(t, 10000)

	var wg sync.WaitGroup
	n := 10000
	wg.Add(n)

	for i := 0; i < n; i++ {
		v := i
		go func() {
			defer wg.Done()
			ar := Task[int]().Context(freshCtx()).
				Go(func(ctx context.Context) (int, error) {
					return v, nil
				})
			val, err := ar.Wait()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if val != v {
				t.Errorf("expected %d, got %d", v, val)
			}
		}()
	}
	wg.Wait()
}
