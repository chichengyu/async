package async

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/testutil"
)

// ============================================================
// PoolBuilder 四档链式测试（万/十万/百万/千万）
//
// 只测试对外暴露 API：
//
//	Pool[T]().Context(ctx).Worker(n).FailFast().Timeout(d).MaxPending(n).Run(fn)
//	Pool[T]().Context(ctx).Config(fn).Run(fn)
//	Pool[T]().Context(ctx).DefaultPoolConfig().Worker(n).Run(fn)
//	Pool[T]().Context(ctx).SubmitTimeout(d).Overflow(s).RingBuf(c).Streaming(n).MaxResults(n).Run(fn)
// ============================================================

func TestPoolBuilder_Chain_Basic(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var sum int64
			err := Pool[int]().Context(freshCtx()).
				Worker(32).
				MaxPending(0).
				Run(func(ctx context.Context, p *pool.Pool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						p.Submit(ctx, func(ctx context.Context) (int, error) {
							return v * 2, nil
						})
					}
					for _, r := range p.Wait() {
						if r.Err != nil {
							t.Errorf("unexpected error: %v", r.Err)
						}
						atomic.AddInt64(&sum, int64(r.Value))
					}
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			n := int64(tier.Size)
			expected := n * (n - 1) // sum(0..n-1) * 2
			if sum != expected {
				t.Fatalf("sum mismatch: expected %d, got %d", expected, sum)
			}
		})
	}
}

func TestPoolBuilder_Chain_FailFast(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			sentinel := errors.New("fail_fast_boom")
			var errorCount int32
			var okCount int32

			err := Pool[int]().Context(freshCtx()).
				Worker(16).
				FailFast().
				Run(func(ctx context.Context, p *pool.Pool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						p.Submit(ctx, func(ctx context.Context) (int, error) {
							if v%100 == 0 {
								return 0, sentinel
							}
							return v, nil
						})
					}
					for _, r := range p.Wait() {
						if r.Err != nil {
							atomic.AddInt32(&errorCount, 1)
						} else {
							atomic.AddInt32(&okCount, 1)
						}
					}
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if errorCount == 0 {
				t.Fatal("FailFast enabled but no errors captured")
			}
			if okCount == 0 && int32(tier.Size) > 100 {
				t.Log("FailFast: all tasks failed or early-exit — acceptable for fail-fast mode")
			}
		})
	}
}

func TestPoolBuilder_Chain_Timeout(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var timeoutCount int32
			err := Pool[int]().Context(freshCtx()).
				Worker(8).
				Timeout(10 * time.Millisecond).
				Run(func(ctx context.Context, p *pool.Pool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						p.Submit(ctx, func(ctx context.Context) (int, error) {
							select {
							case <-ctx.Done():
								return 0, ctx.Err()
							case <-time.After(200 * time.Millisecond):
								return v, nil
							}
						})
					}
					for _, r := range p.Wait() {
						if r.Err != nil {
							atomic.AddInt32(&timeoutCount, 1)
						}
					}
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if timeoutCount == 0 {
				t.Fatal("Timeout=10ms but all context-aware tasks passed — timeout not enforced")
			}
			t.Logf("timeout count: %d / %d", timeoutCount, tier.Size)
		})
	}
}

func TestPoolBuilder_Chain_MaxResults(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			maxR := 100
			var count int32
			err := Pool[int]().Context(freshCtx()).
				Worker(16).
				MaxResults(maxR).
				Run(func(ctx context.Context, p *pool.Pool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						p.Submit(ctx, func(ctx context.Context) (int, error) {
							return v, nil
						})
					}
					results := p.Wait()
					atomic.StoreInt32(&count, int32(len(results)))
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if count > int32(maxR) {
				t.Fatalf("MaxResults=%d but got %d results", maxR, count)
			}
			t.Logf("results: %d / %d (max=%d)", count, tier.Size, maxR)
		})
	}
}

func TestPoolBuilder_Chain_ConfigFunc(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var sum int64
			err := Pool[int]().Context(freshCtx()).
				Config(func(cfg PoolConfig) PoolConfig {
					cfg.Size = 64
					cfg.FailFast = false
					return cfg
				}).
				Worker(32). // override
				MaxPending(0).
				Run(func(ctx context.Context, p *pool.Pool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						p.Submit(ctx, func(ctx context.Context) (int, error) {
							return v, nil
						})
					}
					for _, r := range p.Wait() {
						if r.Err != nil {
							t.Errorf("unexpected error: %v", r.Err)
						}
						atomic.AddInt64(&sum, int64(r.Value))
					}
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			n := int64(tier.Size)
			expected := n * (n - 1) / 2
			if sum != expected {
				t.Fatalf("sum mismatch: expected %d, got %d", expected, sum)
			}
		})
	}
}

func TestPoolBuilder_Chain_DefaultReset(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var count int32
			err := Pool[int]().Context(freshCtx()).
				Worker(200).
				DefaultPoolConfig().
				Worker(32).
				Run(func(ctx context.Context, p *pool.Pool[int]) error {
					for i := 0; i < tier.Size; i++ {
						p.Submit(ctx, func(ctx context.Context) (int, error) {
							return 1, nil
						})
					}
					atomic.StoreInt32(&count, int32(len(p.Wait())))
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if int32(tier.Size) != count {
				t.Fatalf("expected %d results, got %d", tier.Size, count)
			}
		})
	}
}

func TestPoolBuilder_Chain_SubmitTimeout(t *testing.T) {
	sizes := []struct {
		name string
		n    int
	}{
		{"50tasks", 50},
		{"200tasks", 200},
	}
	for _, sz := range sizes {
		t.Run(sz.name, func(t *testing.T) {
			var submitOK int32
			err := Pool[int]().Context(freshCtx()).
				Worker(2).
				SubmitTimeout(50 * time.Millisecond).
				MaxPending(2).
				RingBuf(0).
				Run(func(ctx context.Context, p *pool.Pool[int]) error {
					for i := 0; i < sz.n; i++ {
						p.Submit(ctx, func(ctx context.Context) (int, error) {
							time.Sleep(200 * time.Millisecond)
							return i, nil
						})
						atomic.AddInt32(&submitOK, 1)
					}
					p.Wait()
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if submitOK == 0 {
				t.Fatal("no submissions succeeded")
			}
			t.Logf("submitted: %d / %d", submitOK, sz.n)
		})
	}
}

func TestPoolBuilder_Chain_Streaming(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var count int32
			err := Pool[int]().Context(freshCtx()).
				Worker(16).
				Streaming(256).
				Run(func(ctx context.Context, p *pool.Pool[int]) error {
					for i := 0; i < tier.Size; i++ {
						p.Submit(ctx, func(ctx context.Context) (int, error) {
							return i, nil
						})
					}
					atomic.StoreInt32(&count, int32(len(p.Wait())))
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if int(count) != tier.Size {
				t.Fatalf("expected %d results, got %d", tier.Size, count)
			}
		})
	}
}

func TestPoolBuilder_Chain_OverflowDrop(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var totalSubmit int32
			var totalResults int32
			err := Pool[int]().Context(freshCtx()).
				Worker(4).
				MaxPending(2).
				Overflow(OverflowDrop).
				RingBuf(0).
				Run(func(ctx context.Context, p *pool.Pool[int]) error {
					for i := 0; i < tier.Size; i++ {
						p.Submit(ctx, func(ctx context.Context) (int, error) {
							time.Sleep(10 * time.Microsecond)
							return i, nil
						})
						atomic.AddInt32(&totalSubmit, 1)
					}
					atomic.StoreInt32(&totalResults, int32(len(p.Wait())))
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("submitted=%d results=%d total=%d (OverflowDrop)", totalSubmit, totalResults, tier.Size)
		})
	}
}

func TestPoolBuilder_Chain_RingBuf(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var count int32
			bufSize := 1024
			err := Pool[int]().Context(freshCtx()).
				Worker(32).
				MaxPending(0).
				RingBuf(bufSize).
				Run(func(ctx context.Context, p *pool.Pool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						p.Submit(ctx, func(ctx context.Context) (int, error) {
							return v, nil
						})
					}
					results := p.Wait()
					atomic.StoreInt32(&count, int32(len(results)))
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if int(count) != tier.Size {
				t.Fatalf("expected %d results, got %d", tier.Size, count)
			}
		})
	}
}

func TestPoolBuilder_Chain_ErrorInFn(t *testing.T) {
	sentinel := errors.New("builder_run_error")
	err := Pool[int]().Context(freshCtx()).
		Worker(4).
		Run(func(ctx context.Context, p *pool.Pool[int]) error {
			return sentinel
		})
	if err == nil {
		t.Fatal("expected error from Run fn, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
}

func TestPoolBuilder_Chain_EmptyRun(t *testing.T) {
	err := Pool[string]().Context(freshCtx()).
		Worker(2).
		Run(func(ctx context.Context, p *pool.Pool[string]) error {
			return nil
		})
	if err != nil {
		t.Fatalf("empty Run should succeed, got: %v", err)
	}
}

// ==================== Must ====================

func TestMust_Success(t *testing.T) {
	val := Must(42, nil)
	if val != 42 {
		t.Fatalf("Must(42, nil) = %d, want 42", val)
	}
}

func TestMust_Panic(t *testing.T) {
	sentinel := errors.New("must_panic_error")
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Must should panic on error")
		}
		if r != sentinel {
			t.Fatalf("Must panic = %v, want %v", r, sentinel)
		}
	}()
	Must(0, sentinel)
}

func TestMust_ZeroValue(t *testing.T) {
	val := Must(0, nil)
	if val != 0 {
		t.Fatalf("Must(0, nil) = %d, want 0", val)
	}
}

func TestMust_StringType(t *testing.T) {
	val := Must("hello", nil)
	if val != "hello" {
		t.Fatalf("Must('hello', nil) = %s, want hello", val)
	}
}

func TestMust_StructType(t *testing.T) {
	type S struct{ X int }
	val := Must(S{X: 10}, nil)
	if val.X != 10 {
		t.Fatalf("Must(S{10}, nil).X = %d, want 10", val.X)
	}
}

func TestMust_NilErrorPanics(t *testing.T) {
	err := errors.New("boom")
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic")
		}
		if !errors.Is(r.(error), err) {
			t.Fatalf("panic = %v, want %v", r, err)
		}
	}()
	Must(1, err)
}

// ==================== ForEachPool (top-level) ====================

func TestForEachPool_Basic(t *testing.T) {
	var sum atomic.Int64
	items := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	p, err := ForEachPool(freshCtx(), items, func(ctx context.Context, item int) error {
		sum.Add(int64(item))
		return nil
	}, 4)
	if err != nil {
		t.Fatalf("ForEachPool err = %v, want nil", err)
	}
	if p == nil {
		t.Fatal("ForEachPool returned nil pool")
	}
	if sum.Load() != 55 {
		t.Fatalf("sum = %d, want 55", sum.Load())
	}
}

func TestForEachPool_Empty(t *testing.T) {
	p, err := ForEachPool(freshCtx(), []int{}, func(ctx context.Context, item int) error {
		return nil
	}, 4)
	if err != nil {
		t.Fatalf("ForEachPool empty err = %v, want nil", err)
	}
	if p == nil {
		t.Fatal("ForEachPool empty returned nil pool")
	}
}

func TestForEachPool_ErrorPropagation(t *testing.T) {
	sentinel := errors.New("foreach_failed")
	items := []int{1, 2, 3}
	p, err := ForEachPool(freshCtx(), items, func(ctx context.Context, item int) error {
		if item == 2 {
			return sentinel
		}
		return nil
	}, 2)
	if p == nil {
		t.Fatal("ForEachPool returned nil pool")
	}
	if err == nil {
		t.Fatal("ForEachPool should propagate error")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want %v", err, sentinel)
	}
}

func TestForEachPool_ConcurrencyZero(t *testing.T) {
	var counter atomic.Int64
	items := make([]int, 20)
	p, err := ForEachPool(freshCtx(), items, func(ctx context.Context, item int) error {
		counter.Add(1)
		return nil
	}, 0)
	if err != nil {
		t.Fatalf("ForEachPool concurrency=0 err = %v", err)
	}
	_ = p
	if counter.Load() != 20 {
		t.Fatalf("counter = %d, want 20", counter.Load())
	}
}

func TestForEachPool_ConcurrencyExceedsLen(t *testing.T) {
	var counter atomic.Int64
	items := make([]int, 5)
	p, err := ForEachPool(freshCtx(), items, func(ctx context.Context, item int) error {
		counter.Add(1)
		return nil
	}, 100)
	if err != nil {
		t.Fatalf("ForEachPool concurrency>len err = %v", err)
	}
	_ = p
	if counter.Load() != 5 {
		t.Fatalf("counter = %d, want 5", counter.Load())
	}
}

func TestForEachPool_WorksWithPoolStruct(t *testing.T) {
	var executed atomic.Int64
	p, err := ForEachPool(freshCtx(), []int{1}, func(ctx context.Context, item int) error {
		executed.Store(1)
		return nil
	}, 1)
	if err != nil {
		t.Fatalf("ForEachPool err = %v", err)
	}
	_ = p
	if executed.Load() != 1 {
		t.Fatal("ForEachPool function was not executed")
	}
}
