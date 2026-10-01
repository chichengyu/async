package async

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/group"
	"github.com/chichengyu/async/testutil"
)

// ============================================================
// MultiGroupBuilder 四档链式测试（万/十万/百万/千万）
//
// 只测试对外暴露 API：
//
//	GroupMulti[T](ctx).Shards(n).Worker(n).FailFast().Timeout(d).Run(fn)
// ============================================================

func TestMultiGroupBuilder_Chain_Basic(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var sum int64
			err := GroupMulti[int]().Context(freshCtx()).
				Shards(2).
				Worker(16).
				Run(func(ctx context.Context, mg *group.MultiGroup[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						mg.Go(ctx, func(ctx context.Context) (int, error) {
							return v * 2, nil
						})
					}
					results := mg.Wait()
					for _, r := range results {
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
			expected := n * (n - 1)
			if sum != expected {
				t.Fatalf("sum mismatch: expected %d, got %d", expected, sum)
			}
		})
	}
}

func TestMultiGroupBuilder_Chain_FailFast(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		if tier.Size > 100_000 {
			t.Skip("skip large tier for failfast")
		}
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var errCount int32
			err := GroupMulti[int]().Context(freshCtx()).
				Shards(2).
				Worker(4).
				FailFast().
				Timeout(5 * time.Second).
				Run(func(ctx context.Context, mg *group.MultiGroup[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						mg.Go(ctx, func(ctx context.Context) (int, error) {
							if v%10 == 0 {
								return 0, errors.New("failfast test error")
							}
							return v, nil
						})
					}
					results := mg.Wait()
					for _, r := range results {
						if r.Err != nil {
							atomic.AddInt32(&errCount, 1)
						}
					}
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if errCount == 0 {
				t.Fatal("FailFast enabled but no errors detected")
			}
			t.Logf("errors: %d / %d", errCount, tier.Size)
		})
	}
}

func TestMultiGroupBuilder_Chain_Timeout(t *testing.T) {
	sizes := []struct {
		name string
		n    int
	}{
		{"10tasks", 10},
		{"100tasks", 100},
	}
	for _, sz := range sizes {
		t.Run(sz.name, func(t *testing.T) {
			var timeoutCount int32
			err := GroupMulti[int]().Context(freshCtx()).
				Shards(2).
				Worker(2).
				Timeout(10 * time.Millisecond).
				Run(func(ctx context.Context, mg *group.MultiGroup[int]) error {
					for i := 0; i < sz.n; i++ {
						v := i
						mg.Go(ctx, func(ctx context.Context) (int, error) {
							select {
							case <-ctx.Done():
								return 0, ctx.Err()
							case <-time.After(200 * time.Millisecond):
								return v, nil
							}
						})
					}
					results := mg.Wait()
					for _, r := range results {
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
				t.Fatal("Timeout=10ms but all context-aware tasks passed")
			}
		})
	}
}

func TestMultiGroupBuilder_Chain_ErrorInFn(t *testing.T) {
	myErr := errors.New("multi group run error")
	err := GroupMulti[int]().Context(freshCtx()).
		Shards(2).
		Worker(8).
		Run(func(ctx context.Context, mg *group.MultiGroup[int]) error {
			return myErr
		})
	if err != myErr {
		t.Fatalf("expected %v, got %v", myErr, err)
	}
}

func TestMultiGroupBuilder_Chain_EmptyRun(t *testing.T) {
	err := GroupMulti[int]().Context(freshCtx()).
		Shards(2).
		Worker(4).
		Run(func(ctx context.Context, mg *group.MultiGroup[int]) error {
			results := mg.Wait()
			if len(results) != 0 {
				t.Fatalf("expected empty results, got %d", len(results))
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMultiGroupBuilder_Chain_ShardsOnly(t *testing.T) {
	var sum int64
	err := GroupMulti[int]().Context(freshCtx()).
		Shards(4).
		Run(func(ctx context.Context, mg *group.MultiGroup[int]) error {
			if mg.ShardCount() != 4 {
				t.Fatalf("expected 4 shards, got %d", mg.ShardCount())
			}
			for i := 0; i < 100; i++ {
				v := i
				mg.Go(ctx, func(ctx context.Context) (int, error) {
					return v, nil
				})
			}
			results := mg.Wait()
			for _, r := range results {
				atomic.AddInt64(&sum, int64(r.Value))
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if sum != 4950 {
		t.Fatalf("expected 4950, got %d", sum)
	}
}

func TestMultiGroupBuilder_Chain_WithBG(t *testing.T) {
	var count int32
	err := GroupMulti[int]().
		Shards(2).
		Worker(8).
		Run(func(ctx context.Context, mg *group.MultiGroup[int]) error {
			for i := 0; i < 100; i++ {
				mg.Go(ctx, func(ctx context.Context) (int, error) {
					return 1, nil
				})
			}
			results := mg.Wait()
			atomic.StoreInt32(&count, int32(len(results)))
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatalf("expected 100, got %d", count)
	}
}

func TestMultiGroupBuilder_Chain_NilCtx(t *testing.T) {
	var count int32
	err := GroupMulti[int]().
		Shards(2).
		Worker(8).
		Run(func(ctx context.Context, mg *group.MultiGroup[int]) error {
			for i := 0; i < 100; i++ {
				mg.Go(ctx, func(ctx context.Context) (int, error) {
					return 1, nil
				})
			}
			results := mg.Wait()
			atomic.StoreInt32(&count, int32(len(results)))
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatalf("expected 100, got %d", count)
	}
}

func TestMultiGroupBuilder_Chain_ContextSwitch(t *testing.T) {
	ctx1 := freshCtx()
	ctx2 := freshCtx()
	var sum int64
	err := GroupMulti[int]().Context(ctx1).
		Shards(2).
		Worker(8).
		Context(ctx2).
		Run(func(ctx context.Context, mg *group.MultiGroup[int]) error {
			for i := 0; i < 100; i++ {
				v := i
				mg.Go(ctx, func(ctx context.Context) (int, error) {
					return v, nil
				})
			}
			results := mg.Wait()
			for _, r := range results {
				atomic.AddInt64(&sum, int64(r.Value))
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if sum != 4950 {
		t.Fatalf("expected 4950, got %d", sum)
	}
}

func TestMultiGroupBuilder_Chain_SubmitTimeout(t *testing.T) {
	t.Run("submittimeout", func(t *testing.T) {
		var count int32
		err := GroupMulti[int]().Context(freshCtx()).
			Shards(2).
			Worker(8).
			SubmitTimeout(5 * time.Second).
			Run(func(ctx context.Context, mg *group.MultiGroup[int]) error {
				for i := 0; i < 100; i++ {
					mg.Go(ctx, func(ctx context.Context) (int, error) {
						return i, nil
					})
				}
				results := mg.Wait()
				atomic.StoreInt32(&count, int32(len(results)))
				return nil
			})
		if err != nil {
			t.Fatal(err)
		}
		if count != 100 {
			t.Fatalf("expected 100, got %d", count)
		}
	})
}
