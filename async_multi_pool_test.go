package async

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/pool"
	"github.com/chichengyu/async/test"
)

// ============================================================
// MultiPoolBuilder 四档链式测试（万/十万/百万/千万）
//
// 只测试对外暴露 API：
//
//	PoolMulti[T](ctx).Shards(n).Worker(n).FailFast().Timeout(d).Run(fn)
// ============================================================

func TestMultiPoolBuilder_Chain_Basic(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge1M(t, tier.Size)

			var sum int64
			err := PoolMulti[int]().Context(freshCtx()).
				Shards(2).
				Worker(16).
				MaxPending(0).
				Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						mp.Submit(ctx, func(ctx context.Context) (int, error) {
							return v * 2, nil
						})
					}
					for _, r := range mp.Wait() {
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

func TestMultiPoolBuilder_Chain_FailFast(t *testing.T) {
	for _, tier := range test.UseTier {
		if tier.Size > 100_000 {
			t.Skip("skip large tier for failfast")
		}
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge1M(t, tier.Size)

			var errCount int32
			var okCount int32
			err := PoolMulti[int]().Context(freshCtx()).
				Shards(2).
				Worker(4).
				FailFast().
				Timeout(5 * time.Second).
				Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						mp.Submit(ctx, func(ctx context.Context) (int, error) {
							if v%10 == 0 {
								return 0, errors.New("failfast test error")
							}
							return v, nil
						})
					}
					for _, r := range mp.Wait() {
						if r.Err != nil {
							atomic.AddInt32(&errCount, 1)
						} else {
							atomic.AddInt32(&okCount, 1)
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
			t.Logf("errors: %d, ok: %d / %d", errCount, okCount, tier.Size)
		})
	}
}

func TestMultiPoolBuilder_Chain_Timeout(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge1M(t, tier.Size)

			var timeoutCount int32
			err := PoolMulti[int]().Context(freshCtx()).
				Shards(2).
				Worker(8).
				Timeout(10 * time.Millisecond).
				Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						mp.Submit(ctx, func(ctx context.Context) (int, error) {
							select {
							case <-ctx.Done():
								return 0, ctx.Err()
							case <-time.After(200 * time.Millisecond):
								return v, nil
							}
						})
					}
					for _, r := range mp.Wait() {
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

func TestMultiPoolBuilder_Chain_ConfigFunc(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge1M(t, tier.Size)

			var sum int64
			err := PoolMulti[int]().Context(freshCtx()).
				Shards(2).
				Config(func(c PoolConfig) PoolConfig {
					c.Size = 16
					return c
				}).
				Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						mp.Submit(ctx, func(ctx context.Context) (int, error) {
							return v * 2, nil
						})
					}
					for _, r := range mp.Wait() {
						if r.Err != nil {
							return r.Err
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

func TestMultiPoolBuilder_Chain_DefaultReset(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge1M(t, tier.Size)

			var count int32
			err := PoolMulti[int]().Context(freshCtx()).
				Shards(100).
				Worker(200).
				DefaultMultiPoolConfig().
				Shards(2).
				Worker(16).
				Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						mp.Submit(ctx, func(ctx context.Context) (int, error) {
							return 1, nil
						})
					}
					atomic.StoreInt32(&count, int32(len(mp.Wait())))
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

func TestMultiPoolBuilder_Chain_ErrorInFn(t *testing.T) {
	myErr := errors.New("multi pool run error")
	err := PoolMulti[int]().Context(freshCtx()).
		Shards(2).
		Worker(8).
		Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
			return myErr
		})
	if err != myErr {
		t.Fatalf("expected %v, got %v", myErr, err)
	}
}

func TestMultiPoolBuilder_Chain_EmptyRun(t *testing.T) {
	err := PoolMulti[int]().Context(freshCtx()).
		Shards(2).
		Worker(4).
		Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
			results := mp.Wait()
			if len(results) != 0 {
				t.Fatalf("expected empty results, got %d", len(results))
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMultiPoolBuilder_Chain_SubmitTimeout(t *testing.T) {
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
			err := PoolMulti[int]().Context(freshCtx()).
				Shards(2).
				Worker(2).
				SubmitTimeout(50 * time.Millisecond).
				MaxPending(2).
				Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
					for i := 0; i < sz.n; i++ {
						mp.Submit(ctx, func(ctx context.Context) (int, error) {
							time.Sleep(200 * time.Millisecond)
							return i, nil
						})
						atomic.AddInt32(&submitOK, 1)
					}
					mp.Wait()
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

func TestMultiPoolBuilder_Chain_Streaming(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge1M(t, tier.Size)

			var count int32
			err := PoolMulti[int]().Context(freshCtx()).
				Shards(2).
				Worker(16).
				Streaming(256).
				Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						mp.Submit(ctx, func(ctx context.Context) (int, error) {
							return i, nil
						})
					}
					atomic.StoreInt32(&count, int32(len(mp.Wait())))
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

func TestMultiPoolBuilder_Chain_RingBuf(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge1M(t, tier.Size)

			var count int32
			bufSize := 1024
			err := PoolMulti[int]().Context(freshCtx()).
				Shards(2).
				Worker(32).
				MaxPending(0).
				RingBuf(bufSize).
				Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						mp.Submit(ctx, func(ctx context.Context) (int, error) {
							return v, nil
						})
					}
					results := mp.Wait()
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

func TestMultiPoolBuilder_Chain_ShardsOnly(t *testing.T) {
	ctx := freshCtx()
	var sum int64
	err := PoolMulti[int]().Context(ctx).
		Shards(4).
		Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
			if mp.ShardCount() != 4 {
				t.Fatalf("expected 4 shards, got %d", mp.ShardCount())
			}
			for i := 0; i < 100; i++ {
				v := i
				mp.Submit(ctx, func(ctx context.Context) (int, error) {
					return v, nil
				})
			}
			for _, r := range mp.Wait() {
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

func TestMultiPoolBuilder_Chain_WithBG(t *testing.T) {
	var count int32
	err := PoolMulti[int]().
		Shards(2).
		Worker(8).
		Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
			for i := 0; i < 100; i++ {
				mp.Submit(ctx, func(ctx context.Context) (int, error) {
					return 1, nil
				})
			}
			atomic.StoreInt32(&count, int32(len(mp.Wait())))
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatalf("expected 100, got %d", count)
	}
}

func TestMultiPoolBuilder_Chain_NilCtx(t *testing.T) {
	var count int32
	err := PoolMulti[int]().
		Shards(2).
		Worker(8).
		Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
			for i := 0; i < 100; i++ {
				mp.Submit(ctx, func(ctx context.Context) (int, error) {
					return 1, nil
				})
			}
			atomic.StoreInt32(&count, int32(len(mp.Wait())))
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatalf("expected 100, got %d", count)
	}
}

func TestMultiPoolBuilder_Chain_ContextSwitch(t *testing.T) {
	ctx1 := freshCtx()
	ctx2 := freshCtx()
	var sum int64
	err := PoolMulti[int]().Context(ctx1).
		Shards(2).
		Worker(8).
		Context(ctx2).
		Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
			for i := 0; i < 100; i++ {
				mp.Submit(ctx, func(ctx context.Context) (int, error) {
					return i, nil
				})
			}
			for _, r := range mp.Wait() {
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

func TestMultiPoolBuilder_Chain_MaxResults(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge1M(t, tier.Size)

			var count int32
			err := PoolMulti[int]().Context(freshCtx()).
				Shards(2).
				Worker(16).
				MaxResults(tier.Size / 2).
				Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						mp.Submit(ctx, func(ctx context.Context) (int, error) {
							return i, nil
						})
					}
					atomic.StoreInt32(&count, int32(len(mp.Wait())))
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if int(count) > tier.Size {
				t.Fatalf("result count %d exceeds total %d", count, tier.Size)
			}
		})
	}
}

func TestMultiPoolBuilder_Chain_OverflowDrop(t *testing.T) {
	for _, tier := range test.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			test.SkipIfTooLarge1M(t, tier.Size)

			var totalSubmit int32
			var totalResults int32
			err := PoolMulti[int]().Context(freshCtx()).
				Shards(2).
				Worker(4).
				MaxPending(2).
				Overflow(OverflowDrop).
				RingBuf(0).
				Run(func(ctx context.Context, mp *pool.MultiPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						mp.Submit(ctx, func(ctx context.Context) (int, error) {
							time.Sleep(10 * time.Microsecond)
							return i, nil
						})
						atomic.AddInt32(&totalSubmit, 1)
					}
					atomic.StoreInt32(&totalResults, int32(len(mp.Wait())))
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("submitted=%d results=%d total=%d (OverflowDrop)", totalSubmit, totalResults, tier.Size)
		})
	}
}
