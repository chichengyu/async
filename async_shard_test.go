package async

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chichengyu/async/internal/shard"
	"github.com/chichengyu/async/testutil"
)

// ============================================================
// ShardPoolBuilder 四档链式测试（万/十万/百万/千万）
//
// 只测试对外暴露 API：
//
//	PoolSharded[T](ctx).Shards(n).Worker(n).Run(fn)
//	PoolSharded[T](ctx).Config(fn).Run(fn)
//	PoolSharded[T](ctx).FailFast().Timeout(d).MaxPending(n).Run(fn)
//	PoolSharded[T](ctx).DefaultShardPoolConfig().Shards(n).Run(fn)
// ============================================================

func TestShardPoolBuilder_Chain_Basic(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var sum int64
			err := PoolSharded[int]().Context(freshCtx()).
				Shards(4).
				Worker(8).
				MaxPending(0).
				Run(func(ctx context.Context, sp *shard.ShardedPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						sp.Submit(ctx, func(ctx context.Context) (int, error) {
							return v * 2, nil
						})
					}
					for _, r := range sp.Wait() {
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

func TestShardPoolBuilder_Chain_FailFast(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			sentinel := errors.New("shard_fail_boom")
			var errorCount int32

			err := PoolSharded[int]().Context(freshCtx()).
				Shards(4).
				Worker(4).
				FailFast().
				Run(func(ctx context.Context, sp *shard.ShardedPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						sp.Submit(ctx, func(ctx context.Context) (int, error) {
							if v%100 == 0 {
								return 0, sentinel
							}
							return v, nil
						})
					}
					for _, r := range sp.Wait() {
						if r.Err != nil {
							atomic.AddInt32(&errorCount, 1)
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
			t.Logf("shard fail errors: %d / %d", errorCount, tier.Size)
		})
	}
}

func TestShardPoolBuilder_Chain_Timeout(t *testing.T) {
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
			err := PoolSharded[int]().Context(freshCtx()).
				Shards(2).
				Worker(2).
				Timeout(10 * time.Millisecond).
				Run(func(ctx context.Context, sp *shard.ShardedPool[int]) error {
					for i := 0; i < sz.n; i++ {
						v := i
						sp.Submit(ctx, func(ctx context.Context) (int, error) {
							select {
							case <-ctx.Done():
								return 0, ctx.Err()
							case <-time.After(200 * time.Millisecond):
								return v, nil
							}
						})
					}
					for _, r := range sp.Wait() {
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
			t.Logf("shard timeout count: %d / %d", timeoutCount, sz.n)
		})
	}
}

func TestShardPoolBuilder_Chain_ConfigFunc(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var sum int64
			err := PoolSharded[int]().Context(freshCtx()).
				Config(func(cfg ShardPoolConfig[int]) ShardPoolConfig[int] {
					cfg.Shards = 2
					cfg.SizePerShard = 32
					return cfg
				}).
				Shards(4). // override
				Worker(8). // override
				Run(func(ctx context.Context, sp *shard.ShardedPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						sp.Submit(ctx, func(ctx context.Context) (int, error) {
							return v, nil
						})
					}
					for _, r := range sp.Wait() {
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

func TestShardPoolBuilder_Chain_DefaultReset(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var count int32
			err := PoolSharded[int]().Context(freshCtx()).
				Shards(100).
				Worker(200).
				DefaultShardPoolConfig().
				Shards(2).
				Worker(16).
				Run(func(ctx context.Context, sp *shard.ShardedPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						sp.Submit(ctx, func(ctx context.Context) (int, error) {
							return 1, nil
						})
					}
					atomic.StoreInt32(&count, int32(len(sp.Wait())))
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

func TestShardPoolBuilder_Chain_Distribution(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var count int32
			err := PoolSharded[int]().Context(freshCtx()).
				Shards(4).
				Worker(8).
				Distribution(RoundRobin).
				Run(func(ctx context.Context, sp *shard.ShardedPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						sp.Submit(ctx, func(ctx context.Context) (int, error) {
							return i, nil
						})
					}
					atomic.StoreInt32(&count, int32(len(sp.Wait())))
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

func TestShardPoolBuilder_Chain_KeyFn(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var count int32
			err := PoolSharded[int]().Context(freshCtx()).
				Shards(4).
				Worker(8).
				Distribution(Hash).
				KeyFn(func(v int) uint64 { return uint64(v) }).
				Run(func(ctx context.Context, sp *shard.ShardedPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						sp.Submit(ctx, func(ctx context.Context) (int, error) {
							return v, nil
						})
					}
					atomic.StoreInt32(&count, int32(len(sp.Wait())))
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

func TestShardPoolBuilder_Chain_MaxPending(t *testing.T) {
	tiers := []testutil.Tier{
		{Name: "万级_10K", Size: 10_000},
		{Name: "十万级_100K", Size: 100_000},
	}
	for _, tier := range tiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var total int32
			err := PoolSharded[int]().Context(freshCtx()).
				Shards(2).
				Worker(4).
				MaxPending(10).
				Run(func(ctx context.Context, sp *shard.ShardedPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						sp.Submit(ctx, func(ctx context.Context) (int, error) {
							time.Sleep(time.Microsecond)
							return i, nil
						})
						atomic.AddInt32(&total, 1)
					}
					sp.Wait()
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if total == 0 {
				t.Fatal("no submissions succeeded")
			}
			t.Logf("submitted: %d / %d", total, tier.Size)
		})
	}
}

func TestShardPoolBuilder_Chain_ErrorInFn(t *testing.T) {
	sentinel := errors.New("shard_builder_error")
	err := PoolSharded[int]().Context(freshCtx()).
		Shards(2).
		Worker(2).
		Run(func(ctx context.Context, sp *shard.ShardedPool[int]) error {
			return sentinel
		})
	if err == nil {
		t.Fatal("expected error from Run fn, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
}

func TestShardPoolBuilder_Chain_EmptyRun(t *testing.T) {
	err := PoolSharded[string]().Context(freshCtx()).
		Shards(2).
		Worker(2).
		Run(func(ctx context.Context, sp *shard.ShardedPool[string]) error {
			return nil
		})
	if err != nil {
		t.Fatalf("empty Run should succeed, got: %v", err)
	}
}

func TestShardPoolBuilder_Chain_MinimalConfig(t *testing.T) {
	for _, tier := range testutil.UseTier {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var count int32
			err := PoolSharded[int]().Context(freshCtx()).
				Shards(2).
				Worker(4).
				Run(func(ctx context.Context, sp *shard.ShardedPool[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						sp.Submit(ctx, func(ctx context.Context) (int, error) {
							return v, nil
						})
					}
					atomic.StoreInt32(&count, int32(len(sp.Wait())))
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if int(count) != tier.Size {
				t.Fatalf("expected %d, got %d", tier.Size, count)
			}
		})
	}
}
