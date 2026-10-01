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
// ShardedGroupBuilder 四档链式测试（万/十万/百万/千万）
//
// 只测试对外暴露 API：
//
//	GroupSharded[T](ctx).Shards(n).Worker(n).Distribution(d).Run(fn)
// ============================================================

func TestShardedGroupBuilder_Chain_Basic(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var sum int64
			err := GroupSharded[int]().Context(freshCtx()).
				Shards(2).
				Worker(16).
				Distribution(RoundRobin).
				Run(func(ctx context.Context, sg *shard.ShardedGroup[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						sg.Go(ctx, func(ctx context.Context) (int, error) {
							return v * 2, nil
						})
					}
					for _, r := range sg.Wait() {
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

func TestShardedGroupBuilder_Chain_Distribution(t *testing.T) {
	dists := []Distribution{RoundRobin, Hash}
	for _, dist := range dists {
		t.Run("dist", func(t *testing.T) {
			var sum int64
			err := GroupSharded[int]().Context(freshCtx()).
				Shards(2).
				Worker(8).
				Distribution(dist).
				Run(func(ctx context.Context, sg *shard.ShardedGroup[int]) error {
					for i := 0; i < 100; i++ {
						v := i
						sg.Go(ctx, func(ctx context.Context) (int, error) {
							return v, nil
						})
					}
					for _, r := range sg.Wait() {
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
			if sum != 4950 {
				t.Fatalf("expected 4950, got %d", sum)
			}
		})
	}
}

func TestShardedGroupBuilder_Chain_ConfigFunc(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var sum int64
			err := GroupSharded[int]().Context(freshCtx()).
				Config(func(cfg shard.ShardGroupConfig[int]) shard.ShardGroupConfig[int] {
					cfg.Shards = 2
					cfg.ConcurrencyPerShard = 16
					return cfg
				}).
				Run(func(ctx context.Context, sg *shard.ShardedGroup[int]) error {
					for i := 0; i < tier.Size; i++ {
						v := i
						sg.Go(ctx, func(ctx context.Context) (int, error) {
							return v * 2, nil
						})
					}
					for _, r := range sg.Wait() {
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

func TestShardedGroupBuilder_Chain_DefaultReset(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var count int32
			err := GroupSharded[int]().Context(freshCtx()).
				Shards(100).
				Worker(200).
				DefaultShardedGroupConfig().
				Shards(2).
				Worker(16).
				Run(func(ctx context.Context, sg *shard.ShardedGroup[int]) error {
					for i := 0; i < tier.Size; i++ {
						sg.Go(ctx, func(ctx context.Context) (int, error) {
							return 1, nil
						})
					}
					atomic.StoreInt32(&count, int32(len(sg.Wait())))
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

func TestShardedGroupBuilder_Chain_ErrorInFn(t *testing.T) {
	myErr := errors.New("sharded group run error")
	err := GroupSharded[int]().Context(freshCtx()).
		Shards(2).
		Worker(8).
		Run(func(ctx context.Context, sg *shard.ShardedGroup[int]) error {
			return myErr
		})
	if err != myErr {
		t.Fatalf("expected %v, got %v", myErr, err)
	}
}

func TestShardedGroupBuilder_Chain_EmptyRun(t *testing.T) {
	err := GroupSharded[int]().Context(freshCtx()).
		Shards(2).
		Worker(4).
		Run(func(ctx context.Context, sg *shard.ShardedGroup[int]) error {
			results := sg.Wait()
			if len(results) != 0 {
				t.Fatalf("expected empty results, got %d", len(results))
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
}

func TestShardedGroupBuilder_Chain_ShardsOnly(t *testing.T) {
	var sum int64
	err := GroupSharded[int]().Context(freshCtx()).
		Shards(4).
		Run(func(ctx context.Context, sg *shard.ShardedGroup[int]) error {
			if sg.ShardCount() != 4 {
				t.Fatalf("expected 4 shards, got %d", sg.ShardCount())
			}
			for i := 0; i < 100; i++ {
				v := i
				sg.Go(ctx, func(ctx context.Context) (int, error) {
					return v, nil
				})
			}
			for _, r := range sg.Wait() {
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

func TestShardedGroupBuilder_Chain_WithBG(t *testing.T) {
	var count int32
	err := GroupSharded[int]().
		Shards(2).
		Worker(8).
		Run(func(ctx context.Context, sg *shard.ShardedGroup[int]) error {
			for i := 0; i < 100; i++ {
				sg.Go(ctx, func(ctx context.Context) (int, error) {
					return 1, nil
				})
			}
			atomic.StoreInt32(&count, int32(len(sg.Wait())))
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatalf("expected 100, got %d", count)
	}
}

func TestShardedGroupBuilder_Chain_NilCtx(t *testing.T) {
	var count int32
	err := GroupSharded[int]().
		Shards(2).
		Worker(8).
		Run(func(ctx context.Context, sg *shard.ShardedGroup[int]) error {
			for i := 0; i < 100; i++ {
				sg.Go(ctx, func(ctx context.Context) (int, error) {
					return 1, nil
				})
			}
			atomic.StoreInt32(&count, int32(len(sg.Wait())))
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Fatalf("expected 100, got %d", count)
	}
}

func TestShardedGroupBuilder_Chain_ContextSwitch(t *testing.T) {
	ctx1 := freshCtx()
	ctx2 := freshCtx()
	var sum int64
	err := GroupSharded[int]().Context(ctx1).Context(ctx2).
		Run(func(ctx context.Context, sg *shard.ShardedGroup[int]) error {
			for i := 0; i < 100; i++ {
				v := i
				sg.Go(ctx, func(ctx context.Context) (int, error) {
					return v, nil
				})
			}
			for _, r := range sg.Wait() {
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

func TestShardedGroupBuilder_Chain_WaitContext(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var count int32
			err := GroupSharded[int]().Context(freshCtx()).
				Shards(2).
				Worker(16).
				Run(func(ctx context.Context, sg *shard.ShardedGroup[int]) error {
					for i := 0; i < tier.Size; i++ {
						sg.Go(ctx, func(ctx context.Context) (int, error) {
							return i, nil
						})
					}
					results, ok := sg.WaitContext(ctx)
					if !ok {
						t.Fatal("WaitContext returned !ok")
					}
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

func TestShardedGroupBuilder_Chain_WaitTimeout(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			err := GroupSharded[int]().Context(freshCtx()).
				Shards(2).
				Worker(16).
				Run(func(ctx context.Context, sg *shard.ShardedGroup[int]) error {
					for i := 0; i < tier.Size; i++ {
						sg.Go(ctx, func(ctx context.Context) (int, error) {
							return i, nil
						})
					}
					_, ok := sg.WaitTimeout(30 * time.Second)
					if !ok {
						t.Fatal("WaitTimeout returned !ok")
					}
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestShardedGroupBuilder_Chain_FailFast(t *testing.T) {
	for _, tier := range testutil.AllTiers {
		if tier.Size > 100_000 {
			t.Skip("skip large tier for failfast")
		}
		t.Run(tier.Name, func(t *testing.T) {
			testutil.SkipIfTooLarge1M(t, tier.Size)

			var errCount int32
			err := GroupSharded[int]().Context(freshCtx()).
				Shards(2).
				Worker(4).
				Run(func(ctx context.Context, sg *shard.ShardedGroup[int]) error {
					// Set failfast via WithFailFast on individual shard
					for s := 0; s < sg.ShardCount(); s++ {
						sg.GetShard(s).WithFailFast(ctx)
					}
					for i := 0; i < tier.Size; i++ {
						v := i
						sg.Go(ctx, func(ctx context.Context) (int, error) {
							if v%10 == 0 {
								return 0, errors.New("failfast test error")
							}
							return v, nil
						})
					}
					for _, r := range sg.Wait() {
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
