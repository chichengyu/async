package async_test

import (
	"context"
	"testing"

	async "github.com/chichengyu/async"
)

func TestPublicPoolCallbacks(t *testing.T) {
	if err := async.Pool[int]().Worker(1).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
		return p.Submit(ctx, func(context.Context) (int, error) { return 1, nil })
	}); err != nil {
		t.Fatal(err)
	}
	if err := async.PoolMulti[int]().Worker(1).Shards(2).Run(func(ctx context.Context, p *async.MultiPool[int]) error {
		return p.Submit(ctx, func(context.Context) (int, error) { return 1, nil })
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPublicSliceUsesPool(t *testing.T) {
	err := async.Pool[int]().Worker(2).Run(func(ctx context.Context, p *async.PoolInstance[int]) error {
		result := async.Slice[int](ctx, []int{1, 2, 3}).Pool(p).Map(func(_ context.Context, n int) (int, error) {
			return n * 2, nil
		})
		if result.Err() || result.Len() != 3 {
			t.Fatalf("unexpected slice result: %v", result.Error())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPublicSliceModeSwitchPreservesFilteredData(t *testing.T) {
	serial := async.Slice[int](context.Background(), []int{1, 2, 3}).Serial().Filter(func(n int) bool { return n > 1 })
	result := serial.ToParallel().Worker(2).Map(func(_ context.Context, n int) (int, error) { return n, nil })
	if result.Err() || result.Len() != 2 {
		t.Fatalf("result length = %d, error = %v", result.Len(), result.Error())
	}
}

func TestPublicGroupAndShardCallbacks(t *testing.T) {
	if err := async.Group[int]().Run(func(context.Context, *async.GroupInstance[int]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := async.GroupVoid().Run(func(context.Context, *async.GroupNoResult) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := async.GroupMulti[int]().Shards(2).Run(func(context.Context, *async.MultiGroup[int]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := async.GroupSharded[int]().Shards(2).Run(func(context.Context, *async.ShardedGroup[int]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := async.PoolSharded[int]().Shards(2).Run(func(context.Context, *async.ShardedPool[int]) error { return nil }); err != nil {
		t.Fatal(err)
	}
}
