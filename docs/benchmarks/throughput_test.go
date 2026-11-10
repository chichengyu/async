package benchmarks_test

import (
	"context"
	"fmt"
	"testing"

	async "github.com/chichengyu/async"
	"github.com/chichengyu/async/internal/pool"
)

// Pool measurements include submission, execution and waiting. All variants
// use 32 workers in total and retain at most one result per shard.
func BenchmarkThroughputPool(b *testing.B) {
	for _, shards := range []int{1, 8, 16} {
		b.Run(fmt.Sprintf("Shards%d", shards), func(b *testing.B) {
			p := pool.NewPool[int](32 / shards).WithTimeout(0).WithMaxResults(1)
			mp := p.Shard(shards)
			defer mp.Close()
			ctx := context.Background()
			fn := func(context.Context) (int, error) { return 1, nil }
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					var err error
					if shards == 1 {
						err = p.Submit(ctx, fn)
					} else {
						err = mp.Submit(ctx, fn)
					}
					if err != nil {
						b.Error(err)
						return
					}
				}
			})
			mp.Wait()
			b.StopTimer()
			var successes int64
			for i := 0; i < shards; i++ {
				s := mp.GetShard(i).Stats()
				if s.FailTask != 0 {
					b.Fatalf("failed tasks: %+v", s)
				}
				successes += s.OkTask
			}
			if successes != int64(b.N) {
				b.Fatalf("completed %d, want %d", successes, b.N)
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tasks/s")
		})
	}
}

func BenchmarkThroughputMapGet(b *testing.B) {
	m := async.NewMapCap[int, int](1024)
	for i := 0; i < 1024; i++ {
		m.Set(i, i)
	}
	b.ResetTimer()
	var sum int64
	for i := 0; i < b.N; i++ {
		v, ok := m.Get(i & 1023)
		if !ok {
			b.Fatal("missing key")
		}
		sum += int64(v)
	}
	b.StopTimer()
	if b.N >= 1024 && sum == 0 {
		b.Fatal("lookup result unused")
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "lookups/s")
}

func BenchmarkThroughputRateLimiter(b *testing.B) {
	ctx := context.Background()
	rl := async.Ratelimit(ctx).RateLimiter().Rate(1024).Build()
	defer rl.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := rl.Acquire(ctx); err != nil {
			b.Fatal(err)
		}
		rl.Release()
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "pairs/s")
}

func BenchmarkThroughputPipeline(b *testing.B) {
	const batch = 4096
	items := make([]int, batch)
	for i := range items {
		items[i] = i
	}
	fn := func(_ context.Context, _ string, v int) (int, error) { return v + 1, nil }
	p := async.Pipeline(items).Stage("first", 8).Stage("second", 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := p.Execute(fn)
		if err != nil {
			b.Fatal(err)
		}
		if len(results) != batch {
			b.Fatal("incomplete pipeline")
		}
		for j, result := range results {
			if result.Err != nil || result.Value != j+2 {
				b.Fatal("incorrect pipeline result")
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)*batch/b.Elapsed().Seconds(), "items/s")
}
