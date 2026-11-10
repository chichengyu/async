package review_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"

	async "github.com/chichengyu/async"
	"github.com/chichengyu/async/internal/pool"
)

// Block callbacks before completion: the selected pool, rather than Worker(1),
// determines how many can start. Virtual quiescence avoids timing assumptions.
func TestDataDefaultPoolWorkerSelection(t *testing.T) {
	for _, mode := range []string{"slice-map", "slice-each", "slice-stream", "slice-batch", "slice-each-batch", "map", "each", "stream", "batch", "each-batch"} {
		for _, borrowed := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "/default", true: "/borrowed"}[borrowed], func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx := context.Background()
					pl := pool.NewPool[int](1)
					defer pl.Close()
					s := async.Slice(ctx, []int{1, 2}).Worker(1).Chunk(1).DefaultPool()
					m := async.NewMapChain(ctx, map[int]int{1: 1, 2: 2}).Worker(1).Chunk(1).DefaultPool()
					if borrowed {
						s = s.Pool(pl)
						m = m.Pool(pl)
					}
					gate, done := make(chan struct{}), make(chan struct{})
					var calls atomic.Int32
					fn := func(context.Context, int) (int, error) { calls.Add(1); <-gate; return 1, nil }
					act := func(ctx context.Context, v int) error { _, err := fn(ctx, v); return err }
					go func() {
						defer close(done)
						switch mode {
						case "slice-map":
							s.Map(fn)
						case "slice-each":
							s.ForEach(act)
						case "slice-stream":
							for range s.Stream(fn, 1) {
							}
						case "slice-batch":
							s.MapBatch(func(ctx context.Context, vs []int) (int, error) { return fn(ctx, vs[0]) })
						case "slice-each-batch":
							s.ForEachBatch(func(ctx context.Context, vs []int) error { return act(ctx, vs[0]) })
						case "map":
							m.Map(func(ctx context.Context, _, v int) (int, error) { return fn(ctx, v) })
						case "each":
							m.ForEach(func(ctx context.Context, _, v int) error { return act(ctx, v) })
						case "stream":
							for range m.Stream(func(ctx context.Context, _, v int) (int, error) { return fn(ctx, v) }, 1) {
							}
						case "batch":
							m.MapBatch(func(ctx context.Context, es []async.MapEntry[int, int]) (int, error) { return fn(ctx, es[0].Val) })
						case "each-batch":
							m.ForEachBatch(func(ctx context.Context, es []async.MapEntry[int, int]) error { return act(ctx, es[0].Val) })
						}
					}()
					synctest.Wait()
					want := int32(2)
					if borrowed {
						want = 1
					}
					if calls.Load() != want {
						t.Errorf("started=%d want=%d", calls.Load(), want)
					}
					close(gate)
					<-done
					if calls.Load() != 2 {
						t.Fatalf("finished=%d want=2", calls.Load())
					}
				})
			})
		}
	}
}
