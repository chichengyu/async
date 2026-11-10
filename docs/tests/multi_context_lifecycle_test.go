package review_test

import (
	"context"
	"fmt"
	"testing"

	async "github.com/chichengyu/async"
)

// The late shard cannot complete until the first shard's normal Wait returns.
// This tests the shared layer without timing assumptions.
func TestMultiBuilderSharedContextLifetime(t *testing.T) {
	for _, engine := range []string{"pool", "group"} {
		for _, incomplete := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/incomplete=%v", engine, incomplete), func(t *testing.T) {
				parent, stopParent := context.WithCancel(context.Background())
				defer stopParent()
				run := func(shared context.Context, firstWait func(),
					submit func(context.Context, func(context.Context) (int, error)) error,
					wait func() []async.Result[int],
					waitContext func(context.Context) ([]async.Result[int], bool)) error {
					started, release := make(chan struct{}), make(chan struct{})
					defer close(release)
					if err := submit(shared, func(ctx context.Context) (int, error) {
						close(started)
						if incomplete {
							<-release // Deliberately ignores cancellation until released.
						} else {
							<-release
							if ctx.Err() != nil {
								return 0, ctx.Err()
							}
						}
						return 42, nil
					}); err != nil {
						return err
					}
					<-started
					if incomplete {
						canceled, cancel := context.WithCancel(context.Background())
						cancel()
						_, ok := waitContext(canceled)
						if ok || shared.Err() == nil || parent.Err() != nil {
							return fmt.Errorf("incomplete wait: ok=%v shared=%v parent=%v", ok, shared.Err(), parent.Err())
						}
						return nil
					}
					firstWait()
					if shared.Err() != nil {
						return fmt.Errorf("normal first wait canceled shared context: %v", shared.Err())
					}
					release <- struct{}{}
					results := wait()
					if len(results) != 1 || results[0].Value != 42 || results[0].Err != nil {
						return fmt.Errorf("late shard results = %v", results)
					}
					if shared.Err() == nil || parent.Err() != nil {
						return fmt.Errorf("final wait: shared=%v parent=%v", shared.Err(), parent.Err())
					}
					return nil
				}
				var err error
				if engine == "pool" {
					err = async.PoolMulti[int]().Context(parent).Shards(2).Worker(1).FailFast().Run(
						func(ctx context.Context, mp *async.MultiPool[int]) error {
							return run(ctx, func() { mp.GetShard(0).Wait() }, mp.GetShard(1).Submit, mp.Wait, mp.WaitContext)
						})
				} else {
					err = async.GroupMulti[int]().Context(parent).Shards(2).Worker(1).FailFast().Run(
						func(ctx context.Context, mg *async.MultiGroup[int]) error {
							return run(ctx, func() { mg.GetShard(0).Wait() }, mg.GetShard(1).Go, mg.Wait, mg.WaitContext)
						})
				}
				if err != nil {
					t.Fatal(err)
				}
				if parent.Err() != nil {
					t.Fatal("builder canceled owner context")
				}
			})
		}
	}
}
