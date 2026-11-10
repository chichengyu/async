package group

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
)

// The counter oracle uses arbitrary precision with explicit modulo 2^64. Each
// admitted task acknowledges result recording before the next submission, so
// per-shard completion order is controlled without sleeps or a worker race.
func TestMultiGroupMixedRoutingAcrossCounterWrapAndRejection(t *testing.T) {
	for constructor := range 3 {
		for _, shards := range []int{1, 3, 5} {
			for _, closed := range []int{-1, 0} {
				for _, start := range []uint64{0, 1 << 63, ^uint64(0) - 2} {
					t.Run(fmt.Sprintf("constructor%d/shards%d/closed%d/start%d", constructor, shards, closed, start), func(t *testing.T) {
						exercise := func(mg *MultiGroup[int]) error {
							mg.nextIdx.Store(start)
							if mg.ShardCount() != shards || mg.TotalWorker() != shards*2 {
								t.Fatal("construction shape")
							}
							for _, index := range []int{-int(^uint(0)>>1) - 1, -1, shards, int(^uint(0) >> 1)} {
								if mg.GetShard(index) != nil {
									t.Fatal("out of range shard")
								}
							}
							if closed >= 0 {
								mg.GetShard(closed).Close()
							}
							want := make([][]core.Result[int], shards)
							notices := make(chan int, 1)
							var business atomic.Int32
							for s := range shards {
								mg.GetShard(s).WithResultCallback(func(core.Result[int]) { notices <- s })
							}
							counter := new(big.Int).SetUint64(start)
							modulus := new(big.Int).Lsh(big.NewInt(1), 64)
							divisor := big.NewInt(int64(shards))
							wantCalls := int32(0)
							for step := range 18 {
								keyed := step%3 == 1
								key := []uint64{0, 1, 1 << 63, ^uint64(0), ^uint64(0) - 1, 42}[step/3]
								var route int
								if keyed {
									route = int(new(big.Int).Mod(new(big.Int).SetUint64(key), divisor).Int64())
								} else {
									route = int(new(big.Int).Mod(new(big.Int).Set(counter), divisor).Int64())
									counter.Add(counter, big.NewInt(1))
									counter.Mod(counter, modulus)
								}
								ctx := context.Background()
								canceled := step%4 == 0
								if canceled {
									var cancel context.CancelFunc
									ctx, cancel = context.WithCancel(ctx)
									cancel()
								}
								var submitWant, errorWant error
								valueWant := step + 1
								if route == closed {
									submitWant = core.ErrGroupWaited
								} else if canceled {
									submitWant = context.Canceled
								}
								if submitWant != nil {
									errorWant = submitWant
									valueWant = 0
								} else if step%2 == 1 {
									errorWant = fmt.Errorf("business %d", step)
								}
								businessErr := errorWant
								fn := func(context.Context) (int, error) { business.Add(1); return step + 1, businessErr }
								var got error
								if keyed {
									got = mg.GoKeyed(key, ctx, fn)
								} else {
									got = mg.Go(ctx, fn)
								}
								if got != submitWant {
									t.Fatalf("step %d route %d submit=%v want=%v", step, route, got, submitWant)
								}
								if submitWant == nil {
									wantCalls++
								}
								if route != closed {
									if gotRoute := <-notices; gotRoute != route {
										t.Fatalf("step %d routed to %d want %d", step, gotRoute, route)
									}
								}
								want[route] = append(want[route], core.Result[int]{Value: valueWant, Err: errorWant, Occupied: true})
								if mg.nextIdx.Load() != counter.Uint64() {
									t.Fatalf("step %d counter=%d want %d", step, mg.nextIdx.Load(), counter.Uint64())
								}
							}
							var all []core.Result[int]
							var values []int
							var errs []error
							for s, results := range want {
								all = append(all, results...)
								for _, r := range results {
									if r.Err != nil {
										errs = append(errs, r.Err)
									} else {
										values = append(values, r.Value)
									}
								}
								got := mg.GetShard(s).Wait()
								if !slices.Equal(got, results) || mg.GetShard(s).TotalCount() != int64(len(results)) {
									t.Fatalf("shard %d results=%v want=%v", s, got, results)
								}
							}
							for range 2 {
								if got := mg.Wait(); !slices.Equal(got, all) {
									t.Fatalf("merged result order=%v want=%v", got, all)
								}
							}
							if !slices.Equal(mg.Values(), values) || !slices.Equal(mg.Errors(), errs) || mg.TotalTaskCount() != 18 || mg.TotalFailCount() != int64(len(errs)) || mg.TotalOkCount() != 18-int64(len(errs)) || mg.TotalActive() != 0 || mg.TotalBusy() != 0 || business.Load() != wantCalls {
								t.Fatal("aggregate routing/count/projection disagreement")
							}
							select {
							case <-notices:
								t.Fatal("closed shard rejection notified")
							default:
							}
							gotValues, gotErrors := mg.Values(), mg.Errors()
							if len(gotValues) > 0 {
								gotValues[0] = -999
							}
							if len(gotErrors) > 0 {
								gotErrors[0] = errors.New("caller edit")
							}
							if !slices.Equal(mg.Values(), values) || !slices.Equal(mg.Errors(), errs) {
								t.Fatal("query slice aliases internal slots")
							}
							return nil
						}
						var err error
						switch constructor {
						case 0:
							mg := NewGroup[int](2).Shard(shards)
							defer mg.Close()
							err = exercise(mg)
						case 1:
							err = NewMultiBuilder[int]().Worker(2).Shards(shards).Run(func(_ context.Context, mg *MultiGroup[int]) error { return exercise(mg) })
						case 2:
							err = WithMultiGroup(2, shards, exercise)
						}
						if err != nil {
							t.Fatal(err)
						}
					})
				}
			}
		}
	}
}
