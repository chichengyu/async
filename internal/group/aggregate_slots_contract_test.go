package group

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

// Completion is acknowledged before the next submission. The oracle stores only
// logical slots, finding the highest vacant position without using freeIndices
// or any result accessor to derive the expected aggregate. No timeout or active
// business is allowed while FillNoResultSkipped runs.
func TestNoResultAggregateSparseDuplicateAndSkippedSlots(t *testing.T) {
	histories := [][]int{nil, {5}, {5, 5, 0, -1, 2}, {0, 1, 2}, {-1, 4, -1, 4, 0}, {5, 3, 1}}
	for engine := range 3 {
		for history, indices := range histories {
			for outcome := range 3 {
				for _, target := range []int{-int(^uint(0)>>1) - 1, -int(^uint(0) >> 1), -1, 0, 1, 3, 5, 6, 9} {
					t.Run(fmt.Sprintf("engine%d/history%d/outcome%d/target%d", engine, history, outcome, target), func(t *testing.T) {
						var nr *NoResult
						if engine == 0 {
							nr = NewNoResult(2)
						} else {
							b := NewGroupNoResultBuilder().Worker(2)
							if engine == 2 {
								p := pool.NewPool[struct{}](2).WithMaxResults(1)
								t.Cleanup(p.Close)
								b.Pool(p)
							}
							nr = b.Build()
						}
						t.Cleanup(nr.Close)
						notified := make(chan core.Result[struct{}], len(indices)+1)
						var callbacks, business atomic.Int32
						nr.WithStreaming(len(indices) + 1).WithResultCallback(func(r core.Result[struct{}]) {
							callbacks.Add(1)
							notified <- r
						})
						type slot struct {
							live bool
							err  error
						}
						var model []slot
						put := func(index int, value slot) {
							if index >= 0 {
								for len(model) <= index {
									model = append(model, slot{})
								}
								old := model[index]
								model[index] = value
								if !old.live {
									return
								}
								value = old
							}
							for i := len(model) - 1; i >= 0; i-- {
								if !model[i].live {
									model[i] = value
									return
								}
							}
							model = append(model, value)
						}
						wantBusiness := 0
						for step, index := range indices {
							ctx := context.Background()
							var wantErr error
							canceled := outcome == 2 && step%2 == 0
							if canceled {
								var cancel context.CancelFunc
								ctx, cancel = context.WithCancel(ctx)
								cancel()
								wantErr = context.Canceled
							} else {
								wantBusiness++
								if outcome != 0 && step%2 == 1 {
									wantErr = fmt.Errorf("business step %d", step)
								}
							}
							fn := func(context.Context) error { business.Add(1); return wantErr }
							var err error
							if index < 0 {
								err = nr.Go(ctx, fn)
							} else {
								err = nr.GoAt(index, ctx, fn)
							}
							if (canceled && err != context.Canceled) || (!canceled && err != nil) {
								t.Fatalf("submission %d: %v", step, err)
							}
							if r := <-notified; !r.Occupied || r.Err != wantErr {
								t.Fatalf("notification %d: %+v", step, r)
							}
							put(index, slot{true, wantErr})
						}
						nr.Wait()
						wantTotal := len(indices)
						check := func() []core.Result[struct{}] {
							t.Helper()
							var wantErrors []error
							for _, item := range model {
								if item.live && item.err != nil {
									wantErrors = append(wantErrors, item.err)
								}
							}
							var wantFirst error
							if len(wantErrors) > 0 {
								wantFirst = wantErrors[0]
							}
							total, failed, first, got := BuildAggregateNoResult(nr)
							if total != int64(wantTotal) || failed != int64(len(wantErrors)) || first != wantFirst || len(got) != len(model) {
								t.Fatalf("aggregate %d/%d/%v slots=%d want %d/%d/%v slots=%d", total, failed, first, len(got), wantTotal, len(wantErrors), wantFirst, len(model))
							}
							for i, item := range model {
								if got[i].Occupied != item.live || got[i].Err != item.err {
									t.Fatalf("slot %d: %+v want %+v", i, got[i], item)
								}
							}
							if nr.TotalCount() != total || nr.FailCount() != failed || nr.OkCount() != total-failed || nr.Active() != 0 || nr.Busy() != 0 || nr.FirstError() != wantFirst || nr.HasError() != (failed > 0) || !slices.Equal(nr.Errors(), wantErrors) {
								t.Fatalf("aggregate/query/count disagreement: %+v", nr.Stats())
							}
							joined := nr.JoinErrors()
							if len(wantErrors) == 0 {
								if joined != nil {
									t.Fatal("unexpected joined error")
								}
							} else {
								unwrapper, ok := joined.(interface{ Unwrap() []error })
								if !ok || !slices.Equal(unwrapper.Unwrap(), wantErrors) {
									t.Fatal("joined errors lost ordering or duplicates")
								}
								for _, e := range wantErrors {
									if !errors.Is(joined, e) {
										t.Fatal("joined error lost identity")
									}
								}
							}
							if callbacks.Load() != int32(len(indices)) || business.Load() != int32(wantBusiness) {
								t.Fatal("fill/aggregate executed business or notified")
							}
							return got
						}
						saved := check()
						edit := errors.New("caller edit")
						if len(saved) > 0 {
							saved[0] = core.Result[struct{}]{Err: edit, Occupied: true}
							check()
						}
						before := slices.Clone(saved)
						for _, fillTarget := range []int{target, target, 0, -1} {
							for wantTotal < fillTarget {
								filled := false
								for i := range model {
									if !model[i].live {
										model[i] = slot{true, core.ErrSkipped}
										filled = true
										break
									}
								}
								if !filled {
									model = append(model, slot{true, core.ErrSkipped})
								}
								wantTotal++
							}
							FillNoResultSkipped(nr, fillTarget)
							check()
						}
						streamed := 0
						for range nr.StreamResults() {
							streamed++
						}
						if streamed != len(indices) {
							t.Fatalf("stream events=%d want %d", streamed, len(indices))
						}
						if !slices.Equal(saved, before) {
							t.Fatal("fill overwrote caller snapshot")
						}
						final := check()
						if _, err := nr.Reset(); err != nil {
							t.Fatal(err)
						}
						if total, failed, first, slots := BuildAggregateNoResult(nr); total != 0 || failed != 0 || first != nil || len(slots) != 0 {
							t.Fatal("reset retained aggregate")
						}
						if err := nr.Go(context.Background(), func(context.Context) error { return nil }); err != nil {
							t.Fatal(err)
						}
						nr.Wait()
						if total, failed, first, slots := BuildAggregateNoResult(nr); total != 1 || failed != 0 || first != nil || len(slots) != 1 || !slots[0].Occupied {
							t.Fatal("reset retained sparse bookkeeping")
						}
						for i, item := range model {
							if final[i].Occupied != item.live || final[i].Err != item.err {
								t.Fatal("reset overwrote returned aggregate")
							}
						}
					})
				}
			}
		}
	}
}
