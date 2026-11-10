package maps

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chichengyu/async/internal/core"
	"github.com/chichengyu/async/internal/pool"
)

type poolProjectionExecutor interface {
	Map(func(context.Context, int, int) (int, error)) *MapResult[int, int]
	ForEach(func(context.Context, int, int) error) *MapForEachResult
	MapBatch(func(context.Context, []MapEntry[int, int]) (int, error)) *MapResult[int, int]
	ForEachBatch(func(context.Context, []MapEntry[int, int]) error) *MapForEachResult
}

// The oracle records the callback's actual batch head and element count. It
// never infers either from Go's map iteration or pool retention order.
func TestMapPoolProjectionIndependentOfRetention(t *testing.T) {
	for _, mode := range []string{"chain", "parallel"} {
		for _, retention := range []string{"prefix", "drop", "error", "block"} {
			for _, outcome := range []string{"success", "partial", "canceled", "closed"} {
				for _, method := range []string{"map", "each", "map-batch", "each-batch"} {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", mode, retention, outcome, method), func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							pl := pool.NewPool[int](3).WithMaxResults(1)
							defer pl.Close()
							switch retention {
							case "drop":
								pl.WithRingBuffer(1, core.OverflowDrop)
							case "error":
								pl.WithRingBuffer(1, core.OverflowError)
							case "block":
								pl.WithRingBuffer(1, core.OverflowBlock)
							}
							ctx, cancel := context.WithCancel(context.Background())
							defer cancel()
							var rejected error
							if outcome == "canceled" {
								cancel()
								rejected = context.Canceled
							} else if outcome == "closed" {
								pl.Close()
								rejected = core.ErrPoolClosed
							}
							data := map[int]int{1: 11, 2: 22, 3: 33, 4: 44, 5: 55, 6: 66, 7: 77}
							chain := NewMapChain[int, int](ctx, data)
							var executor poolProjectionExecutor = chain.Pool(pl).Chunk(3)
							if mode == "parallel" {
								executor = chain.Parallel().Pool(pl).Chunk(3)
							}
							// Repeat on the borrowed pool to check ResetWait as well.
							for cycle := 0; cycle < 2; cycle++ {
								wantErr := rejected
								businessErr := errors.New("projection business failure")
								var started atomic.Int32
								var mu sync.Mutex
								visits, wantValues := make(map[int]int), make(map[int]int)
								var completed []int32
								var failed int64
								process := func(entries []MapEntry[int, int]) (int, error) {
									order := started.Add(1)
									// Force the first started callback to finish last, so
									// collecting retained/completion-ordered values cannot pass.
									if order == 1 {
										time.Sleep(time.Hour)
									}
									mu.Lock()
									defer mu.Unlock()
									completed = append(completed, order)
									sum := 0
									for _, entry := range entries {
										visits[entry.Key]++
										if entry.Val != data[entry.Key] {
											t.Errorf("changed input: %+v", entry)
										}
										sum += entry.Val
									}
									if outcome == "partial" && order == 2 {
										failed += int64(len(entries))
										wantErr = businessErr
										return -1, businessErr
									}
									wantValues[entries[0].Key] = sum
									return sum, nil
								}
								var mapped *MapResult[int, int]
								var each *MapForEachResult
								switch method {
								case "map":
									mapped = executor.Map(func(_ context.Context, k, v int) (int, error) {
										return process([]MapEntry[int, int]{{Key: k, Val: v}})
									})
								case "each":
									each = executor.ForEach(func(_ context.Context, k, v int) error {
										_, err := process([]MapEntry[int, int]{{Key: k, Val: v}})
										return err
									})
								case "map-batch":
									mapped = executor.MapBatch(func(_ context.Context, entries []MapEntry[int, int]) (int, error) { return process(entries) })
								case "each-batch":
									each = executor.ForEachBatch(func(_ context.Context, entries []MapEntry[int, int]) error { _, err := process(entries); return err })
								}
								if rejected != nil {
									failed = int64(len(data))
									if started.Load() != 0 {
										t.Fatal("rejected task called business callback")
									}
								} else {
									if len(completed) < 2 || completed[0] == 1 {
										t.Fatalf("did not force out-of-order completion: %v", completed)
									}
									for key := range data {
										if visits[key] != 1 {
											t.Fatalf("key %d visited %d times", key, visits[key])
										}
									}
								}
								if mapped != nil {
									if !errors.Is(mapped.Error(), wantErr) || !reflect.DeepEqual(mapped.Data(), wantValues) {
										t.Fatalf("cycle %d map=%v/%v want=%v/%v", cycle, mapped.Data(), mapped.Error(), wantValues, wantErr)
									}
								} else if each.Total() != int64(len(data)) || each.FailCount() != failed || !errors.Is(each.Error(), wantErr) {
									t.Fatalf("cycle %d stats=%d/%d/%v want=7/%d/%v", cycle, each.Total(), each.FailCount(), each.Error(), failed, wantErr)
								}
							}
						})
					})
				}
			}
		}
	}
}
